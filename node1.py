import threading
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class Coordinator:
    def __init__(self, account_to_node):
        """
        :param account_to_node: Dictionary mapping accounts to participant RPC endpoints.
        """
        self.account_to_node = account_to_node
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}

    def execute_transaction(self, transaction_id, transactions, timeout=5):
        """
        Execute a distributed transaction involving multiple accounts using the 2PC protocol.
        :param transaction_id: A unique ID for the transaction.
        :param transactions: A dictionary of account -> amount.
        :param timeout: Timeout for waiting for participant responses.
        :return: Commit or Abort decision.
        """
        print(f"Coordinator: Starting transaction {transaction_id} for {transactions}.")
        
        # Track responses
        prepare_responses = {}
        threads = []

        # Phase 1: Prepare
        def prepare_request(account, amount, done_event):
            participant = self.participants.get(account)
            if not participant:
                print(f"Coordinator: No participant found for account {account}. Aborting.")
                prepare_responses[account] = False
                done_event.set()
                return
            try:
                response = participant.prepare(transaction_id, amount)
                prepare_responses[account] = response
                if not response:
                    print(f"Coordinator: Prepare failed for account {account}.")
            except Exception as e:
                print(f"Coordinator: Error during prepare for account {account}: {e}")
                prepare_responses[account] = False
            finally:
                done_event.set()

        # Send prepare requests concurrently
        for account, amount in transactions.items():
            done_event = threading.Event()
            thread = threading.Thread(target=prepare_request, args=(account, amount, done_event))
            threads.append((thread, done_event))
            thread.start()

        # Wait for all prepare threads to finish or timeout
        for thread, done_event in threads:
            done_event.wait(timeout)
            if not done_event.is_set():
                print(f"Coordinator: Timeout waiting for prepare response from {thread.name}.")
                prepare_responses[thread.name] = False

        # Check if all participants are prepared
        if not all(prepare_responses.values()):
            print(f"Coordinator: Prepare phase failed for transaction {transaction_id}. Aborting.")
            self._send_abort(transaction_id, transactions.keys())
            return "Transaction Aborted"

        # Phase 2: Commit
        print(f"Coordinator: All participants prepared. Sending commit requests for transaction {transaction_id}.")
        commit_responses = {}
        commit_threads = []

        def commit_request(account, done_event):
            participant = self.participants.get(account)
            if participant:
                try:
                    participant.commit(transaction_id)
                    commit_responses[account] = True
                except Exception as e:
                    print(f"Coordinator: Error during commit for account {account}: {e}")
                    commit_responses[account] = False
                finally:
                    done_event.set()

        for account in transactions.keys():
            done_event = threading.Event()
            thread = threading.Thread(target=commit_request, args=(account, done_event))
            commit_threads.append((thread, done_event))
            thread.start()

        # Wait for all commit threads to finish or timeout
        for thread, done_event in commit_threads:
            done_event.wait(timeout)
            if not done_event.is_set():
                print(f"Coordinator: Timeout waiting for commit response from {thread.name}.")
                commit_responses[thread.name] = False

        # Check if all commits succeeded
        if not all(commit_responses.values()):
            print(f"Coordinator: Commit phase failed for transaction {transaction_id}. Rolling back all participants.")
            self._roll_back_all(transaction_id, transactions.keys())
            return "Transaction Aborted"

        print(f"Coordinator: Transaction {transaction_id} committed successfully.")
        return "Transaction Committed"

    def _roll_back_all(self, transaction_id, accounts):
        """Roll back all participants."""
        print(f"Coordinator: Rolling back transaction {transaction_id} for all participants.")
        for account in accounts:
            participant = self.participants.get(account)
            if participant:
                try:
                    participant.roll_back(transaction_id)
                    print(f"Coordinator: Rolled back account {account}.")
                except Exception as e:
                    print(f"Coordinator: Error during rollback for account {account}: {e}")


    def _send_abort(self, transaction_id, accounts):
        """Send abort requests to all participants."""
        print(f"Coordinator: Sending abort requests for transaction {transaction_id}.")
        for account in accounts:
            participant = self.participants.get(account)
            if participant:
                try:
                    participant.abort(transaction_id)
                    print(f"Coordinator: Abort request sent to account {account}.")
                except ConnectionRefusedError:
                    print(f"Coordinator: Connection refused by account {account}. Assuming node is down.")
                except Exception as e:
                    print(f"Coordinator: Error during abort for account {account}. Node likely crashed: {e}")
            else:
                print(f"Coordinator: No participant found for account {account}. Skipping abort.")

    def shutdown(self):
        """Gracefully shut down all participants."""
        print("Coordinator: Initiating graceful shutdown of participants.")
        for account, participant in self.participants.items():
            try:
                print(f"Coordinator: Sending shutdown request to participant handling account {account}.")
                participant.shutdown()
            except ConnectionRefusedError:
                print(f"Coordinator: Participant handling account {account} is already shut down.")
            except Exception as e:
                print(f"Coordinator: Error during shutdown of account {account}: {e}")
        print("Coordinator: All participants shut down. Coordinator shutting down.")


def start_coordinator():
    account_to_node = {
        "A": "http://localhost:8001",
        "B": "http://localhost:8002"
    }
    coordinator = Coordinator(account_to_node)
    server = SimpleXMLRPCServer(("localhost", 8000))
    server.register_instance(coordinator)
    server.logRequests = False

    try:
        print("Coordinator (Node-0) started and waiting for requests...")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCoordinator: Shutdown signal received. Shutting down...")
        coordinator.shutdown()
        print("Coordinator: Exiting.")

if __name__ == "__main__":
    start_coordinator()