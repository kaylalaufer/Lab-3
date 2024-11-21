from concurrent.futures import ThreadPoolExecutor, TimeoutError
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import time

class Coordinator:
    def __init__(self, account_to_node):
        self.account_to_node = account_to_node
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}
        self.executor = ThreadPoolExecutor(max_workers=len(self.account_to_node))
        self.shutting_down = False
        self.transaction_log = {}  # Dictionary to store transaction states

    def _call_with_timeout(self, func, *args, timeout=5):
        """Call a function with a timeout."""
        future = self.executor.submit(func, *args)
        try:
            return future.result(timeout=timeout)  # Wait for the result with a timeout
        except TimeoutError:
            print(f"Coordinator: Timeout during RPC call.")
            return None
        except Exception as e:
            print(f"Coordinator: Error during RPC call: {e}")
            return None

    def execute_transaction(self, transaction_id, transactions, timeout=5):
        print(f"Coordinator: Starting transaction {transaction_id} for {transactions}.")
        if self.shutting_down:
            print(f"Coordinator: Rejecting transaction {transaction_id} as the coordinator is shutting down.")
            return "Coordinator is shutting down. No new transactions are accepted."
        # Phase 1: Prepare
        prepared_nodes = []  # Track nodes that successfully prepare
        prepare_responses = {}
        for account, amount in transactions.items():
            participant = self.participants.get(account)
            if not participant:
                print(f"Coordinator: No participant found for account {account}.")
                prepare_responses[account] = False
                continue

            response = self._call_with_timeout(participant.prepare, transaction_id, amount, timeout=timeout)
            if response is None:
                print(f"Coordinator: Timeout during prepare for account {account}.")
                prepare_responses[account] = False
            else:
                prepare_responses[account] = response
                prepared_nodes.append(account)

        if not all(prepare_responses.values()):
            print(f"Coordinator: Prepare phase failed for transaction {transaction_id}. Aborting.")
            self._send_abort(transaction_id, prepared_nodes)
            self.transaction_log[transaction_id] = "ABORTED"  # Log the result
            return "Transaction Aborted"

        # Phase 2: Commit
        print(f"Coordinator: All participants prepared. Sending commit requests.")
        commit_nodes = []
        commit_responses = {}
        for account in transactions.keys():
            participant = self.participants.get(account)
            response = self._call_with_timeout(participant.commit, transaction_id, timeout=timeout)
            if response is None:
                print(f"Coordinator: Timeout during commit for account {account}.")
                commit_responses[account] = False
            else:
                commit_responses[account] = response
                commit_nodes.append(account)

        if not all(commit_responses.values()):
            print(f"Coordinator: Commit phase failed. Rolling back all participants.")
            self._roll_back_all(transaction_id, commit_nodes)
            self.transaction_log[transaction_id] = "ABORTED"  # Log the result
            return "Transaction Aborted"

        print(f"Coordinator: Transaction {transaction_id} committed successfully.")
        self.transaction_log[transaction_id] = "COMMITTED"  # Log the result
        return "Transaction Committed"

    def _send_abort(self, transaction_id, accounts):
        for account in accounts:
            participant = self.participants.get(account)
            if participant:
                participant.abort(transaction_id)

    def _roll_back_all(self, transaction_id, accounts):
        for account in accounts:    
            participant = self.participants.get(account)
            if participant:
                participant.roll_back_state(transaction_id)

    def handle_recovering_node(self, transaction_id, account):
        """Handle a recovering node by sending the appropriate commit/abort."""
        print(f"Coordinator: Handling recovery for account {account} on transaction {transaction_id}.")
        final_result = self.transaction_log.get(transaction_id, "ABORTED")  # Default to ABORTED if unknown
        return final_result

    def shutdown(self):
        """Gracefully shut down all participants."""
        print("Coordinator: Initiating graceful shutdown of participants.")
        self.shutting_down = True  # Set shutdown flag

        print("Coordinator: Grace period for ongoing recoveries.")
        time.sleep(10)  # Grace period for ongoing recovery

        for account, participant in self.participants.items():
            try:
                print(f"Coordinator: Sending shutdown request to participant handling account {account}.")
                participant.shutdown()
            except ConnectionRefusedError:
                print(f"Coordinator: Participant handling account {account} is already shut down.")
            except Exception as e:
                print(f"Coordinator: Error during shutdown of account {account}: {e}")
        
        print("Coordinator: Finalizing shutdown.")
        self.executor.shutdown(wait=True)  # Wait for ongoing threads to complete
        print("Coordinator: Executor shut down.")

def start_coordinator():
    account_to_node = {
        "A": "http://localhost:8001",
        "B": "http://localhost:8002"
    }
    coordinator = Coordinator(account_to_node)
    server = SimpleXMLRPCServer(("localhost", 8000), allow_none=True)
    server.register_instance(coordinator)
    server.logRequests = False

    #coordinator.server_running = True  # Add a flag to control server loop

    try:
        print("Coordinator (Node-1) started and waiting for requests...")
        while not coordinator.shutting_down:
            #server.serve_forever()
            server.handle_request()
    except KeyboardInterrupt:
        print("\nCoordinator: Shutdown signal received. Shutting down...")
        coordinator.shutdown()
        print("Coordinator: Exiting.")

if __name__ == "__main__":
    start_coordinator()