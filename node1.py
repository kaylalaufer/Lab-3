import threading
import time
import sys
import signal
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class Coordinator:
    def __init__(self, account_to_node):
        """
        :param account_to_node: Dictionary mapping accounts to participant RPC endpoints.
        """
        self.account_to_node = account_to_node
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}
        self.last_request_time = time.time()
        self.shutdown_timeout = 60  # seconds
        self.shutdown_event = threading.Event()  # Event to signal shutdown
        threading.Thread(target=self._monitor_timeout, daemon=True).start()

    def _monitor_timeout(self):
        while not self.shutdown_event.is_set():
            if time.time() - self.last_request_time > self.shutdown_timeout:
                print("No requests received. Initiating shutdown...")
                self.shutdown_all_nodes()
                server.shutdown()  # Gracefully stop the XMLRPC server
                break
            time.sleep(5)  # Check every 5 seconds

    def execute_transaction(self, transaction_id, transactions):
        """
        Execute a distributed transaction involving multiple accounts using the 2PC protocol.
        :param transaction_id: A unique ID for the transaction.
        :param transactions: A dictionary of account -> amount.
                             Example: {"A": -500, "B": 500}
        :return: Commit or Abort decision.
        """
        self.last_request_time = time.time()  # Update the last request timestamp
        print(f"Coordinator: Starting transaction {transaction_id} for {transactions}.")

        # Track responses
        prepare_responses = {}
        threads = []

        # Phase 1: Prepare
        def prepare_request(account, amount):
            participant = self.participants.get(account)
            if not participant:
                print(f"Coordinator: No participant found for account {account}. Aborting.")
                prepare_responses[account] = False
                return
            try:
                response = participant.prepare(transaction_id, amount)
                prepare_responses[account] = response
                if not response:
                    print(f"Coordinator: Prepare failed for account {account}.")
            except Exception as e:
                print(f"Coordinator: Error during prepare for account {account}: {e}")
                prepare_responses[account] = False

        # Send prepare requests concurrently
        for account, amount in transactions.items():
            thread = threading.Thread(target=prepare_request, args=(account, amount))
            threads.append(thread)
            thread.start()

        # Wait for all prepare threads to finish
        for thread in threads:
            thread.join()

        # Check if all participants are prepared
        if not all(prepare_responses.values()):
            print(f"Coordinator: Prepare phase failed for transaction {transaction_id}. Aborting.")
            self._send_abort(transaction_id, transactions.keys())
            return "Transaction Aborted"

        # Phase 2: Commit
        print(f"Coordinator: All participants prepared. Sending commit requests for transaction {transaction_id}.")
        commit_threads = []

        def commit_request(account):
            participant = self.participants.get(account)
            if participant:
                try:
                    participant.commit(transaction_id)
                except Exception as e:
                    print(f"Coordinator: Error during commit for account {account}: {e}")

        for account in transactions.keys():
            thread = threading.Thread(target=commit_request, args=(account,))
            commit_threads.append(thread)
            thread.start()

        # Wait for all commit threads to finish
        for thread in commit_threads:
            thread.join()

        print(f"Coordinator: Transaction {transaction_id} committed successfully.")
        return "Transaction Committed"

    def _send_abort(self, transaction_id, accounts):
        """Send abort requests to all involved participants."""
        for account in accounts:
            participant = self.participants.get(account)
            if participant:
                try:
                    participant.abort(transaction_id)
                except Exception as e:
                    print(f"Coordinator: Error during abort for account {account}: {e}")

    def shutdown_all_nodes(self):
        """Shut down all participant nodes and exit."""
        self.shutdown_event.set()  # Signal the timeout thread to stop
        for account, participant in self.participants.items():
            try:
                # Check connectivity before sending shutdown
                print(f"Coordinator: Sending shutdown to participant managing account {account}.")
                participant.shutdown()
            except Fault as fault:
                print(f"Coordinator: XML-RPC fault for participant managing account {account}: {fault}")
            except ConnectionRefusedError:
                print(f"Coordinator: Connection refused for participant managing account {account}.")
            except Exception as e:
                print(f"Coordinator: Unexpected error shutting down participant managing account {account}: {e}")
        print("Coordinator shutting down.")
        sys.exit(0)


# Signal handler for Ctrl+C
def signal_handler(sig, frame):
    print("\nShutdown signal received. Shutting down...")
    try:
        coordinator.shutdown_all_nodes()
    except Exception as e:
        print(f"Error during shutdown: {e}")
    sys.exit(0)

# Global server instance
server = None

def start_coordinator():
    global coordinator, server
    account_to_node = {
        "A": "http://localhost:8001",  # Node-2 for Account A
        "B": "http://localhost:8002"   # Node-3 for Account B
    }
    coordinator = Coordinator(account_to_node)
    server = SimpleXMLRPCServer(("localhost", 8000), allow_none=True)
    server.register_instance(coordinator)
    server.register_function(coordinator.shutdown_all_nodes, "shutdown")
    server.logRequests = False  # Disable request logging

    print("Coordinator (Node-0) started and waiting for requests...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCoordinator shutting down...")
        server.shutdown()  # Stop the server
        coordinator.shutdown_all_nodes()
        sys.exit(0)

if __name__ == "__main__":
    # Register the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    start_coordinator()
