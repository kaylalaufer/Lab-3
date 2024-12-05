from concurrent.futures import ThreadPoolExecutor, TimeoutError
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import time
import threading

class Coordinator:
    def __init__(self, account_to_node):
        self.account_to_node = account_to_node
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}
        self.executor = ThreadPoolExecutor(max_workers=len(self.account_to_node))
        self.shutting_down = False
        self.shutdown_event = threading.Event()  # Event to signal thread shutdown
        self.transaction_log = {}  # Dictionary to store transaction states
        self.last_activity = time.time()  # Timestamp of the last activity
        self.inactivity_threshold = 30  # Time in seconds before initiating recovery
        self._start_inactivity_thread()  # Start inactivity monitoring

    def _start_inactivity_thread(self):
        """Start a background thread to monitor inactivity."""
        def monitor_inactivity():
            while not self.shutdown_event.is_set():  # Check for shutdown signal
                time.sleep(1)  # Sleep for a short interval
                if time.time() - self.last_activity > self.inactivity_threshold:
                    print(f"Coordinator: Inactivity detected. Signaling shutdown.")
                    self.shutdown_event.set()  # Signal shutdown
                    break  # Exit the loop after signaling

        self.inactivity_thread = threading.Thread(target=monitor_inactivity)
        self.inactivity_thread.start()  # Start the thread

    def is_alive(self):
        """Returns True when Participant pings the Coordinator."""
        return True

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

    def initialize_node(self, account, balance):
        """Initialize a node's account balance."""
        # Used for test cases
        self.last_activity = time.time()
        participant = self.participants.get(account)
        if participant:
            try:
                print(f"Coordinator: Initializing account {account} with balance {balance}.")
                return participant.initialize_account(balance)
            except Exception as e:
                print(f"Coordinator: Failed to initialize account {account}: {e}")
                return False
        else:
            print(f"Coordinator: No participant found for account {account}.")
            return False

    def set_simulation_case(self, case_number):
        """Set a simulation case for all nodes."""
        # Used for test cases
        self.last_activity = time.time()
        results = {}
        for account, participant in self.participants.items():
            try:
                print(f"Coordinator: Setting case {case_number} for account {account}.")
                results[account] = participant.simulation_case(case_number)
            except Exception as e:
                print(f"Coordinator: Failed to set case for account {account}: {e}")
                results[account] = False
        return results

    def get_account_balance(self, account):
        """Get account balance."""
        # Used for test cases
        self.last_activity = time.time()
        participant = self.participants.get(account)
        if participant:
            try:
                return participant.get_balance()
            except Exception as e:
                print(f"Coordinator: Failed to get account {account}: {e}")
                return False
        else:
            print(f"Coordinator: No participant found for account {account}.")
            return False

    def execute_transaction(self, transaction_id, transactions, timeout=5):
        """Recieve a transaction from the client. Entering 2PC."""
        self.last_activity = time.time()
        print(f"Coordinator: Starting transaction {transaction_id} for {transactions}.")
        if self.shutting_down: # Rejects transaction if coordinator is already shutting down
            print(f"Coordinator: Rejecting transaction {transaction_id} as the coordinator is shutting down.")
            return "Coordinator is shutting down. No new transactions are accepted."

        # Phase 1: Prepare
        prepared_nodes = []  # Track nodes that successfully prepare
        prepare_responses = {}
        for account, amount in transactions.items():
            participant = self.participants.get(account)
            if not participant: # Node doesn't exist
                print(f"Coordinator: No participant found for account {account}.")
                prepare_responses[account] = False
                continue

            response = self._call_with_timeout(participant.prepare, transaction_id, amount, timeout=timeout)
            if response is None: # Node crashes
                print(f"Coordinator: Timeout during prepare for account {account}.")
                prepare_responses[account] = False
            else: # Response is received
                prepare_responses[account] = response
                prepared_nodes.append(account)

        if not all(prepare_responses.values()): # Abort transaction
            print(f"Coordinator: Prepare phase failed for transaction {transaction_id}. Aborting.")
            self._send_abort(transaction_id, prepared_nodes)
            self.transaction_log[transaction_id] = "ABORTED"  # Log the result
            return "Transaction Aborted"

        # Phase 2: Commit
        print(f"Coordinator: All participants prepared. Sending commit requests.")
        self.last_activity = time.time()
        commit_nodes = [] # Track nodes that successfully commit
        commit_responses = {}
        for account in transactions.keys():
            participant = self.participants.get(account)
            response = self._call_with_timeout(participant.commit, transaction_id, timeout=timeout)
            if response is None: # Node crashed
                print(f"Coordinator: Timeout during commit for account {account}.")
                commit_responses[account] = False
            else: # Response is received
                commit_responses[account] = response
                commit_nodes.append(account)

        if not all(commit_responses.values()): # Rollback phase - transaction is now aborted
            print(f"Coordinator: Commit phase failed. Rolling back all participants.")
            self._roll_back_all(transaction_id, commit_nodes)
            self.transaction_log[transaction_id] = "ABORTED"  # Log the result
            return "Transaction Aborted"

        # Transaction successfully committed
        print(f"Coordinator: Transaction {transaction_id} committed successfully.")
        self.transaction_log[transaction_id] = "COMMITTED"  # Log the result
        return "Transaction Committed"

    def _send_abort(self, transaction_id, accounts):
        """Transaction is aborted."""
        self.last_activity = time.time()
        for account in accounts:
            participant = self.participants.get(account)
            if participant:
                participant.abort(transaction_id)

    def _roll_back_all(self, transaction_id, accounts):
        """Transaction is rolled back to previous state."""
        self.last_activity = time.time()
        for account in accounts:    
            participant = self.participants.get(account)
            if participant:
                participant.roll_back_state(transaction_id)

    def handle_recovering_node(self, transaction_id, account):
        """Handle a recovering node by sending the appropriate commit/abort."""
        self.last_activity = time.time()
        print(f"Coordinator: Handling recovery for account {account} on transaction {transaction_id}.")
        final_result = self.transaction_log.get(transaction_id, "ABORTED")  # Default to ABORTED if unknown
        return final_result

    def shutdown(self):
        """Gracefully shut down all participants."""
        print("Coordinator: Initiating graceful shutdown of participants.")
        
        self.shutdown_event.set()  # Signal inactivity thread to exit

        # Wait for the inactivity thread to complete, but avoid self-join
        if threading.current_thread() != self.inactivity_thread and self.inactivity_thread.is_alive():
            print("Coordinator: Waiting for inactivity thread to complete...")
            self.inactivity_thread.join()

        # Notify participants to shut down
        for account, participant in self.participants.items():
            try:
                print(f"Coordinator: Sending shutdown request to participant handling account {account}.")
                participant.shutdown()
            except Exception as e:
                print(f"Coordinator: Error during shutdown of account {account}: {e}")

        print("Coordinator: Finalizing shutdown.")
        self.executor.shutdown(wait=True)  # Wait for ongoing threads to complete
        print("Coordinator: Executor shut down.")

def start_coordinator():
    account_to_node = {
        "A": "http://localhost:8001",
        "B": "http://localhost:8002"
    } # In the cloud, change 'localhost' to the internal IP of the participant
    coordinator = Coordinator(account_to_node)
    server = SimpleXMLRPCServer(("localhost", 8000), allow_none=True) # In the cloud, change 'localhost' to the internal IP of coordinator
    server.register_instance(coordinator)
    server.logRequests = False

    try:
        print("Coordinator (Node-1) started and waiting for requests...")
        while not coordinator.shutdown_event.is_set():
            # Use a timeout to avoid indefinite blocking
            server.timeout = 1
            server.handle_request()  
    except KeyboardInterrupt:
        print("\nCoordinator: Shutdown signal received.")
    finally:
        coordinator.shutdown()
        print("Coordinator: Exiting.")

if __name__ == "__main__":
    start_coordinator()