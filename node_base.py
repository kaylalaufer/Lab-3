import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading

class NodeBase:
    def __init__(self, account_file, initial_balance, node_name, port, host="localhost", coordinator_endpoint=None, peer_endpoints=None):
        self.host = host
        self.account_file = account_file
        self.initial_balance = initial_balance
        self.node_name = node_name
        self.port = port
        self.coordinator = xmlrpc.client.ServerProxy(coordinator_endpoint) if coordinator_endpoint else None
        self.peers = {name: xmlrpc.client.ServerProxy(endpoint) for name, endpoint in (peer_endpoints or {}).items()}
        self.server_running = True
        self.state = None # PREPARE, COMMITTED, or ABORTED
        self.pending_transaction = None # Current transaction ID
        self.case = 0 # Used for simulating crashed nodes
        self.roll_back = None # Transaction ID, amount in account before transaction
        self.last_activity = time.time()  # Timestamp of the last activity
        self.inactivity_threshold = 15  # Time in seconds before initiating recovery
        self._start_inactivity_thread()  # Start inactivity monitoring
        self.log = {} # Log of all transactions: {txn ID: result, verified}
        self.prev_txn = None # Previous transaction ID

    def _start_inactivity_thread(self):
        """Start a background thread to monitor inactivity."""
        def monitor_inactivity():
            while self.server_running:
                time.sleep(5)  # Check inactivity every 5 seconds
                if time.time() - self.last_activity > self.inactivity_threshold:
                    print(f"{self.node_name}: Inactivity detected. Initiating recovery.")
                    self.ping_coordinator() # Check if coordinator is still active, shutdown if not
                    self.recover()  # Trigger recovery
                    self.last_activity = time.time()  # Reset inactivity after recovery

        thread = threading.Thread(target=monitor_inactivity, daemon=True)
        thread.start()

    def _update_last_activity(self):
        """Update the timestamp for the last activity."""
        self.last_activity = time.time()

    def ping_coordinator(self):
        """Check if coordinator is still alive."""
        try:
            self.coordinator.is_alive()
        except Exception as e:
            print(f"{self.node_name}: Failed to contact coordinator. Shutting down")
            self.shutdown()

    def initialize_account(self, initial_balance=0):
        """Initialize the account file with a specified starting balance."""
        self._update_last_activity()  # Mark activity
        if self._read_account() == initial_balance:
            print(f"{self.node_name}: Account already initialized with balance {initial_balance}.")
            return initial_balance
        print(f"{self.node_name}: Account initialized with balance {initial_balance}.")
        return self._write_account(initial_balance)

    def get_balance(self):
        """Public method to expose the current account balance.""" 
        balance = self._read_account()
        return balance if balance is not None else 0

    def simulation_case(self, case):
        """Set simulation case for testing."""
        self.case = case
        return True

    def _read_account(self):
        """Read account balance from the file."""
        try:
            with open(self.account_file, "r") as f:
                return float(f.read().strip())  
        except FileNotFoundError:
            return None  # Account does not exist


    def _write_account(self, balance):
        """Write account balance to the file."""
        try:
            with open(self.account_file, "w") as f:
                f.write(f"{balance:.2f}")  # Write as float with 2 decimal places
        except IOError as e:
            print(f"{self.node_name}: Error writing account balance. {e}")
            return False
        return True

    def prepare(self, transaction_id, amount):
        """Prepare phase: validate the transaction."""
        self._update_last_activity()  # Mark activity
        print(f"{self.node_name}: Received prepare request for transaction {transaction_id} with amount {amount}.")
        # Check for unclean state
        if self.state is not None:
            print(f"{self.node_name}: Unclean state detected! Current state: {self.state}")

            # If the previous transaction was PREPARED but unresolved, roll it back
            if self.state == "PREPARED" and self.pending_transaction:
                prev_transaction_id, prev_balance = self.roll_back
                print(f"{self.node_name}: Rolling back previous transaction {prev_transaction_id}.")
                self._write_account(prev_balance)  # Restore the previous balance
                self.state = None
                self.pending_transaction = None
                self.roll_back = None
                print(f"{self.node_name}: Previous transaction rolled back successfully.")

            # If the previous transaction is COMMITTED or ABORTED, do not roll back
            elif self.state in ["COMMITTED", "ABORTED"]:
                print(f"{self.node_name}: Transaction {transaction_id} rejected. Previous transaction already finalized.")
                return False
        self.state = "PREPARED"
        self.pending_transaction = (transaction_id, amount)
        balance = self._read_account()
        self.roll_back = (transaction_id, balance)

        if self.case == 1 and self.node_name == "Node-2":
            time.sleep(20) # Node-2 crashes (does not respond to coordinator)

        if balance is None:
            print(f"{self.node_name}: Account does not exist for transaction {transaction_id}.")
            return False
        elif amount < 0 and balance < abs(amount):  # Check for sufficient balance for withdrawal
            print(f"{self.node_name}: Insufficient funds for transaction {transaction_id}.")
            return False
        print(f"{self.node_name}: Prepared for transaction {transaction_id}.")
        return True

    def commit(self, transaction_id):
        """Commit the transaction."""
        self.prev_txn = transaction_id
        self._update_last_activity()  # Mark activity
        print(f"{self.node_name}: Received commit request for transaction {transaction_id}.")
        self.log[transaction_id] = ("COMMITTED", False)
        if self.case == 2 and self.node_name == "Node-2":
            time.sleep(20) # Node-2 crashes (does not respond to coordinator)

        if self.state == "PREPARED" and self.pending_transaction and self.pending_transaction[0] == transaction_id:
            _, amount = self.pending_transaction
            balance = self._read_account()
            if balance is None:
                print(f"{self.node_name}: Cannot commit transaction {transaction_id}. Failed to read account.")
                return False
            new_balance = balance + amount
            self._write_account(new_balance)
            self.state = "COMMITTED"
            self.pending_transaction = None
            print(f"{self.node_name}: Transaction {transaction_id} committed successfully.")
            self.state = None  # Reset state after commit
            return True
        print(f"{self.node_name}: Cannot commit transaction {transaction_id}. Not in prepared state.")
        self.state = None  # Reset state after commit
        return False

    def abort(self, transaction_id):
        """Abort the transaction."""
        self.prev_txn = transaction_id
        self.log[transaction_id] = ("ABORTED", False)
        self._update_last_activity()  # Mark activity

        print(f"{self.node_name}: Received abort request for transaction {transaction_id}.")
        if self.state == "PREPARED" and self.pending_transaction and self.pending_transaction[0] == transaction_id:
            self.state = "ABORTED"
            self.pending_transaction = None
            print(f"{self.node_name}: Transaction {transaction_id} aborted.")
        else:
            print(f"{self.state} {self.pending_transaction}")
            print(f"{self.node_name}: Cannot abort transaction {transaction_id}. Not in prepared state or transaction does not match.")
            self.state = None  # Reset state after abort
            return False
        self.state = None  # Reset state after abort
        return True

    def roll_back_state(self, transaction_id):
        """Roll back the account to its state before the transaction."""
        self._update_last_activity()  # Mark activity
        if not self.roll_back:
            print(f"{self.node_name}: No rollback state available. Nothing to roll back.")
            return False

        if self.roll_back[0] != transaction_id:
            print(f"{self.node_name}: Rollback skipped. Transaction ID {transaction_id} does not match rollback state.")
            return False

        print(f"{self.node_name}: Rolling back transaction {transaction_id}.")
        self._write_account(self.roll_back[1])  # Restore the previous balance
        self.state = None  # Reset state
        self.pending_transaction = None
        self.roll_back = None  # Clear rollback state
        print(f"{self.node_name}: Rollback completed for transaction {transaction_id}.")
        return True

    def recover(self):
        """Recover the node's state after inactivity or crash."""
        print(f"{self.node_name}: Starting recovery process.")

        # Determine the transaction to recover
        transaction_id, state = self._get_transaction_to_recover()
        if not transaction_id:
            print(f"{self.node_name}: No recovery needed. State is clean.")
            return

        try:
            # Query the coordinator for the transaction outcome
            outcome = self.coordinator.handle_recovering_node(transaction_id, self.node_name)
            print(f"{self.node_name}: Coordinator outcome for transaction {transaction_id}: {outcome}")
            print(f"{self.node_name}: Current state: {state}")

            if self._is_recovery_needed(state, outcome):
                self._finalize_recovery(transaction_id, state, outcome)
            else:
                print(f"{self.node_name}: Transaction {transaction_id} already consistent. No recovery needed.")
                self.log[transaction_id] = (outcome, True)  # Mark as verified

        except Exception as e:
            print(f"{self.node_name}: Failed to contact coordinator: {e}. Assuming abort.")
            self.abort(transaction_id)

    def _get_transaction_to_recover(self):
        """Identify the transaction and state for recovery."""
        if self.pending_transaction:
            return self.pending_transaction[0], self.state
        if self.prev_txn:
            transaction_id = self.prev_txn
            state, checked = self.log.get(transaction_id, (None, False))
            if checked:
                print(f"{self.node_name}: Transaction {transaction_id} already verified. Skipping recovery.")
                return None, None
            return transaction_id, state
        return None, None

    def _is_recovery_needed(self, state, outcome):
        """Determine if recovery is needed based on state and outcome."""
        return state != outcome

    def _finalize_recovery(self, transaction_id, state, outcome):
        """Resolve inconsistencies during recovery."""
        if state == "PREPARED":
            if outcome == "COMMITTED":
                print(f"{self.node_name}: Commit confirmed. Finalizing transaction.")
                self.commit(transaction_id)
            elif outcome == "ABORTED":
                print(f"{self.node_name}: Abort confirmed. Rolling back transaction.")
                self.abort(transaction_id)
        elif state == "COMMITTED" and outcome == "ABORTED":
            print(f"{self.node_name}: Coordinator indicates abort. Rolling back transaction.")
            self.roll_back_state(transaction_id)
        elif state == "ABORTED":
            print(f"{self.node_name}: Transaction already aborted. No recovery needed.")
        else:
            print(f"{self.node_name}: Unexpected state {state}. Assuming abort for safety.")
            self.abort(transaction_id)

        # Mark the transaction as verified
        self.log[transaction_id] = (outcome, True)

    def shutdown(self):
        """Stop the server gracefully."""
        print(f"{self.node_name}: Remote shutdown requested.")
        self.server_running = False
        print(f"{self.node_name}: Server stopping gracefully.")
        return "Shutdown initiated"

    def run_server(self):
        """Run the XML-RPC server."""
        def server_thread():
            with SimpleXMLRPCServer((self.host, self.port), allow_none=True) as server:
                server.register_instance(self)  # Expose all methods in this class
                server.logRequests = False
                print(f"{self.node_name} started on port {self.port} and waiting for requests...")

                while self.server_running:
                    server.handle_request()  # Process one request at a time

                print(f"{self.node_name}: Server has stopped.")

        thread = threading.Thread(target=server_thread, daemon=True)
        thread.start()

        try:
            while self.server_running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n{self.node_name}: Shutdown signal received. Exiting...")
            self.shutdown()
