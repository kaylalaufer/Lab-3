import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading

class NodeBase:
    def __init__(self, account_file, initial_balance, node_name, port, coordinator_endpoint=None, peer_endpoints=None):
        self.account_file = account_file
        self.initial_balance = initial_balance
        self.node_name = node_name
        self.port = port
        self.coordinator = xmlrpc.client.ServerProxy(coordinator_endpoint) if coordinator_endpoint else None
        self.peers = {name: xmlrpc.client.ServerProxy(endpoint) for name, endpoint in (peer_endpoints or {}).items()}
        self.server_running = True
        self.state = None
        self.pending_transaction = None
        self.case = 0
        self.initialize_account()

    def initialize_account(self, initial_balance=0):
        """Initialize the account file with a specified starting balance."""
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
        self.case = case
        return True

    def _read_account(self):
        """Read account balance from the file."""
        try:
            with open(self.account_file, "r") as f:
                return float(f.read().strip())  # Use float instead of int
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
        print(f"{self.node_name}: Received prepare request for transaction {transaction_id} with amount {amount}.")
        self.state = "PREPARED"
        self.pending_transaction = (transaction_id, amount)
        balance = self._read_account()

        if self.case == 1 and self.node_name == "Node-2":
            time.sleep(30) # Node-2 crashes (does not respond to coordinator)

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

        if self.case == 2 and self.node_name == "Node-2":
            time.sleep(30) # Node-2 crashes (does not respond to coordinator)

        print(f"{self.node_name}: Received commit request for transaction {transaction_id}.")
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
        return False

    def abort(self, transaction_id):
        """Abort the transaction."""

        if self.case == 2 and self.node_name == "Node-2":
            time.sleep(30) # Node-2 crashes (does not respond to coordinator)

        print(f"{self.node_name}: Received abort request for transaction {transaction_id}.")
        if self.state == "PREPARED" and self.pending_transaction and self.pending_transaction[0] == transaction_id:
            self.state = "ABORTED"
            self.pending_transaction = None
            print(f"{self.node_name}: Transaction {transaction_id} aborted.")
        else:
            print(f"{self.state} {self.pending_transaction}")
            print(f"{self.node_name}: Cannot abort transaction {transaction_id}. Not in prepared state or transaction does not match.")
        self.state = None  # Reset state after abort
        return True

    def shutdown(self):
        """Stop the server gracefully."""
        print(f"{self.node_name}: Remote shutdown requested.")
        self.server_running = False
        print(f"{self.node_name}: Server stopping gracefully.")
        return "Shutdown initiated"

    def run_server(self):
        """Run the XML-RPC server."""
        def server_thread():
            with SimpleXMLRPCServer(("localhost", self.port), allow_none=True) as server:
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
