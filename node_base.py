import os
import xmlrpc.client

class NodeBase:
    def __init__(self, account_file, initial_balance, node_name, coordinator_endpoint=None, peer_endpoints=None):
        self.account_file = account_file
        self.initialize_account(initial_balance)
        self.node_name = node_name
        self.state = None
        self.pending_transaction = None
        self.coordinator = xmlrpc.client.ServerProxy(coordinator_endpoint) if coordinator_endpoint else None
        self.peers = {name: xmlrpc.client.ServerProxy(endpoint) for name, endpoint in (peer_endpoints or {}).items()}

    def initialize_account(self, initial_balance=0):
        """Initialize the account file with a specified starting balance."""
        if not os.path.exists(self.account_file):
            with open(self.account_file, "w") as f:
                f.write(str(initial_balance))
            print(f"{self.node_name}: Account initialized with balance {initial_balance}.")
        else:
            print(f"{self.node_name}: Account file already exists. Skipping initialization.")

    def _read_account(self):
        """Read account balance from the file."""
        try:
            with open(self.account_file, "r") as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return None  # Account does not exist

    def _write_account(self, balance):
        """Write account balance to the file."""
        try:
            with open(self.account_file, "w") as f:
                f.write(str(balance))
        except IOError as e:
            print(f"{self.node_name}: Error writing account balance. {e}")
            return False
        return True

    def prepare(self, transaction_id, amount):
        """Prepare phase: validate the transaction."""
        print(f"{self.node_name}: Received prepare request for transaction {transaction_id} with amount {amount}.")
        balance = self._read_account()
        if balance is None:
            print(f"{self.node_name}: Account does not exist for transaction {transaction_id}.")
            return False
        elif balance >= amount:
            self.state = "PREPARED"
            self.pending_transaction = (transaction_id, amount)
            print(f"{self.node_name}: Prepared for transaction {transaction_id}.")
            return True
        else:
            print(f"{self.node_name}: Insufficient funds for transaction {transaction_id}.")
            return False

    def commit(self, transaction_id):
        """Commit the transaction."""
        print(f"{self.node_name}: Received commit request for transaction {transaction_id}.")
        if self.state == "PREPARED" and self.pending_transaction:
            _, amount = self.pending_transaction
            balance = self._read_account()
            if balance is None:
                print(f"{self.node_name}: Cannot commit transaction {transaction_id}. Failed to read account.")
                return False
            self._write_account(balance - amount)
            self.state = "COMMITTED"
            self.pending_transaction = None
            print(f"{self.node_name}: Transaction {transaction_id} committed successfully.")
            self.state = None  # Reset state after commit
            return True
        print(f"{self.node_name}: Cannot commit transaction {transaction_id}. Not in prepared state.")
        return False

    def abort(self, transaction_id):
        """Abort the transaction."""
        print(f"{self.node_name}: Received abort request for transaction {transaction_id}.")
        if self.state == "PREPARED":
            self.state = "ABORTED"
            self.pending_transaction = None
            print(f"{self.node_name}: Transaction {transaction_id} aborted.")
        elif self.state == "ABORTED":
            print(f"{self.node_name}: Transaction {transaction_id} already aborted.")
        else:
            print(f"{self.node_name}: Cannot abort transaction {transaction_id}. Not in prepared state.")
        self.state = None  # Reset state after abort
        return True

    def communicate_with_coordinator(self, method, transaction_id=None, amount=None):
        """Sends a request to the coordinator with fixed arguments."""
        if not self.coordinator:
            raise ValueError(f"{self.node_name}: Coordinator not configured.")
        try:
            return getattr(self.coordinator, method)(transaction_id, amount)
        except AttributeError:
            raise ValueError(f"{self.node_name}: Unknown method '{method}' requested.")
