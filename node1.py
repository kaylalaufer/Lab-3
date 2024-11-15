from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

class Coordinator:
    def __init__(self, account_to_node):
        """
        :param account_to_node: Dictionary mapping accounts to participant RPC endpoints.
                                Example: {"A": "http://localhost:8001", "B": "http://localhost:8002"}
        """
        # Store the account to node mapping
        self.account_to_node = account_to_node
        # Create a proxy connection for each participant
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}

    def execute_transaction(self, transaction_id, account, amount):
        """
        Execute a distributed transaction using the 2PC protocol for a specific account.
        :param transaction_id: A unique ID for the transaction.
        :param account: The target account (e.g., "A" or "B").
        :param amount: The transaction amount (positive for deposit, negative for withdrawal).
        :return: Commit or Abort decision.
        """
        print(f"Coordinator: Starting transaction {transaction_id} on account {account} for amount {amount}.")
        
        # Get the participant responsible for this account
        participant = self.participants.get(account)
        if not participant:
            print(f"Coordinator: No participant found for account {account}.")
            return "Transaction Failed - Invalid Account"

        # Phase 1: Prepare
        try:
            response = participant.prepare(transaction_id, amount)
            if not response:
                print(f"Coordinator: Participant unable to prepare transaction {transaction_id}. Aborting.")
                participant.abort(transaction_id)
                return "Transaction Aborted"
        except Exception as e:
            print(f"Coordinator: Error during prepare phase: {e}")
            participant.abort(transaction_id)
            return "Transaction Aborted"

        # Phase 2: Commit
        print(f"Coordinator: Participant prepared. Committing transaction {transaction_id}.")
        try:
            participant.commit(transaction_id)
        except Exception as e:
            print(f"Coordinator: Error during commit phase: {e}")
            return "Transaction Commit Failed"
        
        return "Transaction Committed"

def start_coordinator():
    # Define account-to-node mapping
    account_to_node = {
        "A": "http://localhost:8001",  # Node-2 for Account A
        "B": "http://localhost:8002"   # Node-3 for Account B
    }
    
    # Create coordinator instance
    coordinator = Coordinator(account_to_node)
    
    # Start XMLRPC server
    server = SimpleXMLRPCServer(("localhost", 8000))
    server.register_instance(coordinator)
    print("Coordinator (Node-0) started and waiting for requests...")
    server.serve_forever()

if __name__ == "__main__":
    start_coordinator()
