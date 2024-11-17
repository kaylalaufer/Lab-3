import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class Coordinator:
    def __init__(self, account_to_node):
        """
        :param account_to_node: Dictionary mapping accounts to participant RPC endpoints.
        """
        self.account_to_node = account_to_node
        self.participants = {account: ServerProxy(endpoint) for account, endpoint in account_to_node.items()}

    def execute_transaction(self, transaction_id, transactions):
        """
        Execute a distributed transaction involving multiple accounts using the 2PC protocol.
        :param transaction_id: A unique ID for the transaction.
        :param transactions: A dictionary of account -> amount.
                             Example: {"A": -500, "B": 500}
        :return: Commit or Abort decision.
        """
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

def start_coordinator():
    account_to_node = {
        "A": "http://localhost:8001",  # Node-2 for Account A
        "B": "http://localhost:8002"   # Node-3 for Account B
    }
    coordinator = Coordinator(account_to_node)
    server = SimpleXMLRPCServer(("localhost", 8000))
    server.register_instance(coordinator)
    server.logRequests = False  # Disable request logging
    print("Coordinator (Node-0) started and waiting for requests...")
    server.serve_forever()

if __name__ == "__main__":
    start_coordinator()
