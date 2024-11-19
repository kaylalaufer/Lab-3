import xmlrpc.client

# Connect to the coordinator and nodes
coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")
node2 = xmlrpc.client.ServerProxy("http://localhost:8001")
node3 = xmlrpc.client.ServerProxy("http://localhost:8002")

def transactions(initial_a=200, initial_b=300, case=0):

    # Initialize Accounts for case simulation
    print(f"Initializing Account A with balance: {initial_a}")
    node2.initialize_account(initial_a)
    print(f"Initializing Account B with balance: {initial_b}")
    node3.initialize_account(initial_b)

    # Set simulation case
    print(f"Setting simulation case to: {case}")
    node2.simulation_case(case)
    node3.simulation_case(case)

    # Transaction 1: Transfer 100 dollars from account A to account B
    transaction_id_1 = "txn1"
    txn1_details = {
        "A": -100,  # Withdraw 100 from Account A
        "B": 100    # Deposit 100 into Account B
    }
    print(f"Executing Transaction 1 (ID: {transaction_id_1})")
    txn1_result = coordinator.execute_transaction(transaction_id_1, txn1_details)
    print(f"Transaction 1 Result: {txn1_result}")

    # Transaction 2: Add a 20% bonus to A and B
    balance_a = node2.get_balance()  # Get Account A balance
    bonus = 0.2 * balance_a
    transaction_id_2 = "txn2"
    txn2_details = {
        "A": bonus,  # Add bonus to Account A
        "B": bonus   # Add same bonus to Account B
    }
    print(f"Executing Transaction 2 (ID: {transaction_id_2}) with bonus: {bonus}")
    txn2_result = coordinator.execute_transaction(transaction_id_2, txn2_details)
    print(f"Transaction 2 Result: {txn2_result}")


if __name__ == "__main__":
    # Case 1a
    print("=== Running Case 1a ===")
    transactions(200, 300, 0)
    # Expected:
    # Transaction 1: Committed
    # Transaction 2: Committed

    # Case 1b
    print("=== Running Case 1b ===")
    transactions(90, 50, 0)
    # Expected:
    # Transaction 1: Aborted
    # Transaction 2: Committed

    coordinator.shutdown()