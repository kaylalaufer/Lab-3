import xmlrpc.client

# Connect to the coordinator and nodes
coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")
node2 = xmlrpc.client.ServerProxy("http://localhost:8001")
node3 = xmlrpc.client.ServerProxy("http://localhost:8002")

def transaction1(case=0):

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

def transaction2(case=0):

    # Set simulation case
    print(f"Setting simulation case to: {case}")
    node2.simulation_case(case)
    node3.simulation_case(case)

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

def shutdown_coordinator():
    #coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")
    try:
        print("Client: Sending shutdown request to the coordinator...")
        coordinator.shutdown()
        print("Client: Shutdown request sent successfully.")
    except Exception as e:
        print(f"Client: Failed to send shutdown request: {e}")

if __name__ == "__main__":
    # Case 1a
    print("=== Running Case 1a ===\n")

    # Initialize Accounts for case simulation
    print(f"Initializing Account A with balance: 200")
    node2.initialize_account(200)
    print(f"Initializing Account B with balance: 300\n")
    node3.initialize_account(300)
    print("\nTxn 1\n")
    transaction1(0)
    print("\nTxn 2\n")
    transaction2(0)

    # Expected:
    # Transaction 1: Committed
    # Transaction 2: Committed

    # Case 1b
    print("\n=== Running Case 1b ===\n")
        
    # Initialize Accounts for case simulation
    print(f"Initializing Account A with balance: 90")
    node2.initialize_account(90)
    print(f"Initializing Account B with balance: 50\n")
    node3.initialize_account(50)

    print("\nTxn 1\n")
    transaction1(0)
    print("\nTxn 2\n")
    transaction2(0)
    # Expected:
    # Transaction 1: Aborted
    # Transaction 2: Committed

     # Case 1c
    print("\n=== Running Case 1c ===\n")
        
    # Initialize Accounts for case simulation
    print(f"Initializing Account A with balance: 200")
    node2.initialize_account(200)
    print(f"Initializing Account B with balance: 300")
    node3.initialize_account(300)

    print("\nTxn 1 - Node 2 fails in Prepare phase\n")
    transaction1(1)

    print("\nTxn 1 - Node 2 fails in Commit phase\n")
    transaction1(2)
    # Expected:
    # Transaction 1: Aborted
    # Transaction 2: Committed

    shutdown_coordinator()