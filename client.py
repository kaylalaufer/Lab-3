import xmlrpc.client
import time

# Connect to the coordinator and nodes
coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")
transaction_id = 0

def shutdown_coordinator():
    #coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")
    try:
        print("Client: Sending shutdown request to the coordinator...")
        time.sleep(10) # Delay to allow smoother clean up
        coordinator.shutdown()
        print("Client: Shutdown request sent successfully.")
    except Exception as e:
        print(f"Client: Failed to send shutdown request: {e}")

def initialize_nodes(account_a=200, account_b=300):
    """Initialize node balances via the Coordinator."""
    try:
        response_a = coordinator.initialize_node("A", account_a)
        print(f"Account A: {account_a}")
        response_b = coordinator.initialize_node("B", account_b)
        print(f"Account B: {account_b}")
        return True
    except Exception as e:
        print(f"Failed to contact coordinator: {e}.")
        return False

def set_simulation_case(case_number):
    """Set a simulation case via the Coordinator."""
    print(f"Client: Setting simulation case {case_number}.")
    try:
        response = coordinator.set_simulation_case(case_number)
        return True
    except Exception as e:
        print(f"Failed to contact coordinator: {e}.")
        return False

def execute_transaction(txn_a, txn_b):
    """Execute a transaction via the Coordinator."""
    global transaction_id
    transaction_id = transaction_id + 1
    txn_id = "txn" + str(transaction_id)
    print(f"\nClient: Executing transaction {txn_id}:")
    txn_details = {"A": txn_a, "B": txn_b}  # Transfer 100 from A to B
    print(f"  A: {txn_a}\n  B: {txn_b}")
    try:
        result = coordinator.execute_transaction(txn_id, txn_details)
        print(f"Client: Transaction result: {result}")
        return True
    except Exception as e:
        print(f"Failed to contact coordinator: {e}.")
        return False

def scenarios(accout_a, account_b, case_number=0):
    if not initialize_nodes(accout_a, account_b):
        return False
    if case_number != 0:
        if not set_simulation_case(case_number):
            return False

    if not execute_transaction(-100, 100):
        return False

    if case_number != 0:
        time.sleep(15)

    try: 
        balance_a = coordinator.get_account_balance("A")
        bonus = balance_a * 0.2
        execute_transaction(bonus, bonus)
        return True
    except Exception as e:
        print(f"Failed to contact coordinator: {e}.")
        return False


if __name__ == "__main__":
    # Case 1a
    print("=== Running Case 1a ===\n")
    scenarios(200, 300, 0)

    print("\n=== Running Case 1b ===\n")
    scenarios(90, 50, 0)

    print("\n=== Running Case 1c.i ===\n")
    scenarios(200, 300, 1)

    time.sleep(15)

    print("\n=== Running Case 1c.ii ===\n")
    scenarios(200, 300, 2)

   