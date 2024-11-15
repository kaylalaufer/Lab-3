import xmlrpc.client

# Connect to the coordinator
coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")

# Transaction 1: Withdraw from Account A
result_withdraw = coordinator.execute_transaction("txn123", "A", -500)
print(result_withdraw)  # Expected: "Transaction Committed" or "Transaction Aborted"

# Transaction 2: Deposit to Account B
result_deposit = coordinator.execute_transaction("txn123", "B", 100)
print(result_deposit)  # Expected: "Transaction Committed" or "Transaction Aborted"
