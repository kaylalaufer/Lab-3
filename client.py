import xmlrpc.client

coordinator = xmlrpc.client.ServerProxy("http://localhost:8000")

# Transaction: Withdraw $500 from Account A, Deposit $500 into Account B
result = coordinator.execute_transaction("txn1", {"A": -500, "B": 500})
print(result)  # Expected: "Transaction Aborted"

# Transaction: Withdraw $100 from Account A, Deposit 200 into Account B
result2 = coordinator.execute_transaction("txn2", {"A": -100, "B": 200})
print(result2)  # Expected: "Transaction Committed"
