from KL_node_base import NodeBase

def start_node3():
    node = NodeBase(
        account_file="account_B.txt",
        initial_balance=300,
        node_name="Node-3",
        port=8002,
        host="localhost",
        coordinator_endpoint="http://localhost:8000",
        peer_endpoints={"Node 2": "http://localhost:8001"}
    ) # In the cloud, update 'localhost' to the correct internal IP of participant/coordinator
    node.run_server()

if __name__ == "__main__":
    start_node3()
