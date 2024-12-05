from node_base import NodeBase

def start_node2():
    node = NodeBase(
        account_file="account_A.txt",
        initial_balance=200,
        node_name="Node-2",
        port=8001,
        host="localhost",
        coordinator_endpoint="http://localhost:8000",
        peer_endpoints={"Node 3": "http://localhost:8002"}
    ) # In the cloud, update 'localhost' to the correct internal IP of participant/coordinator
    node.run_server()

if __name__ == "__main__":
    start_node2()
