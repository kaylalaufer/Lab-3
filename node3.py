from node_base import NodeBase
from xmlrpc.server import SimpleXMLRPCServer

class Node3(NodeBase):
    def __init__(self):
        super().__init__(
            account_file="account_B.txt", 
            initial_balance=300, 
            node_name="Node-3", 
            coordinator_endpoint="http://localhost:8000", 
            peer_endpoints={"Node 2":"http://localhost:8001"})

def start_node3():
    server = SimpleXMLRPCServer(("localhost", 8002))
    node = Node3()
    server.register_instance(node)
    server.logRequests = False

    try:
        print("Node-3 started and waiting for requests...")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nNode-3: Shutdown signal received. Exiting...")
        node.shutdown()

if __name__ == "__main__":
    start_node3()