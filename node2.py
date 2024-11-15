from node_base import NodeBase
from xmlrpc.server import SimpleXMLRPCServer

class Node2(NodeBase):
    def __init__(self):
        super().__init__(
            account_file="account_A.txt", 
            initial_balance=200, 
            node_name="Node-2", 
            coordinator_endpoint="http://localhost:8000", 
            peer_endpoints={"Node 3":"http://localhost:8002"})

def start_node2():
    server = SimpleXMLRPCServer(("localhost", 8001))
    node = Node2()
    server.register_instance(node)
    print("Node-2 started and waiting for requests...")
    server.serve_forever()

if __name__ == "__main__":
    start_node2()