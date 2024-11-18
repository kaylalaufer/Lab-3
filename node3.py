from node_base import NodeBase
from xmlrpc.server import SimpleXMLRPCServer
import sys

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

    # Register shutdown callback
    def shutdown_server():
        print("Node-2: Stopping XMLRPC server.")
        server.shutdown()  # Stop the XMLRPC server
        sys.exit(0)  # Exit the process

    node.set_shutdown_callback(shutdown_server)
    server.register_instance(node)
    server.register_function(node.shutdown, "shutdown")
    server.logRequests = False  # Disable request logging
    print("Node-3 started and waiting for requests...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\Participant 2 shutting down...")
        node.shutdown()  # Stop the server
        sys.exit(0)
   

if __name__ == "__main__":
    start_node3()