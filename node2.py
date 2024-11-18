from node_base import NodeBase
from xmlrpc.server import SimpleXMLRPCServer
import sys

class Node2(NodeBase):
    def __init__(self):
        super().__init__(
            account_file="account_A.txt",
            initial_balance=200,
            node_name="Node-2",
            coordinator_endpoint="http://localhost:8000",
            peer_endpoints={"Node 3": "http://localhost:8002"}
        )

def start_node2():
    server = SimpleXMLRPCServer(("localhost", 8001), allow_none=True)
    node = Node2()

    # Register shutdown callback
    def shutdown_server():
        print("Node-2: Stopping XMLRPC server.")
        server.shutdown()  # Stop the XMLRPC server
        print("Node-2: Exiting process.")
        sys.exit(0)  # Exit the process

    # Set the shutdown callback in NodeBase
    node.set_shutdown_callback(shutdown_server)

    # Register the node's methods
    server.register_instance(node)

    # Explicitly register the `shutdown` method for XMLRPC
    server.register_function(node.shutdown, "shutdown")
    server.logRequests = False  # Disable request logging
    print("Node-2 started and waiting for requests...")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nNode-2 shutting down due to keyboard interrupt...")
        node.shutdown()

if __name__ == "__main__":
    start_node2()
