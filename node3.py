from node_base import NodeBase
from xmlrpc.server import SimpleXMLRPCServer
import threading
import time


class Node3(NodeBase):
    def __init__(self):
        super().__init__(
            account_file="account_B.txt",
            initial_balance=300,
            node_name="Node-3",
            coordinator_endpoint="http://localhost:8000",
            peer_endpoints={"Node 2": "http://localhost:8001"}
        )

    def shutdown(self):
        """Stop the server without forcing process exit."""
        print(f"{self.node_name}: Remote shutdown requested.")
        self.server_running = False  # Signal server loop to stop
        print(f"{self.node_name}: Server stopping gracefully.")
        return "Shutdown initiated"  # Explicitly return a non-None value


def start_node3():
    node = Node3()

    # Custom server loop to manage the server_running flag
    def server_thread():
        with SimpleXMLRPCServer(("localhost", 8002)) as server:
            server.register_instance(node)
            server.logRequests = False
            print("Node-3 started and waiting for requests...")

            # Run the server loop while server_running is True
            while node.server_running:
                server.handle_request()  # Process one request at a time

            print("Node-3: Server has stopped.")

    # Start the server in a separate thread
    server_thread_instance = threading.Thread(target=server_thread, daemon=True)
    server_thread_instance.start()

    try:
        # Keep the main thread alive to detect signals (e.g., KeyboardInterrupt)
        while node.server_running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nNode-3: Shutdown signal received. Exiting...")
        node.shutdown()


if __name__ == "__main__":
    start_node3()
