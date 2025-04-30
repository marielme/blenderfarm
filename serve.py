#!/usr/bin/env python3
"""
Simple Socket Server
This script creates a basic socket server that listens for connections,
receives messages, and sends responses.
"""

import socket
import threading
import time


def handle_client(client_socket, client_address):

    # Send a response
    response = f"Hello client!".encode("utf-8")
    client_socket.send(response)
    print(f"Sent response: {response}")

    # Close with a newline for clients that expect a terminator
    client_socket.send(b"\n")


def main():
    # Create a socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Allow reuse of address
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind to all interfaces on port 9090
    server_ip = "0.0.0.0"  # Listen on all available interfaces
    server_port = 9090
    server_socket.bind((server_ip, server_port))

    # Listen for connections (queue up to 5 connections)
    server_socket.listen(5)

    print(f"Server started on {server_ip}:{server_port}")
    print("Waiting for connections...")

    try:
        while True:
            # Accept a connection
            client_socket, client_address = server_socket.accept()
            print("------", client_address)
            # Handle client in a new thread
            client_handler = threading.Thread(
                target=handle_client, args=(client_socket, client_address)
            )
            client_handler.daemon = True
            client_handler.start()

    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
