#!/usr/bin/env python3
"""
Blender Render Farm Client (for Minimal Server)
===============================================
Connects to the minimal Blender render farm server script and processes tasks.

Usage:
  blender --background --python client_minimal.py -- --server SERVER_IP [--port SERVER_PORT] [--name CLIENT_NAME]

Author: Claude (Adjusted by AI Assistant)
License: GPL v3
"""

import argparse
import socket
import json
import os
import sys
import time
import threading
import subprocess
import tempfile
import shutil
from datetime import datetime

# Default settings (Match server defaults if possible)
DEFAULT_SERVER_PORT = 9090
DEFAULT_CLIENT_PORT = 9091  # This port isn't used by client to listen, only reported during registration (and ignored by minimal server)
DEFAULT_CLIENT_NAME = f"Client-{socket.gethostname()}"
HEARTBEAT_INTERVAL = 30  # seconds
RECONNECT_DELAY = 10  # seconds


def parse_args():
    """Parse command line arguments"""
    # Use Blender's "--" separator convention
    argv = sys.argv
    if "--" in argv:
        argv = argv[sys.argv.index("--") + 1 :]
    else:
        argv = []  # No arguments provided after --

    parser = argparse.ArgumentParser(description="Blender Render Farm Client (Minimal)")
    parser.add_argument("--server", required=True, help="Server IP address or hostname")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_SERVER_PORT, help="Server port"
    )
    parser.add_argument(
        "--name", default=DEFAULT_CLIENT_NAME, help="Client name reported to server"
    )
    # Removed --output argument as job-specific dirs are created within a temp/base location
    # Let's use a temporary directory structure relative to the script or cwd
    # parser.add_argument("--output", default="./render_client_work", help="Base directory for temporary job files")

    return parser.parse_args(argv)


class RenderFarmClient:
    def __init__(self, server_ip, server_port, client_name):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        self.base_work_dir = os.path.join(
            tempfile.gettempdir(), f"blender_client_{self.client_name}"
        )
        self.status = "idle"  # idle, assigned, rendering, error
        self.current_job = None  # Dictionary holding current job details
        self.running = True
        self.socket = None
        self.lock = threading.Lock()  # Lock for thread-safe socket access

        # Create base work directory
        os.makedirs(self.base_work_dir, exist_ok=True)
        print(f"Using base work directory: {self.base_work_dir}")

    def connect_to_server(self):
        """Establishes or re-establishes connection to the server."""
        with self.lock:  # Ensure exclusive access for connection attempt
            if self.socket:  # Close existing socket if attempting reconnect
                try:
                    self.socket.close()
                except Exception:
                    pass  # Ignore errors on close
                self.socket = None
            try:
                print(
                    f"Attempting to connect to {self.server_ip}:{self.server_port}..."
                )
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(10.0)  # Connection timeout
                self.socket.connect((self.server_ip, self.server_port))
                self.socket.settimeout(None)  # Reset timeout for normal operations
                print(f"Connected to server.")
                return True
            except Exception as e:
                print(f"Failed to connect to server: {e}")
                self.socket = None
                return False

    def _send_message(self, message):
        """Safely sends a JSON message, attempting connection if needed."""
        with self.lock:
            if not self.socket:
                print("No active connection. Attempting to connect...")
                if not self.connect_to_server():
                    return False  # Failed to connect, cannot send
            try:
                json_message = json.dumps(message)
                self.socket.sendall(json_message.encode("utf-8"))
                # print(f"Sent: {json_message}") # Debug: Log sent messages
                return True
            except socket.error as e:
                print(f"Socket error during send: {e}. Connection lost.")
                self.socket = None  # Mark connection as lost
                return False
            except Exception as e:
                print(f"Error sending message: {e}")
                self.socket = None  # Assume connection lost on other errors too
                return False

    def _receive_message(self, timeout=10.0):
        """Safely receives and decodes a JSON message."""
        with self.lock:
            if not self.socket:
                print("Cannot receive, no active connection.")
                return None
            try:
                self.socket.settimeout(timeout)
                response = self.socket.recv(4096)  # Adjust buffer size if needed
                self.socket.settimeout(None)

                if not response:
                    print("Server disconnected.")
                    self.socket = None  # Mark connection as lost
                    return None

                response_data = json.loads(response.decode("utf-8"))
                # print(f"Received: {response_data}") # Debug: Log received messages
                return response_data

            except socket.timeout:
                print(f"Socket timeout waiting for response (limit: {timeout}s)")
                # Don't kill socket on timeout, maybe just slow network or server busy
                return None
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON response: {e}")
                raw_response = response.decode("utf-8", errors="replace")
                print(f"Raw response data: '{raw_response}'")
                # Don't kill socket, maybe next message is fine
                return None
            except socket.error as e:
                print(f"Socket error during receive: {e}. Connection lost.")
                self.socket = None  # Mark connection as lost
                return None
            except Exception as e:
                print(f"Error receiving message: {e}")
                self.socket = None  # Assume connection lost
                return None

    def register_with_server(self):
        """Register this client with the server."""
        # Attempt connection first
        if not self.socket:
            if not self.connect_to_server():
                return False

        # Send registration message
        message = {
            "type": "register",
            "name": self.client_name,
            # "port": DEFAULT_CLIENT_PORT # Minimal server ignores this
        }
        print(f"Sending registration: {message}")
        if not self._send_message(message):
            print("Failed to send registration message.")
            return False

        # Wait for the single JSON acknowledgement response from the server
        print("Waiting for registration confirmation...")
        response_data = self._receive_message(timeout=15.0)

        if response_data and response_data.get("status") == "ok":
            print(f"Successfully registered as {self.client_name}")
            return True
        elif response_data:
            print(
                f"Registration failed: {response_data.get('message', 'Unknown server error')}"
            )
            # Close socket on registration failure? Maybe server rejected the name.
            # with self.lock:
            #     if self.socket: self.socket.close(); self.socket = None
            return False
        else:
            print("No valid response received from server during registration.")
            # Connection likely failed, _receive_message should have set self.socket to None
            return False

    def send_heartbeat(self):
        """Send a heartbeat message to the server."""
        # _send_message handles connection check
        message = {
            "type": "heartbeat",
            "name": self.client_name,  # Crucial: Server identifies client by name+IP
            "status": self.status,
            # Optional fields (Minimal server ignores these in heartbeat):
            # "job_id": self.current_job["id"] if self.current_job else None,
            # "progress": self.current_job["progress"] if self.current_job else 0,
        }

        if not self._send_message(message):
            print("Failed to send heartbeat.")
            return False  # Let main loop handle potential reconnection

        # Receive acknowledgement (server should send {"status": "ok"})
        response_data = self._receive_message(timeout=5.0)
        if response_data and response_data.get("status") == "ok":
            # print("Heartbeat acknowledged.") # Reduce noise
            return True
        elif response_data:
            print(
                f"Heartbeat rejected or error: {response_data.get('message', 'Unknown')}"
            )
            # If heartbeat is rejected, might mean server dropped registration?
            # Consider triggering re-registration attempt? For now, just log.
            return False
        else:
            # Failed to receive ack, indicates connection issue
            print("Did not receive heartbeat acknowledgement.")
            return False

    def request_job(self):
        """Request a job from the server."""
        # _send_message handles connection check
        message = {"type": "job_request", "name": self.client_name}

        print("Requesting job...")
        if not self._send_message(message):
            print("Failed to send job request.")
            return None

        print("Waiting for job assignment...")
        response_data = self._receive_message(
            timeout=10.0
        )  # Server might take time to assign

        if not response_data:
            print("No response received for job request.")
            return None  # Let main loop retry later

        status = response_data.get("status")
        print(f"Received job response status: {status}")

        if status == "ok":
            # --- Job Assigned ---
            job_data = {
                "id": response_data.get("job_id"),
                "frames": response_data.get("frames", []),
                "blend_file_name": response_data.get("blend_file_name"),
                "output_format": response_data.get("output_format"),  # Informational
                "file_size": response_data.get("file_size", 0),
            }

            # Validate essential data
            if not all(
                [
                    job_data["id"],
                    job_data["frames"],
                    job_data["blend_file_name"],
                    job_data["file_size"] > 0,
                ]
            ):
                print("Error: Incomplete job data received from server.")
                return None

            # Prepare local directory for the job
            job_dir = os.path.join(self.base_work_dir, job_data["id"])
            try:
                os.makedirs(job_dir, exist_ok=True)
            except Exception as e:
                print(f"Error creating job directory {job_dir}: {e}")
                return None  # Cannot proceed without job directory

            blend_path = os.path.join(job_dir, job_data["blend_file_name"])

            print(
                f"Job assigned: {job_data['id']} ({len(job_data['frames'])} frames). Blend: '{job_data['blend_file_name']}' ({job_data['file_size']} bytes)"
            )

            # --- Blend File Transfer ---
            # 1. Send READY signal to server (Required by minimal server)
            print("Sending READY signal to server for blend file...")
            try:
                with self.lock:
                    if not self.socket:
                        return None  # Connection dropped
                    self.socket.sendall(b"READY")
            except Exception as e:
                print(f"Error sending READY signal: {e}")
                self.socket = None  # Assume connection broken
                return None

            # 2. Receive the blend file data
            print(f"Receiving blend file -> {blend_path}")
            bytes_received = 0
            try:
                with open(blend_path, "wb") as f:
                    # Acquire lock only for socket operations inside loop
                    expected_size = job_data["file_size"]
                    transfer_start_time = time.time()
                    while bytes_received < expected_size:
                        chunk_size = min(
                            65536, expected_size - bytes_received
                        )  # Read up to 64k chunks
                        with self.lock:
                            if not self.socket:
                                raise ConnectionError(
                                    "Socket closed during file transfer"
                                )
                            self.socket.settimeout(60.0)  # Generous timeout per chunk
                            chunk = self.socket.recv(chunk_size)
                            self.socket.settimeout(None)  # Reset after successful recv
                        if not chunk:
                            raise ConnectionError(
                                "Server disconnected during blend file transfer"
                            )
                        f.write(chunk)
                        bytes_received += len(chunk)
                        # Optional: Add progress feedback for large files
                        # print(f"Received {bytes_received}/{expected_size} bytes...", end='\r')

                    transfer_time = time.time() - transfer_start_time
                    print(
                        f"Received {bytes_received} bytes in {transfer_time:.2f}s. Blend file saved."
                    )

                if bytes_received != expected_size:
                    print(
                        f"Error: Received file size ({bytes_received}) does not match expected size ({expected_size})"
                    )
                    # Decide whether to proceed or fail
                    try:
                        os.remove(blend_path)
                        print("Removed incomplete blend file.")
                    except OSError:
                        pass
                    return None  # Fail the job request

            except Exception as e:
                print(f"Error receiving blend file: {e}")
                if isinstance(e, socket.timeout):
                    print("Timeout receiving file chunk.")
                self.socket = None  # Assume connection broken
                # Clean up partial file
                if os.path.exists(blend_path):
                    try:
                        os.remove(blend_path)
                    except OSError:
                        pass
                return None

            # --- Job Data Finalization ---
            job_data["blend_path"] = blend_path
            job_data["job_dir"] = job_dir
            job_data["progress"] = 0
            job_data["start_time"] = time.time()

            print(f"Successfully received job details and blend file.")
            self.status = "assigned"  # Mark client as assigned before returning job
            return job_data

        elif status == "no_work":
            print("Server reported no work available.")
            return None
        elif status == "busy":
            print(
                "Server reported client is busy (status mismatch?). Setting self to idle."
            )
            self.status = "idle"
            return None
        else:  # error or unknown status
            print(
                f"Server returned error or unexpected status '{status}': {response_data.get('message', 'N/A')}"
            )
            return None

    def process_job(self, job):
        """Process a rendering job using Blender executable."""
        if not job:
            return

        self.current_job = job
        self.status = "rendering"
        # self.completed_frames = [] # Redundant, but ensure clear

        blend_path = job["blend_path"]
        frames = job["frames"]
        job_dir = job["job_dir"]
        total_frames = len(frames)
        job_id = job["id"]

        print(f"--- Starting Job {job_id} ({total_frames} frames) ---")
        print(f"   Blend file: {blend_path}")
        print(f"   Output dir: {job_dir}")
        print(f"   Frames: {frames}")
        print(f"------------------------------------------------")

        completed_count = 0
        errors_count = 0
        for i, frame in enumerate(frames):
            if not self.running:
                print(f"Stopping job {job_id} - client shutting down.")
                break

            self.current_job["progress"] = (
                (completed_count / total_frames) * 100 if total_frames > 0 else 0
            )

            try:
                # Define output path *pattern* for Blender (Blender adds frame number/extension)
                # Using job_dir ensures outputs are isolated per job.
                output_filename_pattern = f"frame_{frame:04d}_"  # Consistent pattern
                render_output_path_pattern = os.path.join(
                    job_dir, output_filename_pattern
                )

                # Find Blender executable
                # Use 'blender' assuming it's in PATH, or specify full path if needed
                blender_executable = "blender"
                # Check if running inside Blender already? sys.executable might be Blender
                # if "blender" in sys.executable.lower(): blender_executable = sys.executable

                # Construct Blender command
                cmd = [
                    blender_executable,
                    "-b",  # Background mode
                    blend_path,  # Input blend file (the downloaded copy)
                    "-o",  # Output path pattern
                    render_output_path_pattern,
                    "-f",  # Render specific frame
                    str(frame),
                    # "--render-format",    # Optionally force format, but usually rely on .blend settings
                    # job["output_format"].split('_')[0], # e.g., PNG from PNG_RGB_8
                    # "-E", "CYCLES",      # Optionally force engine
                    # "--",                # End Blender options (useful if path has dashes)
                ]

                print(f"Rendering frame {frame} ({i+1}/{total_frames})...")
                # print(f"Command: {' '.join(cmd)}") # Debug command
                start_render_time = time.time()

                # Execute Blender process
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,  # Decode stdout/stderr as text
                )

                # Stream output for real-time feedback (optional)
                # while True:
                #     output = process.stdout.readline()
                #     if output == '' and process.poll() is not None: break
                #     if output: print(output.strip()) # Process Blender output line
                stdout, stderr = process.communicate()  # Wait for process to finish

                render_time = time.time() - start_render_time

                if process.returncode == 0:
                    print(
                        f"Frame {frame} completed successfully in {render_time:.2f}s."
                    )

                    # --- Find the actual output file ---
                    # Blender should have created a file like 'frame_####_.[ext]'
                    rendered_file_path = None
                    found_file = None
                    for filename in os.listdir(job_dir):
                        if filename.startswith(output_filename_pattern):
                            # Basic check: If it starts with the pattern, assume it's the output
                            # More robust: Check extension based on job["output_format"]?
                            found_file = filename
                            break  # Take the first match

                    if found_file:
                        rendered_file_path = os.path.join(job_dir, found_file)
                        print(f"Found rendered file: {rendered_file_path}")

                        # --- Send frame back to server ---
                        if self.send_frame(job_id, frame, rendered_file_path):
                            completed_count += 1
                            # Optionally delete local file after successful send
                            try:
                                os.remove(rendered_file_path)
                                # print(f"Removed local file: {rendered_file_path}")
                            except Exception as e_rem:
                                print(
                                    f"Warning: Failed to remove local frame file {rendered_file_path}: {e_rem}"
                                )
                        else:
                            print(
                                f"Error: Failed to send frame {frame} to server. Stopping job."
                            )
                            errors_count += 1
                            # Decide if we should stop the whole client or just this job
                            # For now, stop processing this job on send failure
                            break  # Exit frame loop

                    else:
                        print(
                            f"Error: Render finished but could not find output file matching '{output_filename_pattern}*' in {job_dir}"
                        )
                        print("--- Blender stdout ---")
                        print(stdout)
                        print("--- Blender stderr ---")
                        print(stderr)
                        print("----------------------")
                        errors_count += 1
                        # Continue to next frame or stop? Let's continue for now.
                else:
                    # Render failed
                    print(
                        f"Error: Blender failed to render frame {frame} (Return code: {process.returncode}) after {render_time:.2f}s."
                    )
                    print("--- Blender stdout ---")
                    print(stdout)
                    print("--- Blender stderr ---")
                    print(stderr)
                    print("----------------------")
                    errors_count += 1
                    # Stop job on render failure? Or try next frame? Let's try next frame.

            except Exception as e:
                print(f"Critical error processing frame {frame}: {e}")
                import traceback

                traceback.print_exc()
                errors_count += 1
                # Stop job on critical errors
                break

        # --- Job Post-Processing ---
        total_processed = completed_count + errors_count
        print(f"--- Job {job_id} Finished ---")
        print(f"   Successfully rendered and sent: {completed_count}/{total_frames}")
        print(f"   Errors encountered: {errors_count}")
        print(f"---------------------------")

        self.status = "idle"  # Ready for next job
        self.current_job = None

        # Clean up the job directory (blend file, any remaining frames)
        try:
            print(f"Cleaning up job directory: {job_dir}")
            shutil.rmtree(job_dir)
        except Exception as e:
            print(f"Warning: Error cleaning up job directory {job_dir}: {e}")

    def send_frame(self, job_id, frame, file_path):
        """Send a single rendered frame file to the server."""
        if not os.path.exists(file_path):
            print(f"Error: Cannot send frame, file not found: {file_path}")
            return False

        try:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)

            # --- Frame Transfer Protocol ---
            # 1. Send frame metadata
            message = {
                "type": "frame_complete",
                "job_id": job_id,
                "frame": frame,
                "file_name": file_name,
                "file_size": file_size,
            }
            print(
                f"Sending metadata for frame {frame} ({file_name}, {file_size} bytes)..."
            )
            if not self._send_message(message):
                print(f"Failed to send metadata for frame {frame}.")
                return False  # Connection likely lost

            # 2. Wait for server READY signal (Required by minimal server)
            print(f"Waiting for server READY signal for frame {frame} file...")
            ready_signal = None
            try:
                with self.lock:
                    if not self.socket:
                        return False
                    self.socket.settimeout(
                        15.0
                    )  # Wait a bit for server to process metadata & create dir
                    ready_signal = self.socket.recv(1024)
                    self.socket.settimeout(None)
            except socket.timeout:
                print(f"Timeout waiting for server READY signal for frame {frame}.")
                return False
            except Exception as e:
                print(f"Error receiving READY signal for frame {frame}: {e}")
                self.socket = None  # Assume connection broken
                return False

            if ready_signal != b"READY":
                print(
                    f"Error: Server not ready or sent unexpected signal ({ready_signal!r}) instead of READY for frame {frame}."
                )
                # Try to decode potential error message from server
                try:
                    print(f"Server response: {ready_signal.decode()}")
                except:
                    pass
                return False

            # 3. Send the actual file data
            print(f"Server ready. Sending file data for {file_name}...")
            bytes_sent = 0
            try:
                with open(file_path, "rb") as f:
                    transfer_start_time = time.time()
                    while True:
                        chunk = f.read(65536)  # Send in 64k chunks
                        if not chunk:
                            break  # End of file

                        with self.lock:
                            if not self.socket:
                                raise ConnectionError("Socket closed during frame send")
                            # Use sendall to ensure all data in chunk is sent
                            self.socket.sendall(chunk)
                        bytes_sent += len(chunk)
                        # Optional progress: print(f"Sent {bytes_sent}/{file_size} bytes...", end='\r')

                    transfer_time = time.time() - transfer_start_time
                    print(
                        f"Sent {bytes_sent} bytes for frame {frame} in {transfer_time:.2f}s."
                    )

                if bytes_sent != file_size:
                    # This usually indicates an issue reading the local file
                    print(
                        f"Warning: Sent bytes ({bytes_sent}) does not match original file size ({file_size}) for frame {frame}"
                    )
                    # Server will likely reject based on size mismatch anyway

            except Exception as e:
                print(f"Error sending file data for frame {frame}: {e}")
                self.socket = None  # Assume connection broken
                return False

            # 4. Wait for final acknowledgement from server
            print(f"Waiting for final acknowledgement for frame {frame}...")
            response_data = self._receive_message(
                timeout=10.0
            )  # Server might do post-processing

            if response_data and response_data.get("status") == "ok":
                print(f"Successfully sent and acknowledged frame {frame}.")
                return True
            elif response_data:
                print(
                    f"Error: Server failed to acknowledge frame {frame}: {response_data.get('message', 'Unknown server error')}"
                )
                return False
            else:
                print(f"Did not receive final acknowledgement for frame {frame}.")
                # Connection likely lost if no response
                return False

        except Exception as e:
            print(f"Unexpected error during send_frame for frame {frame}: {e}")
            import traceback

            traceback.print_exc()
            return False

    def heartbeat_thread(self):
        """Thread that sends regular heartbeats to maintain connection and status."""
        print("Heartbeat thread started.")
        while self.running:
            try:
                # Heartbeat sending handles connection check internally
                if not self.send_heartbeat():
                    # If heartbeat fails (returns False), connection is likely dead.
                    # The main loop will detect self.socket == None and attempt reconnection/registration.
                    print(
                        "Heartbeat failed, connection likely lost. Main loop will handle."
                    )
                    # Sleep longer after a failure to avoid spamming connection attempts
                    time.sleep(RECONNECT_DELAY)
                else:
                    # Sleep normally after successful heartbeat
                    # Use a loop with shorter sleeps to check self.running more often
                    start_sleep = time.time()
                    while (
                        self.running and time.time() - start_sleep < HEARTBEAT_INTERVAL
                    ):
                        time.sleep(0.5)

            except Exception as e:
                print(f"Heartbeat loop encountered an unexpected error: {e}")
                # Avoid busy-looping on errors within the heartbeat thread itself
                time.sleep(HEARTBEAT_INTERVAL)  # Wait full interval before trying again

        print("Heartbeat thread finished.")

    def run(self):
        """Main client loop: Connect, register, request jobs, process."""
        print("Client starting run loop...")

        # --- Initial Connection & Registration ---
        while self.running:
            if self.register_with_server():
                print("Initial registration successful.")
                break  # Proceed to main loop
            else:
                print(
                    f"Initial registration failed. Retrying in {RECONNECT_DELAY} seconds..."
                )
                # Sleep interruptibly
                start_sleep = time.time()
                while self.running and time.time() - start_sleep < RECONNECT_DELAY:
                    time.sleep(0.5)

        if not self.running:
            print("Client stopped during initial registration.")
            return  # Exit if stopped

        # --- Start Heartbeat Thread ---
        heartbeat_thr = threading.Thread(target=self.heartbeat_thread, daemon=True)
        heartbeat_thr.start()

        # --- Main Work Loop ---
        while self.running:
            try:
                # Check connection status (socket might be None if heartbeat failed)
                with self.lock:
                    is_connected = self.socket is not None

                if not is_connected:
                    print("Connection lost. Attempting to re-register...")
                    if self.register_with_server():
                        print("Re-registration successful.")
                        # If registration worked, connection is back, continue loop
                    else:
                        print(
                            f"Re-registration failed. Retrying in {RECONNECT_DELAY} seconds..."
                        )
                        start_sleep = time.time()
                        while (
                            self.running and time.time() - start_sleep < RECONNECT_DELAY
                        ):
                            time.sleep(0.5)
                        continue  # Skip job request attempt this iteration

                # If connected and idle, request a job
                if self.status == "idle":
                    job = self.request_job()
                    if job:
                        # We have a job, process it (this blocks until job is done)
                        self.process_job(job)
                        # process_job sets status back to 'idle' when finished/failed
                    else:
                        # No job available or error requesting job. Wait before asking again.
                        # request_job already prints messages like "No work available"
                        wait_time = 15  # Wait longer if no work
                        # print(f"Waiting {wait_time}s before next job request...")
                        start_sleep = time.time()
                        while self.running and time.time() - start_sleep < wait_time:
                            time.sleep(0.5)
                elif self.status == "rendering" or self.status == "assigned":
                    # Currently busy, just sleep briefly. Job processing happens in process_job.
                    # Heartbeat thread keeps server updated.
                    time.sleep(1)
                else:  # Error state?
                    print(
                        f"Client in unexpected state: {self.status}. Setting to idle."
                    )
                    self.status = "idle"
                    time.sleep(5)  # Wait a bit before requesting job again

            except KeyboardInterrupt:
                print("\nCtrl+C detected, shutting down client...")
                self.running = False
                break
            except Exception as e:
                print(f"Critical error in main loop: {e}")
                import traceback

                traceback.print_exc()
                # Prevent busy-looping on unexpected errors
                time.sleep(RECONNECT_DELAY)

        # --- Cleanup ---
        print("Shutting down client...")
        self.running = False  # Ensure flag is set for threads

        if heartbeat_thr.is_alive():
            print("Waiting for heartbeat thread to finish...")
            heartbeat_thr.join(timeout=2.0)

        with self.lock:
            if self.socket:
                print("Closing final server connection.")
                try:
                    self.socket.close()
                except Exception as e:
                    print(f"Error closing socket: {e}")
                self.socket = None

        # Optional: Clean up base work directory? Might leave logs/failed jobs
        # try:
        #     if os.path.exists(self.base_work_dir):
        #         print(f"Cleaning up base work directory: {self.base_work_dir}")
        #         shutil.rmtree(self.base_work_dir)
        # except Exception as e:
        #     print(f"Warning: Failed to clean up work directory {self.base_work_dir}: {e}")

        print("Client finished.")


def main():
    """Script entry point."""
    args = parse_args()

    print(f"--- Blender Render Client (Minimal) v1.0 ---")
    print(f"  Name:   {args.name}")
    print(f"  Server: {args.server}:{args.port}")
    print(f"-------------------------------------------")

    client = RenderFarmClient(
        server_ip=args.server,
        server_port=args.port,
        client_name=args.name,
    )

    try:
        client.run()
    except Exception as e:
        print(f"Unhandled exception reached main: {e}")
        import traceback

        traceback.print_exc()
    finally:
        print("Client process exiting.")


if __name__ == "__main__":
    main()
