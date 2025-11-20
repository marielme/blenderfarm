#!/usr/bin/env python3
"""
MicroFarm: Client python script
===============================================
Connects to the minimal Blender render farm server script and processes tasks.

Usage:
  blender --background --python microfarm_client.py -- --server SERVER_IP [--port SERVER_PORT] [--name CLIENT_NAME]

Author: Mariel Martinez + AI tools
License: Apache 2.0
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

    return parser.parse_args(argv)


class RenderFarmClient:
    def __init__(self, server_ip, server_port, client_name):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        
        # Base directory structure:
        # /tmp/blender_farm/[client_name]/[job_id]/
        self.base_work_dir = os.path.join(
            tempfile.gettempdir(), "blender_farm", self.client_name
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
        # No lock needed here as it's called from methods that already hold the lock or run before threading starts
        # with self.lock:  # Ensure exclusive access for connection attempt
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
        # Lock is acquired *before* checking the socket
        with self.lock:
            if not self.socket:
                print("No active connection. Attempting to connect...")
                # Pass self.lock to connect_to_server IF it needed to acquire the lock itself
                # but since connect_to_server is only called here while lock is held, it's okay.
                if not self.connect_to_server():
                    print("Connection attempt failed in send_message.")
                    return False  # Failed to connect, cannot send

            # We should have a socket now if connect_to_server succeeded
            if not self.socket:
                 print("Error: Socket still None after connection attempt in send_message.")
                 return False

            try:
                json_message = json.dumps(message)
                self.socket.sendall(json_message.encode("utf-8"))
                # print(f"Sent: {json_message}") # Debug: Log sent messages
                return True
            except socket.error as e:
                print(f"Socket error during send: {e}. Connection lost.")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Mark connection as lost
                return False
            except Exception as e:
                print(f"Error sending message: {e}")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Assume connection lost on other errors too
                return False

    def _receive_message(self, timeout=10.0):
        """Safely receives and decodes a JSON message."""
        # Lock is acquired *before* checking the socket
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
                    try: # Attempt to close cleanly
                        self.socket.close()
                    except Exception: pass
                    self.socket = None  # Mark connection as lost
                    return None
                    
                # Check if this is a raw READY response
                if response == b"READY":
                    print("Received raw READY signal (not JSON)")
                    return {"status": "ready", "raw_signal": "READY"}
                
                # Try to parse as JSON
                try:
                    response_data = json.loads(response.decode("utf-8"))
                    # print(f"Received: {response_data}") # Debug: Log received messages
                    return response_data
                except json.JSONDecodeError as e:
                    # Special case - if we got "READY", treat it specially
                    if response.strip() == b"READY":
                        print("Received non-JSON READY signal")
                        return {"status": "ready", "raw_signal": "READY"}
                    # Re-raise for regular handling
                    raise

            except socket.timeout:
                print(f"Socket timeout waiting for response (limit: {timeout}s)")
                # Don't kill socket on timeout, maybe just slow network or server busy
                return None
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON response: {e}")
                # Attempt to decode for logging, might fail again
                try:
                    raw_response = response.decode("utf-8", errors="replace")
                    print(f"Raw response data: '{raw_response}'")
                except Exception:
                     print("Could not decode raw response.")
                # Don't kill socket, maybe next message is fine
                return None
            except socket.error as e:
                print(f"Socket error during receive: {e}. Connection lost.")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Mark connection as lost
                return None
            except Exception as e:
                print(f"Error receiving message: {e}")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Assume connection lost
                return None

    def register_with_server(self):
        """Register this client with the server. Assumes lock is NOT held."""
        # Acquire lock for this operation
        with self.lock:
            # Attempt connection first (uses lock internally, but we already hold it)
            if not self.socket:
                if not self.connect_to_server():
                    return False

            # Send registration message (uses lock internally, but we already hold it)
            message = {
                "type": "register",
                "name": self.client_name,
            }
            print(f"Sending registration: {message}")
            # We need to call the underlying send without re-acquiring lock
            try:
                json_message = json.dumps(message)
                self.socket.sendall(json_message.encode("utf-8"))
            except socket.error as e:
                 print(f"Socket error during registration send: {e}. Connection lost.")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False
            except Exception as e:
                 print(f"Error sending registration message: {e}")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False


            # Wait for the single JSON acknowledgement response from the server
            # We need to call the underlying receive without re-acquiring lock
            print("Waiting for registration confirmation...")
            response_data = None
            try:
                self.socket.settimeout(15.0)
                response = self.socket.recv(4096)
                self.socket.settimeout(None)
                if not response:
                    print("Server disconnected during registration confirmation.")
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
                    return False
                response_data = json.loads(response.decode("utf-8"))
            except socket.timeout:
                print("Socket timeout waiting for registration confirmation.")
                # Keep socket open? Maybe server is just slow. Let's keep it open for now.
                return False # Indicate failure for now
            except json.JSONDecodeError as e:
                 print(f"Failed to decode registration JSON response: {e}")
                 try:
                     raw_response = response.decode("utf-8", errors="replace")
                     print(f"Raw registration response data: '{raw_response}'")
                 except Exception:
                     print("Could not decode raw registration response.")
                 # Don't kill socket yet
                 return False # Indicate failure
            except socket.error as e:
                 print(f"Socket error during registration receive: {e}. Connection lost.")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False
            except Exception as e:
                 print(f"Error receiving registration message: {e}")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False
            # ---- End of internal receive ----

            if response_data and response_data.get("status") == "ok":
                print(f"Successfully registered as {self.client_name}")
                # --- KEEP SOCKET OPEN ---
                return True
            elif response_data:
                print(
                    f"Registration failed: {response_data.get('message', 'Unknown server error')}"
                )
                # Close socket on registration failure? Server might expect disconnect.
                try: self.socket.close()
                except Exception: pass
                self.socket = None
                return False
            else:
                print("No valid response received from server during registration.")
                # Connection likely failed if response_data is None
                # Socket should have been set to None in the exception handling
                return False

    def send_heartbeat(self):
        """Send a heartbeat message to the server."""
        # _send_message handles connection check and locking
        message = {
            "type": "heartbeat",
            "name": self.client_name,
            "status": self.status,
        }

        if not self._send_message(message):
            print("Failed to send heartbeat.")
            # self.socket should be None if _send_message failed
            return False

        # Receive acknowledgement (server should send {"status": "ok"})
        # _receive_message handles locking
        response_data = self._receive_message(timeout=10.0) # Increase timeout slightly
        if response_data and response_data.get("status") == "ok":
            # print("Heartbeat acknowledged.") # Reduce noise
            return True
        elif response_data:
            print(
                f"Heartbeat rejected or error: {response_data.get('message', 'Unknown')}"
            )
            # If heartbeat is rejected, might mean server dropped registration?
            # Let's close the connection and force re-registration
            with self.lock:
                if self.socket:
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
            return False
        else:
            # Failed to receive ack, indicates connection issue
            print("Did not receive heartbeat acknowledgement.")
            # self.socket should be None if _receive_message failed or returned None
            return False

    def request_job(self):
        """Request a job from the server."""
        # _send_message handles connection check and locking
        message = {"type": "job_request", "name": self.client_name}

        print("Requesting job...")
        if not self._send_message(message):
            print("Failed to send job request.")
            return None

        # _receive_message handles locking
        print("Waiting for job assignment...")
        response_data = self._receive_message(
            timeout=20.0 # Increase timeout, server might be busy finding/preparing job
        )

        if not response_data:
            print("No response received for job request.")
            # self.socket should be None if _receive_message failed
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
                "camera": response_data.get("camera", "Camera"),  # Camera to use for rendering, default to "Camera"
            }

            # Validate essential data
            if not all(
                [
                    job_data["id"],
                    isinstance(job_data["frames"], list), # Ensure it's a list
                    job_data["blend_file_name"],
                    isinstance(job_data["file_size"], int) and job_data["file_size"] > 0,
                ]
            ):
                print(f"Error: Incomplete job data received from server: {job_data}")
                # Don't close socket, maybe next request works
                return None

             # Check if frames list is empty - server should ideally not assign empty jobs
            if not job_data["frames"]:
                print("Error: Job assigned with an empty frame list.")
                return None


            # Extract project name from blend file name (remove extension and timestamp)
            project_name = job_data["blend_file_name"].split('_')[0]
            
            # Prepare local directory for the job with improved structure:
            # /tmp/blender_farm/[client_name]/[project_name]/[job_id]/
            project_dir = os.path.join(self.base_work_dir, project_name)
            job_dir = os.path.join(project_dir, job_data["id"])
            
            try:
                # Create both project and job directories
                os.makedirs(project_dir, exist_ok=True)
                os.makedirs(job_dir, exist_ok=True)
                print(f"Created job directory: {job_dir}")
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
            # Use lock for socket access
            ready_sent = False
            try:
                with self.lock:
                    if not self.socket:
                        print("Cannot send READY, no connection.")
                        return None  # Connection dropped
                    self.socket.sendall(b"READY")
                    ready_sent = True
            except Exception as e:
                print(f"Error sending READY signal: {e}")
                with self.lock: # Ensure lock is held while modifying socket
                   if self.socket:
                       try: self.socket.close()
                       except Exception: pass
                       self.socket = None
                return None

            if not ready_sent: # Should not happen unless lock fails, but safety check
                 print("Failed to send READY signal.")
                 return None

            # 2. Receive the blend file data
            print(f"Receiving blend file -> {blend_path}")
            bytes_received = 0
            try:
                # Open file first
                with open(blend_path, "wb") as f:
                    expected_size = job_data["file_size"]
                    transfer_start_time = time.time()
                    while bytes_received < expected_size:
                        chunk_size = min(
                            65536, expected_size - bytes_received
                        )
                        # Acquire lock ONLY for the socket operation
                        chunk = None
                        with self.lock:
                            if not self.socket:
                                raise ConnectionError(
                                    "Socket closed during file transfer"
                                )
                            self.socket.settimeout(60.0)  # Generous timeout per chunk
                            try:
                                chunk = self.socket.recv(chunk_size)
                            finally: # Ensure timeout is reset even on error
                                try:
                                    self.socket.settimeout(None)
                                except Exception: # Socket might already be closed
                                     pass

                        if not chunk:
                            raise ConnectionError(
                                "Server disconnected during blend file transfer (received 0 bytes)"
                            )
                        f.write(chunk) # Write outside the lock
                        bytes_received += len(chunk)
                        # Optional: Add progress feedback for large files
                        # print(f"Received {bytes_received}/{expected_size} bytes...", end='\r')

                    transfer_time = time.time() - transfer_start_time
                    print( # Print final message outside loop
                        f"\nReceived {bytes_received} bytes in {transfer_time:.2f}s. Blend file saved."
                    )

                # Verify size after closing the file
                if bytes_received != expected_size:
                    print(
                        f"Error: Received file size ({bytes_received}) does not match expected size ({expected_size})"
                    )
                    try:
                        os.remove(blend_path)
                        print("Removed incomplete blend file.")
                    except OSError:
                        pass
                    # Close connection? Server might be sending garbage. Let's close it.
                    with self.lock:
                         if self.socket:
                             try: self.socket.close()
                             except Exception: pass
                             self.socket = None
                    return None  # Fail the job request

            except Exception as e:
                print(f"Error receiving blend file: {e}")
                if isinstance(e, (socket.timeout, ConnectionError)):
                    print("Timeout or connection error receiving file chunk.")
                # Assume connection is broken on any error during receive
                with self.lock:
                   if self.socket:
                       try: self.socket.close()
                       except Exception: pass
                       self.socket = None
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
            # --- KEEP SOCKET OPEN ---
            self.status = "assigned"  # Mark client as assigned before returning job
            return job_data

        elif status == "no_work":
            print("Server reported no work available.")
            # Keep socket open, just no work right now
            return None
        elif status == "busy":
            print(
                "Server reported client is busy (status mismatch?). Setting self to idle."
            )
            self.status = "idle"
             # Keep socket open
            return None
        else:  # error or unknown status
            print(
                f"Server returned error or unexpected status '{status}': {response_data.get('message', 'N/A')}"
            )
            # Assume server error means we should disconnect/retry registration
            with self.lock:
                if self.socket:
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
            return None

    def process_job(self, job):
        """Process a rendering job using Blender executable."""
        if not job:
            return

        self.current_job = job
        self.status = "rendering"

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
        original_stdout = sys.stdout # Store original stdout
        original_stderr = sys.stderr # Store original stderr

        for i, frame in enumerate(frames):
            if not self.running:
                print(f"Stopping job {job_id} - client shutting down.")
                break

            # Update progress based on count before starting next frame
            self.current_job["progress"] = (
                (completed_count / total_frames) * 100 if total_frames > 0 else 0
            )
            # Send a heartbeat before starting a potentially long render? Optional.
            # self.send_heartbeat() # Could be too frequent if frames are fast

            try:
                # Define output path *pattern* for Blender (Blender adds frame number/extension)
                output_filename_pattern = f"frame_{frame:04d}_"
                render_output_path_pattern = os.path.join(
                    job_dir, output_filename_pattern
                )

                # Find Blender executable more reliably
                blender_executable = None
                if hasattr(sys, 'executable') and 'blender' in sys.executable.lower():
                    # If this script is run via 'blender --python script.py', sys.executable is Blender
                     blender_executable = sys.executable
                else:
                    # Try finding 'blender' in PATH (less reliable for multiple versions)
                    blender_executable = shutil.which("blender")

                if not blender_executable:
                     print("Error: Could not find Blender executable.")
                     errors_count += 1
                     break # Cannot continue job without blender

                # Construct Blender command
                # Create a temporary Python script to execute Blender
                temp_script_path = os.path.join(job_dir, f"render_frame_{frame}.py")
                
                # Convert paths to use forward slashes to avoid escaping issues
                safe_blend_path = blend_path.replace('\\', '/')
                safe_output_path = render_output_path_pattern.replace('\\', '/')
                
                # Get the camera from job data
                camera = job.get("camera", "Camera")
                
                with open(temp_script_path, "w") as f:
                    f.write(f"""
import bpy
import sys
import os

# Load the blend file
bpy.ops.wm.open_mainfile(filepath="{safe_blend_path}")

# Force PNG output format
bpy.context.scene.render.image_settings.file_format = 'PNG'
bpy.context.scene.render.image_settings.color_mode = 'RGBA'
bpy.context.scene.render.image_settings.compression = 15

# Set the output path
bpy.context.scene.render.filepath = "{safe_output_path}"

# Set the active camera
camera_name = "{camera}"
if camera_name in bpy.data.objects:
    camera_obj = bpy.data.objects[camera_name]
    if camera_obj.type == 'CAMERA':
        bpy.context.scene.camera = camera_obj
        print(f"Using camera: {{camera_name}}")
    else:
        print(f"Object '{{camera_name}}' exists but is not a camera. Using default camera.")
else:
    print(f"Warning: Camera '{{camera_name}}' not found. Using default camera.")

# Render the frame
bpy.context.scene.frame_set({frame})
bpy.ops.render.render(write_still=True)
""")
                
                # Use Blender executable directly to run the Python script
                # Force use of known Blender paths rather than relying on sys.executable
                # Hard-code paths to common Blender locations
                blender_candidates = [
                    "/Applications/Blender.app/Contents/MacOS/Blender",  # macOS
                    "C:\\Program Files\\Blender Foundation\\Blender\\blender.exe",  # Windows
                    "/usr/bin/blender"  # Linux
                ]
                
                blender_path = None
                for path in blender_candidates:
                    if os.path.exists(path):
                        blender_path = path
                        break
                
                # Fall back to PATH if none of the standard locations work
                if not blender_path:
                    blender_path = shutil.which("blender")
                    
                # If we're running the script directly with Blender (and not finding the Blender executable)
                # we can use the BLENDER_PATH environment variable if set
                if not blender_path and "BLENDER_PATH" in os.environ:
                    potential_path = os.environ["BLENDER_PATH"]
                    if os.path.exists(potential_path):
                        blender_path = potential_path
                
                # Final fallback - if we're running inside Blender, use the current executable
                # Be very careful with this as it can lead to recursion
                if not blender_path and hasattr(sys, 'executable') and os.path.basename(sys.executable).lower().startswith('blender'):
                    blender_path = sys.executable
                
                if not blender_path:
                    raise Exception("Could not find Blender executable for rendering")
                
                # Print detected executable for debugging
                print(f"Initially detected Blender at: {blender_path}")
                
                # On macOS, ensure we're using the actual Blender executable, not Python
                if "python" in blender_path.lower():
                    # We're trying to use Python instead of Blender, adjust path for macOS
                    if os.path.exists("/Applications/Blender.app/Contents/MacOS/Blender"):
                        print("Correcting Python path to use actual Blender executable")
                        blender_path = "/Applications/Blender.app/Contents/MacOS/Blender"
                        print(f"Using Blender at: {blender_path}")
                
                # Construct command using Blender executable
                cmd = [
                    blender_path,
                    "--background",         # Background mode
                    "--python",             # Run a Python script
                    temp_script_path
                ]

                # Print message for rendering
                print(f"Rendering frame {frame} ({i+1}/{total_frames}) using Blender: {blender_path}")
                # print(f"Command: {' '.join(cmd)}") # Debug command
                start_render_time = time.time()

                # Execute Blender process
                # Capture output to avoid polluting farm client log too much unless error
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True, # Use text=True instead of universal_newlines
                    encoding='utf-8', # Specify encoding
                    errors='replace'  # Handle potential decoding errors
                )

                stdout, stderr = process.communicate() # Wait and capture all output
                render_time = time.time() - start_render_time

                if process.returncode == 0:
                    print(
                        f"Frame {frame} completed successfully in {render_time:.2f}s."
                    )
                    # Check stderr for potential warnings even if returncode is 0
                    if stderr:
                        print(f"--- Blender stderr warnings for frame {frame} ---")
                        print(stderr.strip())
                        print("-------------------------------------------")

                    # --- Find the actual output file ---
                    rendered_file_path = None
                    found_file = None
                    try:
                        for filename in os.listdir(job_dir):
                            # Check if it starts with the pattern AND has a common image extension
                            # This is slightly more robust than just checking the start
                            if filename.startswith(output_filename_pattern):
                                # Simple check for common extensions - refine if needed
                                if any(filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.exr', '.tif', '.tiff']):
                                     found_file = filename
                                     break
                    except FileNotFoundError:
                         print(f"Error: Job directory {job_dir} not found after render.")
                         errors_count += 1
                         continue # Try next frame

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
                                f"Error: Failed to send frame {frame} to server. Stopping job processing."
                            )
                            errors_count += 1
                            # Stop processing this specific job on send failure
                            self.status = "error" # Mark client status
                            break  # Exit frame loop

                    else:
                        print(
                            f"Error: Render finished (code 0) but could not find output file matching '{output_filename_pattern}*' in {job_dir}"
                        )
                        print("--- Blender stdout ---")
                        print(stdout.strip())
                        print("--- Blender stderr ---")
                        print(stderr.strip())
                        print("----------------------")
                        errors_count += 1
                        # Continue to next frame? Or stop job? Let's continue for now.

                else: # Render failed (non-zero return code)
                    print(
                        f"Error: Blender failed to render frame {frame} (Return code: {process.returncode}) after {render_time:.2f}s."
                    )
                    print("--- Blender stdout ---")
                    print(stdout.strip())
                    print("--- Blender stderr ---")
                    print(stderr.strip()) # stderr often has the actual error message
                    print("----------------------")
                    errors_count += 1
                    # Stop job on render failure? Or try next frame?
                    # Let's try next frame for now, maybe it was a one-off issue.

            except Exception as e:
                # Catch unexpected errors during the loop for a specific frame
                print(f"Critical error processing frame {frame}: {e}")
                import traceback
                traceback.print_exc()
                errors_count += 1
                # Stop the job on critical errors during frame processing
                self.status = "error"
                break

        # --- Job Post-Processing ---
        total_processed = completed_count + errors_count
        print(f"--- Job {job_id} Finished ---")
        print(f"   Successfully rendered and sent: {completed_count}/{total_frames}")
        print(f"   Errors encountered: {errors_count}")
        print(f"---------------------------")

        # Update final progress if job finished
        if self.current_job: # Check if job wasn't cleared by another thread
             self.current_job["progress"] = (completed_count / total_frames) * 100 if total_frames > 0 else 100

        # Set status based on outcome
        if errors_count > 0 and completed_count < total_frames:
             self.status = "error" # Mark error if frames failed and job incomplete
        else:
             self.status = "idle"  # Ready for next job if all frames attempted (even if some failed but were skipped)

        self.current_job = None # Clear current job reference

        # Clean up the job directory (blend file, any remaining frames)
        if os.path.exists(job_dir):
            try:
                print(f"Cleaning up job directory: {job_dir}")
                shutil.rmtree(job_dir)
            except Exception as e:
                print(f"Warning: Error cleaning up job directory {job_dir}: {e}")
        else:
             print(f"Job directory {job_dir} not found for cleanup.")


    def _continue_send_frame_after_ready(self, job_id, frame, file_path, file_size):
        """Continue the frame sending process after READY signal is received."""
        file_name = os.path.basename(file_path)
        
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

                    # Acquire lock only for the socket operation
                    with self.lock:
                        if not self.socket:
                            raise ConnectionError("Socket closed during frame send")
                        # Use sendall to ensure all data in chunk is sent
                        self.socket.sendall(chunk)
                    bytes_sent += len(chunk) # Increment outside lock
                    # Optional progress: print(f"Sent {bytes_sent}/{file_size} bytes...", end='\r')

                transfer_time = time.time() - transfer_start_time
                print( # Print final message outside loop
                    f"\nSent {bytes_sent} bytes for frame {frame} in {transfer_time:.2f}s."
                )

            # Verify amount sent vs file size (usually indicates local read issue if mismatched)
            if bytes_sent != file_size:
                print(
                    f"Warning: Sent bytes ({bytes_sent}) does not match original file size ({file_size}) for frame {frame}. Local file issue?"
                )
                # Server will likely reject based on size mismatch anyway.
                # Don't necessarily close connection here, but return failure.
                return False

        except Exception as e:
            print(f"Error sending file data for frame {frame}: {e}")
             # Assume connection is broken on any error during send
            with self.lock:
                if self.socket:
                   try: self.socket.close()
                   except Exception: pass
                   self.socket = None
            return False

        # 4. Wait for final acknowledgement from server
        print(f"Waiting for final acknowledgement for frame {frame}...")
        # _receive_message handles locking
        response_data = self._receive_message(timeout=30.0) # Increase timeout, server might save/verify

        if response_data and response_data.get("status") == "ok":
            print(f"Successfully sent and acknowledged frame {frame}.")
            # --- KEEP SOCKET OPEN ---
            return True
        elif response_data:
            print(
                f"Error: Server failed to acknowledge frame {frame}: {response_data.get('message', 'Unknown server error')}"
            )
             # Close socket on server error
            with self.lock:
                 if self.socket:
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
            return False
        else:
            print(f"Did not receive final acknowledgement for frame {frame}.")
            # Connection likely lost if no response or error during receive
            # self.socket should be None now
            return False
    
    def send_frame(self, job_id, frame, file_path):
        """Send a single rendered frame file to the server."""
        if not os.path.exists(file_path):
            print(f"Error: Cannot send frame, file not found: {file_path}")
            return False

        try:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            print(f"Preparing to send frame {frame} ({file_name}, {file_size} bytes)...")


            # --- Frame Transfer Protocol ---
            # 1. Send frame metadata
            message = {
                "type": "frame_complete",
                "job_id": job_id,
                "frame": frame,
                "file_name": file_name,
                "file_size": file_size,
            }
            print(f"Sending metadata...")
            # _send_message handles connection check and locking
            if not self._send_message(message):
                print(f"Failed to send metadata for frame {frame}.")
                # self.socket should be None now
                return False

            # 2. Wait for server READY signal
            # We can use _receive_message now as we've updated it to handle "READY"
            print(f"Waiting for server READY signal...")
            ready_signal_data = self._receive_message(timeout=20.0) # Increased timeout
            
            # Check if we got the special READY response
            if ready_signal_data and (
                ready_signal_data.get("status") == "ready" or
                ready_signal_data.get("raw_signal") == "READY"
            ):
                print("Server READY signal received via _receive_message")
                ready_ok = True
                # Skip the direct socket receive since we already got READY
                return self._continue_send_frame_after_ready(job_id, frame, file_path, file_size)
                
            # If not, fall back to direct socket receive (legacy approach)

            ready_ok = False
            try:
                with self.lock:
                    if not self.socket:
                         print("Cannot receive READY, no connection.")
                         return False

                    # Server sends "READY" as raw bytes (not JSON)
                    self.socket.settimeout(20.0)
                    ready_signal_bytes = self.socket.recv(1024) # Expect raw bytes
                    self.socket.settimeout(None)

                    if ready_signal_bytes == b"READY":
                        print("Server READY received.")
                        ready_ok = True
                    elif not ready_signal_bytes:
                        print("Server disconnected while waiting for READY signal.")
                        try: self.socket.close()
                        except Exception: pass
                        self.socket = None
                        return False
                    else:
                         # Try to log whatever we received for debugging
                         print(f"Error: Server not ready or sent unexpected signal instead of READY for frame {frame}.")
                         print(f"Raw response: {ready_signal_bytes!r}")
                         try:
                             print(f"Response as string: {ready_signal_bytes.decode('utf-8', errors='replace')}")
                         except Exception as e:
                             print(f"Could not decode response: {e}")
                             
                         # Don't close the socket - it might be a server error we can recover from
                         print("Will continue with frame sending anyway...")
                         # Force ready_ok to true to try to continue
                         ready_ok = True

            except socket.timeout:
                print(f"Timeout waiting for server READY signal for frame {frame}.")
                # Don't kill socket on timeout, try again later? For now, fail send.
                return False
            except socket.error as e:
                 print(f"Socket error receiving READY signal for frame {frame}: {e}")
                 if self.socket:
                     try: self.socket.close()
                     except Exception: pass
                     self.socket = None
                 return False
            except Exception as e:
                print(f"Error receiving READY signal for frame {frame}: {e}")
                if self.socket:
                     try: self.socket.close()
                     except Exception: pass
                     self.socket = None # Assume connection broken
                return False

            if not ready_ok:
                 # Error message printed above
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

                        # Acquire lock only for the socket operation
                        with self.lock:
                            if not self.socket:
                                raise ConnectionError("Socket closed during frame send")
                            # Use sendall to ensure all data in chunk is sent
                            self.socket.sendall(chunk)
                        bytes_sent += len(chunk) # Increment outside lock
                        # Optional progress: print(f"Sent {bytes_sent}/{file_size} bytes...", end='\r')

                    transfer_time = time.time() - transfer_start_time
                    print( # Print final message outside loop
                        f"\nSent {bytes_sent} bytes for frame {frame} in {transfer_time:.2f}s."
                    )

                # Verify amount sent vs file size (usually indicates local read issue if mismatched)
                if bytes_sent != file_size:
                    print(
                        f"Warning: Sent bytes ({bytes_sent}) does not match original file size ({file_size}) for frame {frame}. Local file issue?"
                    )
                    # Server will likely reject based on size mismatch anyway.
                    # Don't necessarily close connection here, but return failure.
                    return False

            except Exception as e:
                print(f"Error sending file data for frame {frame}: {e}")
                 # Assume connection is broken on any error during send
                with self.lock:
                    if self.socket:
                       try: self.socket.close()
                       except Exception: pass
                       self.socket = None
                return False

            # 4. Wait for final acknowledgement from server
            print(f"Waiting for final acknowledgement for frame {frame}...")
            # _receive_message handles locking
            response_data = self._receive_message(timeout=30.0) # Increase timeout, server might save/verify

            if response_data and response_data.get("status") == "ok":
                print(f"Successfully sent and acknowledged frame {frame}.")
                # --- KEEP SOCKET OPEN ---
                return True
            elif response_data:
                print(
                    f"Error: Server failed to acknowledge frame {frame}: {response_data.get('message', 'Unknown server error')}"
                )
                 # Close socket on server error
                with self.lock:
                     if self.socket:
                        try: self.socket.close()
                        except Exception: pass
                        self.socket = None
                return False
            else:
                print(f"Did not receive final acknowledgement for frame {frame}.")
                # Connection likely lost if no response or error during receive
                # self.socket should be None now
                return False

        except Exception as e:
            # Catch-all for unexpected errors in the send_frame logic
            print(f"Unexpected error during send_frame for frame {frame}: {e}")
            import traceback
            traceback.print_exc()
             # Close socket on unexpected error
            with self.lock:
                if self.socket:
                   try: self.socket.close()
                   except Exception: pass
                   self.socket = None
            return False


    def heartbeat_thread(self):
        """Thread that sends regular heartbeats to maintain connection and status."""
        print("Heartbeat thread started.")
        while self.running:
            last_heartbeat_time = time.monotonic()
            try:
                # Check connection status BEFORE sending heartbeat
                # Use lock briefly to check socket status
                is_connected = False
                with self.lock:
                     is_connected = self.socket is not None

                if is_connected:
                     # print("Attempting heartbeat...") # Debug
                     if not self.send_heartbeat():
                          # send_heartbeat sets self.socket to None on failure
                          print("Heartbeat failed. Connection likely lost.")
                          # No need to sleep extra here, main loop handles reconnect logic
                     # else:
                          # print("Heartbeat successful.") # Debug
                          pass # Sleep normally after success below
                else:
                     # print("Skipping heartbeat, no connection.") # Debug
                     # If not connected, don't try to send heartbeat.
                     # Main loop should be attempting registration.
                     pass # Sleep normally below

                # Wait for the next interval, checking self.running periodically
                while self.running and (time.monotonic() - last_heartbeat_time < HEARTBEAT_INTERVAL):
                    time.sleep(1.0) # Check every second

            except Exception as e:
                print(f"Heartbeat loop encountered an unexpected error: {e}")
                import traceback
                traceback.print_exc()
                # Avoid busy-looping on errors within the heartbeat thread itself
                time.sleep(HEARTBEAT_INTERVAL) # Wait full interval before trying again

        print("Heartbeat thread finished.")


    def run(self):
        """Main client loop: Connect, register, request jobs, process."""
        print("Client starting run loop...")

        # --- Start Heartbeat Thread ---
        # Start it early so it's ready once registered
        heartbeat_thr = threading.Thread(target=self.heartbeat_thread, daemon=True)
        heartbeat_thr.start()

        # --- Main Connection and Work Loop ---
        while self.running:
            try:
                # Check connection status using the lock
                is_connected = False
                with self.lock:
                     is_connected = self.socket is not None

                if not is_connected:
                    # --- Attempt Registration ---
                    print("Connection lost or not established. Attempting registration...")
                    if self.register_with_server():
                        print("Registration successful.")
                        # Give a brief moment for server/network before first job request
                        time.sleep(1)
                    else:
                        print(
                            f"Registration attempt failed. Retrying in {RECONNECT_DELAY} seconds..."
                        )
                        # Sleep interruptibly
                        start_sleep = time.time()
                        while self.running and time.time() - start_sleep < RECONNECT_DELAY:
                            time.sleep(0.5)
                        continue # Go back to start of loop to check connection/register again

                # --- If Connected ---
                # We should have a connection here if registration was successful
                # Double check status before requesting job
                if self.status == "idle":
                    job = self.request_job()
                    if job:
                        # We have a job, process it (this blocks until job is done)
                        self.process_job(job)
                        # process_job sets status back to 'idle' or 'error' when finished/failed
                    else:
                        # No job available or error requesting job.
                        # request_job() handles connection loss and sets self.socket=None
                        # If socket is still alive, just wait before asking again.
                        is_still_connected = False
                        with self.lock:
                             is_still_connected = self.socket is not None

                        if is_still_connected:
                             wait_time = 15 # Wait longer if no work
                             print(f"No job assigned or available. Waiting {wait_time}s...")
                             start_sleep = time.time()
                             while self.running and time.time() - start_sleep < wait_time:
                                 time.sleep(0.5)
                        # If connection was lost during request_job, the loop will restart and try registration

                elif self.status == "rendering" or self.status == "assigned":
                    # Currently busy, just sleep briefly. Job processing happens in process_job.
                    # Heartbeat thread keeps server updated.
                    time.sleep(1)
                elif self.status == "error":
                     print("Client is in error state. Check logs. Setting to idle to retry.")
                     self.status = "idle"
                     time.sleep(10) # Wait longer after an error
                else: # Unknown state?
                    print(
                        f"Client in unexpected state: {self.status}. Setting to idle."
                    )
                    self.status = "idle"
                    time.sleep(5)

            except KeyboardInterrupt:
                print("\nCtrl+C detected, shutting down client...")
                self.running = False
                # No break needed, loop condition self.running will be false
            except Exception as e:
                print(f"Critical error in main loop: {e}")
                import traceback
                traceback.print_exc()
                # Prevent busy-looping on unexpected errors
                self.status = "error" # Mark as error
                # Close socket on critical errors
                with self.lock:
                    if self.socket:
                       try: self.socket.close()
                       except Exception: pass
                       self.socket = None
                # Wait before potentially restarting the loop/registration
                time.sleep(RECONNECT_DELAY)


        # --- Cleanup ---
        print("Shutting down client...")
        # Ensure flag is set for threads (redundant here, but good practice)
        self.running = False

        # Wait for heartbeat thread to finish
        if heartbeat_thr.is_alive():
            print("Waiting for heartbeat thread to finish...")
            heartbeat_thr.join(timeout=2.0)
            if heartbeat_thr.is_alive():
                 print("Warning: Heartbeat thread did not exit cleanly.")

        # Close final connection if open
        with self.lock:
            if self.socket:
                print("Closing final server connection.")
                try:
                    # Optionally send a 'disconnecting' message?
                    # json_message = json.dumps({"type": "disconnect", "name": self.client_name})
                    # self.socket.sendall(json_message.encode("utf-8"))
                    self.socket.shutdown(socket.SHUT_RDWR) # Graceful shutdown
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

    print(f"--- Blender Render Client (Minimal) v1.1 ---") # Version bump
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
    # Check if running inside Blender
    try:
        import bpy
        print("Script running inside Blender.")
        # Potentially add Blender-specific setup here if needed
    except ImportError:
        print("Script running outside Blender.")
        # bpy module not available

    # Ensure script arguments are handled correctly when run with 'blender --python'
    # The parse_args function already handles the '--' separator.

    main()

# --- END OF FILE ---