"""
MicroFarm: Blender Render Farm Client add-on
============================================
A add-on version of the command-line client script for MicroFarm render farm.
Connects to the MicroFarm server and processes render tasks within Blender.

Author: Mariel Martinez + AI tools
License: GPL v3
"""

bl_info = {
    "name": "MicroFarm: Render Farm Client",
    "author": "Mariel Martinez, Using AI tools",
    "version": (1, 0, 0),
    "blender": (4, 0, 0),
    "location": "Render Properties > Render Farm Client",
    "description": "Client for MicroFarm Render Farm",
    "warning": "Requires a running MicroFarm server",
    "doc_url": "",
    "category": "Render",
}

import bpy
import socket
import json
import os
import sys
import time
import threading
import tempfile
import shutil
from datetime import datetime
import traceback

# Default settings
DEFAULT_SERVER_PORT = 9090
HEARTBEAT_INTERVAL = 30  # seconds
RECONNECT_DELAY = 10  # seconds
CLIENT_LOCK = threading.RLock()  # Lock for thread safety
_timer = None
_running = False
_client = None

# --- Client Implementation ---

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
        self.log_message = "Client initialized"
        self.progress = 0.0
        self.current_frame = 0
        self.total_frames = 0
        self.last_activity = "Initialized"

        # Create base work directory
        os.makedirs(self.base_work_dir, exist_ok=True)
        print(f"Using base work directory: {self.base_work_dir}")
        
    def add_log(self, message):
        """Add a log message and update the UI"""
        self.log_message = message
        print(f"MicroFarm Client: {message}")
        
    def connect_to_server(self):
        """Establishes or re-establishes connection to the server."""
        if self.socket:  # Close existing socket if attempting reconnect
            try:
                self.socket.close()
            except Exception:
                pass  # Ignore errors on close
            self.socket = None
        try:
            self.add_log(f"Connecting to {self.server_ip}:{self.server_port}...")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10.0)  # Connection timeout
            self.socket.connect((self.server_ip, self.server_port))
            self.socket.settimeout(None)  # Reset timeout for normal operations
            self.add_log(f"Connected to server")
            return True
        except Exception as e:
            self.add_log(f"Failed to connect to server: {e}")
            self.socket = None
            return False

    def _send_message(self, message):
        """Safely sends a JSON message, attempting connection if needed."""
        # Lock is acquired *before* checking the socket
        with self.lock:
            if not self.socket:
                self.add_log("No active connection. Attempting to connect...")
                if not self.connect_to_server():
                    self.add_log("Connection attempt failed in send_message.")
                    return False

            # We should have a socket now if connect_to_server succeeded
            if not self.socket:
                 self.add_log("Error: Socket still None after connection attempt in send_message.")
                 return False

            try:
                json_message = json.dumps(message)
                self.socket.sendall(json_message.encode("utf-8"))
                return True
            except socket.error as e:
                self.add_log(f"Socket error during send: {e}. Connection lost.")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Mark connection as lost
                return False
            except Exception as e:
                self.add_log(f"Error sending message: {e}")
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
                self.add_log("Cannot receive, no active connection.")
                return None
            try:
                self.socket.settimeout(timeout)
                response = self.socket.recv(4096)  # Adjust buffer size if needed
                self.socket.settimeout(None)

                if not response:
                    self.add_log("Server disconnected.")
                    try: # Attempt to close cleanly
                        self.socket.close()
                    except Exception: pass
                    self.socket = None  # Mark connection as lost
                    return None
                    
                # Check if this is a raw READY response
                if response == b"READY":
                    self.add_log("Received raw READY signal (not JSON)")
                    return {"status": "ready", "raw_signal": "READY"}
                
                # Try to parse as JSON
                try:
                    response_data = json.loads(response.decode("utf-8"))
                    return response_data
                except json.JSONDecodeError as e:
                    # Special case - if we got "READY", treat it specially
                    if response.strip() == b"READY":
                        self.add_log("Received non-JSON READY signal")
                        return {"status": "ready", "raw_signal": "READY"}
                    # Re-raise for regular handling
                    raise

            except socket.timeout:
                self.add_log(f"Socket timeout waiting for response (limit: {timeout}s)")
                # Don't kill socket on timeout, maybe just slow network or server busy
                return None
            except json.JSONDecodeError as e:
                self.add_log(f"Failed to decode JSON response: {e}")
                # Attempt to decode for logging, might fail again
                try:
                    raw_response = response.decode("utf-8", errors="replace")
                    self.add_log(f"Raw response data: '{raw_response}'")
                except Exception:
                     self.add_log("Could not decode raw response.")
                # Don't kill socket, maybe next message is fine
                return None
            except socket.error as e:
                self.add_log(f"Socket error during receive: {e}. Connection lost.")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Mark connection as lost
                return None
            except Exception as e:
                self.add_log(f"Error receiving message: {e}")
                try: # Attempt to close cleanly
                    self.socket.close()
                except Exception: pass
                self.socket = None  # Assume connection lost
                return None

    def register_with_server(self):
        """Register this client with the server."""
        with self.lock:
            # Attempt connection first
            if not self.socket:
                if not self.connect_to_server():
                    return False

            # Send registration message
            message = {
                "type": "register",
                "name": self.client_name,
            }
            self.add_log(f"Sending registration: {message}")
            
            # We need to call the underlying send without re-acquiring lock
            try:
                json_message = json.dumps(message)
                self.socket.sendall(json_message.encode("utf-8"))
            except socket.error as e:
                 self.add_log(f"Socket error during registration send: {e}. Connection lost.")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False
            except Exception as e:
                 self.add_log(f"Error sending registration message: {e}")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False

            # Wait for the single JSON acknowledgement response from the server
            self.add_log("Waiting for registration confirmation...")
            response_data = None
            try:
                self.socket.settimeout(15.0)
                response = self.socket.recv(4096)
                self.socket.settimeout(None)
                if not response:
                    self.add_log("Server disconnected during registration confirmation.")
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
                    return False
                response_data = json.loads(response.decode("utf-8"))
            except socket.timeout:
                self.add_log("Socket timeout waiting for registration confirmation.")
                # Keep socket open? Maybe server is just slow. Let's keep it open for now.
                return False # Indicate failure for now
            except json.JSONDecodeError as e:
                 self.add_log(f"Failed to decode registration JSON response: {e}")
                 try:
                     raw_response = response.decode("utf-8", errors="replace")
                     self.add_log(f"Raw registration response data: '{raw_response}'")
                 except Exception:
                     self.add_log("Could not decode raw registration response.")
                 # Don't kill socket yet
                 return False # Indicate failure
            except socket.error as e:
                 self.add_log(f"Socket error during registration receive: {e}. Connection lost.")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False
            except Exception as e:
                 self.add_log(f"Error receiving registration message: {e}")
                 try: self.socket.close()
                 except Exception: pass
                 self.socket = None
                 return False

            if response_data and response_data.get("status") == "ok":
                self.add_log(f"Successfully registered as {self.client_name}")
                # --- KEEP SOCKET OPEN ---
                return True
            elif response_data:
                self.add_log(f"Registration failed: {response_data.get('message', 'Unknown server error')}")
                # Close socket on registration failure? Server might expect disconnect.
                try: self.socket.close()
                except Exception: pass
                self.socket = None
                return False
            else:
                self.add_log("No valid response received from server during registration.")
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
            self.add_log("Failed to send heartbeat.")
            # self.socket should be None if _send_message failed
            return False

        # Receive acknowledgement (server should send {"status": "ok"})
        # _receive_message handles locking
        response_data = self._receive_message(timeout=10.0) # Increase timeout slightly
        if response_data and response_data.get("status") == "ok":
            # Heartbeat acknowledged (no need to log every time)
            self.last_activity = f"Heartbeat at {datetime.now().strftime('%H:%M:%S')}"
            return True
        elif response_data:
            self.add_log(f"Heartbeat rejected or error: {response_data.get('message', 'Unknown')}")
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
            self.add_log("Did not receive heartbeat acknowledgement.")
            # self.socket should be None if _receive_message failed or returned None
            return False

    def request_job(self):
        """Request a job from the server."""
        # _send_message handles connection check and locking
        message = {"type": "job_request", "name": self.client_name}

        self.add_log("Requesting job...")
        if not self._send_message(message):
            self.add_log("Failed to send job request.")
            return None

        # _receive_message handles locking
        self.add_log("Waiting for job assignment...")
        response_data = self._receive_message(
            timeout=20.0 # Increase timeout, server might be busy finding/preparing job
        )

        if not response_data:
            self.add_log("No response received for job request.")
            # self.socket should be None if _receive_message failed
            return None  # Let main loop retry later

        status = response_data.get("status")
        self.add_log(f"Received job response status: {status}")

        if status == "ok":
            # --- Job Assigned ---
            job_data = {
                "id": response_data.get("job_id"),
                "frames": response_data.get("frames", []),
                "blend_file_name": response_data.get("blend_file_name"),
                "output_format": response_data.get("output_format"),  # Informational
                "file_size": response_data.get("file_size", 0),
                "camera": response_data.get("camera", "Camera"),  # Camera to use for rendering
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
                self.add_log(f"Error: Incomplete job data received from server: {job_data}")
                # Don't close socket, maybe next request works
                return None

             # Check if frames list is empty - server should ideally not assign empty jobs
            if not job_data["frames"]:
                self.add_log("Error: Job assigned with an empty frame list.")
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
                self.add_log(f"Created job directory: {job_dir}")
            except Exception as e:
                self.add_log(f"Error creating job directory {job_dir}: {e}")
                return None  # Cannot proceed without job directory

            blend_path = os.path.join(job_dir, job_data["blend_file_name"])

            self.add_log(f"Job assigned: {job_data['id']} ({len(job_data['frames'])} frames). Blend: '{job_data['blend_file_name']}' ({job_data['file_size']} bytes)")

            # --- Blend File Transfer ---
            # 1. Send READY signal to server (Required by minimal server)
            self.add_log("Sending READY signal to server for blend file...")
            # Use lock for socket access
            ready_sent = False
            try:
                with self.lock:
                    if not self.socket:
                        self.add_log("Cannot send READY, no connection.")
                        return None  # Connection dropped
                    self.socket.sendall(b"READY")
                    ready_sent = True
            except Exception as e:
                self.add_log(f"Error sending READY signal: {e}")
                with self.lock: # Ensure lock is held while modifying socket
                   if self.socket:
                       try: self.socket.close()
                       except Exception: pass
                       self.socket = None
                return None

            if not ready_sent: # Should not happen unless lock fails, but safety check
                 self.add_log("Failed to send READY signal.")
                 return None

            # 2. Receive the blend file data
            self.add_log(f"Receiving blend file -> {blend_path}")
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
                        
                        # Update progress
                        self.progress = (bytes_received / expected_size) * 100.0
                        self.add_log(f"Received {bytes_received}/{expected_size} bytes ({self.progress:.1f}%)")

                    transfer_time = time.time() - transfer_start_time
                    self.add_log(f"Received {bytes_received} bytes in {transfer_time:.2f}s. Blend file saved.")

                # Verify size after closing the file
                if bytes_received != expected_size:
                    self.add_log(f"Error: Received file size ({bytes_received}) does not match expected size ({expected_size})")
                    try:
                        os.remove(blend_path)
                        self.add_log("Removed incomplete blend file.")
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
                self.add_log(f"Error receiving blend file: {e}")
                if isinstance(e, (socket.timeout, ConnectionError)):
                    self.add_log("Timeout or connection error receiving file chunk.")
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
            
            # Reset for new job
            self.total_frames = len(job_data["frames"])
            self.current_frame = 0
            self.progress = 0.0

            self.add_log(f"Successfully received job details and blend file.")
            # --- KEEP SOCKET OPEN ---
            self.status = "assigned"  # Mark client as assigned before returning job
            return job_data

        elif status == "no_work":
            self.add_log("Server reported no work available.")
            # Keep socket open, just no work right now
            return None
        elif status == "busy":
            self.add_log("Server reported client is busy (status mismatch?). Setting self to idle.")
            self.status = "idle"
             # Keep socket open
            return None
        else:  # error or unknown status
            self.add_log(f"Server returned error or unexpected status '{status}': {response_data.get('message', 'N/A')}")
            # Assume server error means we should disconnect/retry registration
            with self.lock:
                if self.socket:
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
            return None

    def render_frame(self, frame, blend_path, output_path, camera):
        """Renders a single frame using the Blender API directly"""
        try:
            self.add_log(f"Rendering frame {frame} using internal Blender API")
            
            # Save current scene state
            original_filepath = bpy.context.scene.render.filepath
            original_frame = bpy.context.scene.frame_current
            
            try:
                # Load the .blend file
                bpy.ops.wm.open_mainfile(filepath=blend_path)
                
                # Set the output path
                bpy.context.scene.render.filepath = output_path
                
                # Set active camera if specified
                if camera and camera in bpy.data.objects:
                    camera_obj = bpy.data.objects[camera]
                    if camera_obj.type == 'CAMERA':
                        bpy.context.scene.camera = camera_obj
                        self.add_log(f"Using camera: {camera}")
                    else:
                        self.add_log(f"Object '{camera}' exists but is not a camera. Using default camera.")
                else:
                    self.add_log(f"Warning: Camera '{camera}' not found. Using default camera.")
                
                # Set frame and render
                bpy.context.scene.frame_set(frame)
                bpy.ops.render.render(write_still=True)
                
                # Find the rendered file
                render_dir = os.path.dirname(output_path)
                base_name = os.path.basename(output_path)
                
                # Blender adds frame number and extension
                rendered_file = None
                if os.path.exists(output_path + '.png'):
                    rendered_file = output_path + '.png'
                elif os.path.exists(output_path + f'{frame:04d}.png'):
                    rendered_file = output_path + f'{frame:04d}.png'
                
                self.add_log(f"Frame {frame} rendered successfully")
                return rendered_file
            finally:
                # Restore original scene state
                bpy.context.scene.render.filepath = original_filepath
                bpy.context.scene.frame_current = original_frame
                
        except Exception as e:
            self.add_log(f"Error rendering frame {frame}: {e}")
            traceback.print_exc()
            return None

    def process_job(self, job):
        """Process a rendering job using direct Blender API access."""
        if not job:
            return
        
        self.current_job = job
        self.status = "rendering"
        
        blend_path = job["blend_path"]
        frames = job["frames"]
        job_dir = job["job_dir"]
        total_frames = len(frames)
        job_id = job["id"]
        camera = job.get("camera", "Camera")
        
        self.add_log(f"--- Starting Job {job_id} ({total_frames} frames) ---")
        self.add_log(f"   Blend file: {blend_path}")
        self.add_log(f"   Output dir: {job_dir}")
        self.add_log(f"   Frames: {frames}")
        self.add_log(f"   Camera: {camera}")
        
        completed_count = 0
        errors_count = 0
        
        for i, frame in enumerate(frames):
            if not self.running:
                self.add_log(f"Stopping job {job_id} - client shutting down.")
                break
                
            # Update progress 
            self.current_frame = frame
            self.progress = (i / total_frames) * 100 if total_frames > 0 else 0
            
            try:
                # Define output path pattern
                output_filename = f"frame_{frame:04d}_"
                render_output_path = os.path.join(job_dir, output_filename)
                
                # Render the frame using Blender API directly
                rendered_file_path = self.render_frame(frame, blend_path, render_output_path, camera)
                
                if rendered_file_path and os.path.exists(rendered_file_path):
                    self.add_log(f"Frame {frame} completed successfully.")
                    
                    # Send frame back to server
                    if self.send_frame(job_id, frame, rendered_file_path):
                        completed_count += 1
                        # Optionally delete local file after successful send
                        try:
                            os.remove(rendered_file_path)
                            self.add_log(f"Removed local file: {rendered_file_path}")
                        except Exception as e_rem:
                            self.add_log(f"Warning: Failed to remove local frame file {rendered_file_path}: {e_rem}")
                    else:
                        self.add_log(f"Error: Failed to send frame {frame} to server. Stopping job processing.")
                        errors_count += 1
                        # Stop processing this specific job on send failure
                        self.status = "error" # Mark client status
                        break  # Exit frame loop
                else:
                    self.add_log(f"Error: Render failed for frame {frame}")
                    errors_count += 1
                    
            except Exception as e:
                # Catch unexpected errors during the loop for a specific frame
                self.add_log(f"Critical error processing frame {frame}: {e}")
                traceback.print_exc()
                errors_count += 1
                # Stop the job on critical errors during frame processing
                self.status = "error"
                break
                
        # --- Job Post-Processing ---
        total_processed = completed_count + errors_count
        self.add_log(f"--- Job {job_id} Finished ---")
        self.add_log(f"   Successfully rendered and sent: {completed_count}/{total_frames}")
        self.add_log(f"   Errors encountered: {errors_count}")
        
        # Update final progress if job finished
        if self.current_job:  # Check if job wasn't cleared by another thread
            self.current_job["progress"] = (completed_count / total_frames) * 100 if total_frames > 0 else 100
            
        # Set status based on outcome
        if errors_count > 0 and completed_count < total_frames:
            self.status = "error"  # Mark error if frames failed and job incomplete
        else:
            self.status = "idle"  # Ready for next job if all frames attempted
            
        self.current_job = None  # Clear current job reference
        
        # Clean up the job directory (blend file, any remaining frames)
        if os.path.exists(job_dir):
            try:
                self.add_log(f"Cleaning up job directory: {job_dir}")
                shutil.rmtree(job_dir)
            except Exception as e:
                self.add_log(f"Warning: Error cleaning up job directory {job_dir}: {e}")

    def _continue_send_frame_after_ready(self, job_id, frame, file_path, file_size):
        """Continue the frame sending process after READY signal is received."""
        file_name = os.path.basename(file_path)
        
        # 3. Send the actual file data
        self.add_log(f"Server ready. Sending file data for {file_name}...")
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

                transfer_time = time.time() - transfer_start_time
                self.add_log(f"Sent {bytes_sent} bytes for frame {frame} in {transfer_time:.2f}s.")

            # Verify amount sent vs file size (usually indicates local read issue if mismatched)
            if bytes_sent != file_size:
                self.add_log(f"Warning: Sent bytes ({bytes_sent}) does not match original file size ({file_size}) for frame {frame}. Local file issue?")
                # Server will likely reject based on size mismatch anyway.
                # Don't necessarily close connection here, but return failure.
                return False

        except Exception as e:
            self.add_log(f"Error sending file data for frame {frame}: {e}")
             # Assume connection is broken on any error during send
            with self.lock:
                if self.socket:
                   try: self.socket.close()
                   except Exception: pass
                   self.socket = None
            return False

        # 4. Wait for final acknowledgement from server
        self.add_log(f"Waiting for final acknowledgement for frame {frame}...")
        # _receive_message handles locking
        response_data = self._receive_message(timeout=30.0) # Increase timeout, server might save/verify

        if response_data and response_data.get("status") == "ok":
            self.add_log(f"Successfully sent and acknowledged frame {frame}.")
            # --- KEEP SOCKET OPEN ---
            return True
        elif response_data:
            self.add_log(f"Error: Server failed to acknowledge frame {frame}: {response_data.get('message', 'Unknown server error')}")
             # Close socket on server error
            with self.lock:
                 if self.socket:
                    try: self.socket.close()
                    except Exception: pass
                    self.socket = None
            return False
        else:
            self.add_log(f"Did not receive final acknowledgement for frame {frame}.")
            # Connection likely lost if no response or error during receive
            # self.socket should be None now
            return False

    def send_frame(self, job_id, frame, file_path):
        """Send a single rendered frame file to the server."""
        if not os.path.exists(file_path):
            self.add_log(f"Error: Cannot send frame, file not found: {file_path}")
            return False

        try:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            self.add_log(f"Preparing to send frame {frame} ({file_name}, {file_size} bytes)...")

            # --- Frame Transfer Protocol ---
            # 1. Send frame metadata
            message = {
                "type": "frame_complete",
                "job_id": job_id,
                "frame": frame,
                "file_name": file_name,
                "file_size": file_size,
            }
            self.add_log(f"Sending metadata...")
            # _send_message handles connection check and locking
            if not self._send_message(message):
                self.add_log(f"Failed to send metadata for frame {frame}.")
                # self.socket should be None now
                return False

            # 2. Wait for server READY signal
            # We can use _receive_message now as we've updated it to handle "READY"
            self.add_log(f"Waiting for server READY signal...")
            ready_signal_data = self._receive_message(timeout=20.0) # Increased timeout
            
            # Check if we got the special READY response
            if ready_signal_data and (
                ready_signal_data.get("status") == "ready" or
                ready_signal_data.get("raw_signal") == "READY"
            ):
                self.add_log("Server READY signal received via _receive_message")
                ready_ok = True
                # Skip the direct socket receive since we already got READY
                return self._continue_send_frame_after_ready(job_id, frame, file_path, file_size)
                
            # If not, fall back to direct socket receive (legacy approach)
            ready_ok = False
            try:
                with self.lock:
                    if not self.socket:
                         self.add_log("Cannot receive READY, no connection.")
                         return False

                    # Server sends "READY" as raw bytes (not JSON)
                    self.socket.settimeout(20.0)
                    ready_signal_bytes = self.socket.recv(1024) # Expect raw bytes
                    self.socket.settimeout(None)

                    if ready_signal_bytes == b"READY":
                        self.add_log("Server READY received.")
                        ready_ok = True
                    elif not ready_signal_bytes:
                        self.add_log("Server disconnected while waiting for READY signal.")
                        try: self.socket.close()
                        except Exception: pass
                        self.socket = None
                        return False
                    else:
                         # Try to log whatever we received for debugging
                         self.add_log(f"Error: Server not ready or sent unexpected signal instead of READY for frame {frame}.")
                         self.add_log(f"Raw response: {ready_signal_bytes!r}")
                         try:
                             self.add_log(f"Response as string: {ready_signal_bytes.decode('utf-8', errors='replace')}")
                         except Exception as e:
                             self.add_log(f"Could not decode response: {e}")
                             
                         # Don't close the socket - it might be a server error we can recover from
                         self.add_log("Will continue with frame sending anyway...")
                         # Force ready_ok to true to try to continue
                         ready_ok = True

            except socket.timeout:
                self.add_log(f"Timeout waiting for server READY signal for frame {frame}.")
                # Don't kill socket on timeout, try again later? For now, fail send.
                return False
            except socket.error as e:
                 self.add_log(f"Socket error receiving READY signal for frame {frame}: {e}")
                 if self.socket:
                     try: self.socket.close()
                     except Exception: pass
                     self.socket = None
                 return False
            except Exception as e:
                self.add_log(f"Error receiving READY signal for frame {frame}: {e}")
                if self.socket:
                     try: self.socket.close()
                     except Exception: pass
                     self.socket = None # Assume connection broken
                return False

            if not ready_ok:
                 # Error message printed above
                 return False

            # 3. Send the actual file data
            return self._continue_send_frame_after_ready(job_id, frame, file_path, file_size)

        except Exception as e:
            # Catch-all for unexpected errors in the send_frame logic
            self.add_log(f"Unexpected error during send_frame for frame {frame}: {e}")
            traceback.print_exc()
             # Close socket on unexpected error
            with self.lock:
                if self.socket:
                   try: self.socket.close()
                   except Exception: pass
                   self.socket = None
            return False

    def heartbeat_thread_func(self):
        """Thread that sends regular heartbeats to maintain connection and status."""
        self.add_log("Heartbeat thread started.")
        while self.running:
            last_heartbeat_time = time.monotonic()
            try:
                # Check connection status BEFORE sending heartbeat
                # Use lock briefly to check socket status
                is_connected = False
                with self.lock:
                     is_connected = self.socket is not None

                if is_connected:
                     if not self.send_heartbeat():
                          # send_heartbeat sets self.socket to None on failure
                          self.add_log("Heartbeat failed. Connection likely lost.")
                else:
                     # If not connected, don't try to send heartbeat.
                     # Main loop should be attempting registration.
                     pass # Sleep normally below

                # Wait for the next interval, checking self.running periodically
                while self.running and (time.monotonic() - last_heartbeat_time < HEARTBEAT_INTERVAL):
                    time.sleep(1.0) # Check every second

            except Exception as e:
                self.add_log(f"Heartbeat loop encountered an unexpected error: {e}")
                traceback.print_exc()
                # Avoid busy-looping on errors within the heartbeat thread itself
                time.sleep(HEARTBEAT_INTERVAL) # Wait full interval before trying again

        self.add_log("Heartbeat thread finished.")

    def run_once(self):
        """Run a single iteration of the client loop. Non-blocking."""
        try:
            # Check connection status using the lock
            is_connected = False
            with self.lock:
                 is_connected = self.socket is not None

            if not is_connected:
                # --- Attempt Registration ---
                self.add_log("Connection lost or not established. Attempting registration...")
                if self.register_with_server():
                    self.add_log("Registration successful.")
                    # Continue to request_job below
                else:
                    self.add_log(f"Registration attempt failed. Will retry on next iteration.")
                    return  # Exit this iteration, try again next time

            # --- If Connected and Idle ---
            if self.status == "idle":
                job = self.request_job()
                if job:
                    # We have a job, process it (this blocks until job is done)
                    self.process_job(job)
                    # process_job sets status back to 'idle' or 'error' when finished/failed
                elif self.socket:  # Socket still alive but no job
                    self.add_log("No job assigned or available.")
                # If connection was lost during request_job, the loop will restart and try registration next iteration

            elif self.status == "rendering" or self.status == "assigned":
                # Currently busy, nothing to do here - job processing happens in process_job
                pass
            elif self.status == "error":
                 self.add_log("Client is in error state. Check logs. Setting to idle to retry.")
                 self.status = "idle"
            else:  # Unknown state?
                self.add_log(f"Client in unexpected state: {self.status}. Setting to idle.")
                self.status = "idle"

        except Exception as e:
            self.add_log(f"Critical error in main loop: {e}")
            traceback.print_exc()
            # Prevent busy-looping on unexpected errors
            self.status = "error" # Mark as error
            # Close socket on critical errors
            with self.lock:
                if self.socket:
                   try: self.socket.close()
                   except Exception: pass
                   self.socket = None

    def shutdown(self):
        """Gracefully shut down the client."""
        self.add_log("Shutting down client...")
        # Ensure flag is set for threads
        self.running = False
        
        # Close final connection if open
        with self.lock:
            if self.socket:
                self.add_log("Closing final server connection.")
                try:
                    self.socket.shutdown(socket.SHUT_RDWR) # Graceful shutdown
                    self.socket.close()
                except Exception as e:
                    self.add_log(f"Error closing socket: {e}")
                self.socket = None
        
        self.add_log("Client finished.")


# --- Blender UI Implementation ---

class ClientProperties(bpy.types.PropertyGroup):
    server_ip: bpy.props.StringProperty(
        name="Server IP",
        description="IP address or hostname of the render farm server",
        default="127.0.0.1"
    )
    server_port: bpy.props.IntProperty(
        name="Port",
        description="Server port number",
        default=DEFAULT_SERVER_PORT,
        min=1024,
        max=65535
    )
    client_name: bpy.props.StringProperty(
        name="Client Name",
        description="Name to identify this client on the server",
        default=f"Blender-{socket.gethostname()}"
    )
    is_connected: bpy.props.BoolProperty(
        name="Connected",
        description="Whether the client is currently connected to the server",
        default=False
    )
    log: bpy.props.StringProperty(
        name="Log",
        description="Most recent log message",
        default=""
    )
    progress: bpy.props.FloatProperty(
        name="Progress",
        description="Current job progress in percent",
        default=0.0,
        min=0.0,
        max=100.0,
        subtype='PERCENTAGE'
    )


class RENDERCLIENT_OT_connect(bpy.types.Operator):
    bl_idname = "renderclient.connect"
    bl_label = "Connect to Server"
    bl_description = "Connect to the render farm server"
    
    def execute(self, context):
        props = context.scene.render_client
        
        global _client, _running
        
        if _client and _client.running:
            self.report({'WARNING'}, "Client is already running")
            return {'CANCELLED'}
            
        # Create and start client
        _client = RenderFarmClient(
            server_ip=props.server_ip,
            server_port=props.server_port,
            client_name=props.client_name
        )
        
        # Set global running flag
        _running = True
        
        # Start the heartbeat thread
        heartbeat_thread = threading.Thread(target=_client.heartbeat_thread_func, daemon=True)
        heartbeat_thread.start()
        
        # Set connected status
        props.is_connected = True
        
        # Start timer for UI updates
        if _timer is None:
            bpy.app.timers.register(timer_update, first_interval=1.0, persistent=True)
            
        return {'FINISHED'}


class RENDERCLIENT_OT_disconnect(bpy.types.Operator):
    bl_idname = "renderclient.disconnect"
    bl_label = "Disconnect from Server"
    bl_description = "Disconnect from the render farm server"
    
    def execute(self, context):
        props = context.scene.render_client
        
        global _client, _running
        
        if not _client or not _client.running:
            self.report({'WARNING'}, "Client is not running")
            return {'CANCELLED'}
            
        # Shut down the client
        _running = False
        if _client:
            _client.running = False
            _client.shutdown()
            _client = None
            
        # Set disconnected status
        props.is_connected = False
        
        # Stop timer
        if _timer is not None:
            if _timer in bpy.app.timers.timers:
                bpy.app.timers.unregister(_timer)
            _timer = None
            
        return {'FINISHED'}


class RENDERCLIENT_PT_main_panel(bpy.types.Panel):
    bl_label = "MicroFarm Client"
    bl_idname = "RENDERCLIENT_PT_main_panel"
    bl_space_type = "PROPERTIES"
    bl_region_type = "WINDOW"
    bl_context = "render"  # Show in Render Properties tab
    
    def draw(self, context):
        layout = self.layout
        props = context.scene.render_client
        
        # Server Connection Settings
        box = layout.box()
        box.label(text="Server Connection")
        
        if not props.is_connected:
            # Show connection settings when not connected
            col = box.column()
            col.prop(props, "server_ip")
            col.prop(props, "server_port")
            col.prop(props, "client_name")
            
            # Connect button
            row = col.row()
            row.scale_y = 1.5
            row.operator("renderclient.connect", icon="PLAY")
        else:
            # Show status when connected
            col = box.column()
            col.label(text=f"Connected to: {props.server_ip}:{props.server_port}")
            col.label(text=f"Client Name: {props.client_name}")
            
            if _client:
                col.label(text=f"Status: {_client.status}")
                if _client.current_job:
                    col.label(text=f"Job: {_client.current_job.get('id', 'Unknown')}")
                    col.label(text=f"Frame: {_client.current_frame}")
                col.label(text=f"Last Activity: {_client.last_activity}")
                
            # Progress bar
            col.label(text="Progress:")
            col.prop(props, "progress", text="")
            
            # Log message
            if _client and _client.log_message:
                box.label(text=f"Log: {_client.log_message}")
            
            # Disconnect button
            row = box.row()
            row.scale_y = 1.5
            row.operator("renderclient.disconnect", icon="PAUSE")


# --- Timer Function ---

def timer_update():
    """Timer function for UI updates and client operations"""
    global _client, _running
    
    if not _running or not _client:
        return None  # Stop timer
        
    # Run client logic
    _client.run_once()
    
    # Update UI properties
    if bpy.context.scene.render_client:
        props = bpy.context.scene.render_client
        props.progress = _client.progress
        props.log = _client.log_message
        
        # Force UI redraw if possible
        for window in bpy.context.window_manager.windows:
            for area in window.screen.areas:
                if area.type == 'PROPERTIES':
                    area.tag_redraw()
    
    # Continue timer
    return 1.0  # Run again in 1 second


# --- Registration ---

classes = (
    ClientProperties,
    RENDERCLIENT_OT_connect,
    RENDERCLIENT_OT_disconnect,
    RENDERCLIENT_PT_main_panel,
)

def register():
    for cls in classes:
        bpy.utils.register_class(cls)
    bpy.types.Scene.render_client = bpy.props.PointerProperty(type=ClientProperties)
    
def unregister():
    # Clean up client if running
    global _client, _running
    
    _running = False
    if _client:
        _client.running = False
        _client.shutdown()
        _client = None
    
    # Remove timer if registered
    if _timer is not None and _timer in bpy.app.timers.timers:
        bpy.app.timers.unregister(_timer)
    
    # Unregister classes
    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)
    del bpy.types.Scene.render_client

if __name__ == "__main__":
    register()