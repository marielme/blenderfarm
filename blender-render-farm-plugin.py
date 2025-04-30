# --- START OF FILE blender-render-farm-plugin-minimal-memory.py ---

"""
Minimal Blender Render Server (Simplified - In-Memory Blend Data)
=================================================================
A simplified server component for Blender that distributes rendering tasks.
This version loads the blend file content into memory when a job is created
and sends it directly, instead of saving a persistent temp file.

Key Feature: When a job is created, the current scene is saved temporarily,
read into memory, and the temporary file is deleted. The in-memory data
is sent to clients.

Author: Claude (Modified by AI Assistant)
License: GPL v3
"""

bl_info = {
    "name": "Minimal Render Server (In-Memory)",
    "author": "Claude (Modified by AI Assistant)",
    "version": (1, 1),  # Version bump for memory handling
    "blender": (3, 0, 0),
    "location": "Render Properties > Render Server",
    "description": "Minimal server (loads blend to memory before sending)",
    "warning": "Job creation reads the entire blend file into memory and can freeze Blender temporarily. Requires client script.",
    "doc_url": "",
    "category": "Render",
}

import bpy
import os
import socket
import json
import threading
import tempfile
import time

# import shutil # Not strictly needed
from datetime import datetime, timedelta

# import base64 # Not needed if storing raw bytes

# --- Global Variables ---
CLIENTS = {}
CLIENT_LOCK = threading.Lock()
ACTIVE_JOBS = {}  # Job dict will contain 'blend_data' (bytes) instead of 'blend_path'
SERVER_PORT = 9090
STATUS_UPDATE_INTERVAL = 2.0
CLIENT_TIMEOUT_SECONDS = 180
JOB_COUNTER = 0
JOB_COUNTER_LOCK = threading.Lock()


# --- Helper Functions ---
def get_client_id(ip, name):
    """Generates a unique ID for a client."""
    return f"{ip}-{name}"


# --- Data Structures ---
class RenderClient:
    """Stores information about a connected client."""

    def __init__(self, client_id, ip, name, status="idle"):
        self.id = client_id
        self.ip = ip
        self.name = name
        self.status = status  # idle, assigned, rendering, error
        self.last_ping = datetime.now()
        self.current_job_id = None
        self.current_frames = []  # List of frames currently assigned


# --- Blender Integration: Preferences ---
class RenderServerPreferences(bpy.types.AddonPreferences):
    bl_idname = __name__

    server_port: bpy.props.IntProperty(
        name="Server Port",
        description="Port for the render server",
        default=9090,
        min=1024,
        max=65535,
    )

    output_directory: bpy.props.StringProperty(
        name="Output Directory",
        description="Directory where final rendered frames will be stored",
        default="//render_output/",
        subtype="DIR_PATH",
    )

    def draw(self, context):
        layout = self.layout
        layout.prop(self, "server_port")
        layout.prop(self, "output_directory")


# --- Blender Integration: Scene Properties (UI State) ---
class RenderServerProperties(bpy.types.PropertyGroup):
    is_server_running: bpy.props.BoolProperty(name="Server Running", default=False)

    frame_chunks: bpy.props.IntProperty(
        name="Frames Per Chunk",
        description="Max frames assigned to a client at once",
        default=5,
        min=1,
    )
    pack_textures: bpy.props.BoolProperty(
        name="Pack Textures into Temp File",
        description="Pack textures before loading to memory. WARNING: Can significantly increase job creation time/freeze",
        default=False,
    )

    active_job_id: bpy.props.StringProperty(name="Active Job ID", default="None")
    job_progress: bpy.props.FloatProperty(
        name="Job Progress", default=0.0, subtype="PERCENTAGE", min=0.0, max=100.0
    )
    total_frames: bpy.props.IntProperty(name="Total Frames", default=0)
    completed_frames_count: bpy.props.IntProperty(name="Completed Frames", default=0)


# --- Helper Function to Update UI Job Progress ---
def update_job_progress_display(context):
    """Updates the job progress properties based on ACTIVE_JOBS."""
    if not hasattr(context.scene, "render_server"):
        return
    props = context.scene.render_server
    job_id = props.active_job_id
    if job_id != "None" and job_id in ACTIVE_JOBS:
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if job:
                total = len(job.get("all_frames", []))
                completed = len(job.get("completed_frames", []))
                props.total_frames = total
                props.completed_frames_count = completed
                props.job_progress = (completed / total) * 100.0 if total > 0 else 0.0
            else:
                props.active_job_id = "None"
                props.job_progress = 0.0
                props.total_frames = 0
                props.completed_frames_count = 0
    else:
        if props.active_job_id != "None":
            props.active_job_id = "None"
        props.job_progress = 0.0
        props.total_frames = 0
        props.completed_frames_count = 0


# --- Operator: Start Server ---
class RENDERSERVER_OT_start_server(bpy.types.Operator):
    bl_idname = "renderserver.start_server"
    bl_label = "Start Server"
    bl_description = "Start the render server"

    _timer = None
    _server_thread = None
    _server_socket = None
    _running = False

    def modal(self, context, event):
        """Runs periodically to check client timeouts and update UI."""
        if not self._running or not context.scene.render_server.is_server_running:
            self.cancel(context)
            return {"CANCELLED"}

        if event.type == "TIMER":
            now = datetime.now()
            timeout_delta = timedelta(seconds=CLIENT_TIMEOUT_SECONDS)
            clients_to_remove = []
            with CLIENT_LOCK:
                # Check for inactive clients
                for client_id, client in list(CLIENTS.items()):
                    if now - client.last_ping > timeout_delta:
                        print(f"Client '{client.name}' ({client.ip}) timed out.")
                        clients_to_remove.append(client_id)
                        # Re-queue frames
                        if (
                            client.current_job_id
                            and client.current_job_id in ACTIVE_JOBS
                        ):
                            job = ACTIVE_JOBS.get(client.current_job_id)
                            if (
                                job
                                and job.get("status") == "active"
                                and client.id in job.get("assigned_clients", {})
                            ):
                                frames_to_reassign = [
                                    f
                                    for f in client.current_frames
                                    if f not in job.get("completed_frames", [])
                                ]
                                if frames_to_reassign:
                                    print(
                                        f"   Re-queueing frames {frames_to_reassign} from job {client.current_job_id}"
                                    )
                                    job.setdefault("unassigned_frames", []).extend(
                                        frames_to_reassign
                                    )
                                del job["assigned_clients"][client.id]

                # Remove timed-out clients
                for client_id in clients_to_remove:
                    if client_id in CLIENTS:
                        del CLIENTS[client_id]

                # Update job progress for UI
                update_job_progress_display(context)

            # Force UI redraw
            for window in context.window_manager.windows:
                for area in window.screen.areas:
                    if area.type == "PROPERTIES":
                        area.tag_redraw()

        return {"PASS_THROUGH"}

    def execute(self, context):
        """Starts the server socket and listening thread."""
        # (Execute setup remains the same as the minimal version)
        if not hasattr(context.scene, "render_server"):
            self.report({"ERROR"}, "Render server properties not initialized.")
            return {"CANCELLED"}
        if context.scene.render_server.is_server_running:
            self.report({"WARNING"}, "Server is already running.")
            return {"CANCELLED"}

        prefs = context.preferences.addons[__name__].preferences
        global SERVER_PORT
        SERVER_PORT = prefs.server_port

        output_dir = bpy.path.abspath(prefs.output_directory)
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            self.report({"ERROR"}, f"Output directory error: {output_dir} - {e}")
            return {"CANCELLED"}

        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind(("0.0.0.0", SERVER_PORT))
            self._server_socket.settimeout(1.0)
            self._server_socket.listen(5)

            self._running = True
            context.scene.render_server.is_server_running = True

            self._server_thread = threading.Thread(
                target=self.server_loop, args=(context, output_dir)
            )
            self._server_thread.daemon = True
            self._server_thread.start()

            wm = context.window_manager
            self._timer = wm.event_timer_add(
                STATUS_UPDATE_INTERVAL, window=context.window
            )
            wm.modal_handler_add(self)

            self.report({"INFO"}, f"Server started on port {SERVER_PORT}")
            print(f"Server listening on 0.0.0.0:{SERVER_PORT}")
            return {"RUNNING_MODAL"}

        except Exception as e:
            if self._server_socket:
                self._server_socket.close()
            self._running = False
            if hasattr(context.scene, "render_server"):
                context.scene.render_server.is_server_running = False
            self.report({"ERROR"}, f"Could not start server: {e}")
            return {"CANCELLED"}

    def server_loop(self, context, output_dir):
        """Accepts incoming client connections."""
        # (Server loop remains the same as the minimal version)
        print("Server accept loop started...")
        while self._running and context.scene.render_server.is_server_running:
            try:
                client_socket, client_address = self._server_socket.accept()
                print(f"Connection attempt from {client_address}")
                client_socket.settimeout(10.0)

                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address, context, output_dir),
                )
                client_handler.daemon = True
                client_handler.start()

            except socket.timeout:
                continue
            except AttributeError:
                print("Server loop: render_server attribute missing (likely stopping).")
                self._running = False
                break
            except Exception as e:
                if self._running and context.scene.render_server.is_server_running:
                    print(f"Server accept error: {e}")

        print("Server accept loop finished.")

    def handle_client(self, client_socket, client_address, context, output_dir):
        """Handles communication with a single connected client."""
        client_ip = client_address[0]
        print(f"Handling connection from {client_ip}:{client_address[1]}")
        client_id = None
        try:
            data = client_socket.recv(4096)
            if not data:
                return

            message = json.loads(data.decode("utf-8"))
            msg_type = message.get("type", "unknown")
            print(f"Received [{msg_type}] from {client_address}")

            # --- Registration ---
            if msg_type == "register":
                client_name = message.get("name", f"Client-{client_ip}")
                client_id = get_client_id(client_ip, client_name)
                print(f"Registering client: ID='{client_id}', Name='{client_name}'")
                with CLIENT_LOCK:
                    if client_id in CLIENTS:
                        CLIENTS[client_id].last_ping = datetime.now()
                        CLIENTS[client_id].status = "idle"
                        CLIENTS[client_id].ip = client_ip
                    else:
                        CLIENTS[client_id] = RenderClient(
                            client_id, client_ip, client_name
                        )
                    print(f"Current clients: {list(CLIENTS.keys())}")
                client_socket.sendall(json.dumps({"status": "ok"}).encode("utf-8"))

            # --- Heartbeat ---
            elif msg_type == "heartbeat":
                client_name = message.get("name")
                if not client_name:
                    return
                client_id = get_client_id(client_ip, client_name)
                with CLIENT_LOCK:
                    client = CLIENTS.get(client_id)
                    if client:
                        client.status = message.get("status", client.status)
                        client.last_ping = datetime.now()
                        client_socket.sendall(
                            json.dumps({"status": "ok"}).encode("utf-8")
                        )
                    else:
                        client_socket.sendall(
                            json.dumps(
                                {"status": "error", "message": "Not registered"}
                            ).encode("utf-8")
                        )

            # --- Job Request (Client wants work) ---
            elif msg_type == "job_request":
                client_name = message.get("name")
                if not client_name:
                    return
                client_id = get_client_id(client_ip, client_name)

                job_to_assign = None
                frames_for_client = []
                job_id = None  # Define job_id scope

                with CLIENT_LOCK:
                    client = CLIENTS.get(client_id)
                    if not client or client.status != "idle":
                        status = "busy" if client else "error"
                        msg = (
                            f"Client status is {client.status}"
                            if client
                            else "Client not registered"
                        )
                        client_socket.sendall(
                            json.dumps({"status": status, "message": msg}).encode(
                                "utf-8"
                            )
                        )
                        return

                    # Find an active job with unassigned frames
                    chunk_size = context.scene.render_server.frame_chunks
                    for current_job_id, job in ACTIVE_JOBS.items():
                        if job.get("status") == "active" and job.get(
                            "unassigned_frames"
                        ):
                            frames_to_take = min(
                                len(job["unassigned_frames"]), chunk_size
                            )
                            frames_for_client = job["unassigned_frames"][
                                :frames_to_take
                            ]
                            job["unassigned_frames"] = job["unassigned_frames"][
                                frames_to_take:
                            ]
                            job_to_assign = job
                            job_id = current_job_id  # Assign job_id here
                            print(
                                f"Assigning {len(frames_for_client)} frames ({frames_for_client}) from job '{job_id}' to client '{client.name}'"
                            )
                            break

                    if job_to_assign and frames_for_client:
                        # --- Get Blend Data from Memory ---
                        blend_data = job_to_assign.get("blend_data")
                        blend_filename = job_to_assign.get("blend_filename")

                        if not blend_data or not blend_filename:
                            print(
                                f"Error: Job {job_id} is missing blend_data or blend_filename in memory."
                            )
                            # Re-queue frames if data is missing
                            job_to_assign.setdefault("unassigned_frames", []).extend(
                                frames_for_client
                            )
                            # Remove client from assignment if present
                            if client_id in job_to_assign.get("assigned_clients", {}):
                                try:
                                    del job_to_assign["assigned_clients"][client_id]
                                except KeyError:
                                    pass  # Should not happen if just assigned
                            client_socket.sendall(
                                json.dumps(
                                    {
                                        "status": "error",
                                        "message": "Server error: Missing job data",
                                    }
                                ).encode("utf-8")
                            )
                            return  # Exit handler

                        file_size = len(blend_data)

                        # Update client state
                        client.status = "assigned"
                        client.current_job_id = job_id
                        client.current_frames = frames_for_client
                        job_to_assign.setdefault("assigned_clients", {})[
                            client_id
                        ] = frames_for_client

                        # --- Send job details (including filename and size) ---
                        response = {
                            "status": "ok",
                            "job_id": job_id,
                            "frames": frames_for_client,
                            "blend_file_name": blend_filename,  # Send filename
                            "output_format": job_to_assign["output_format"],
                            "file_size": file_size,  # Send size of data
                        }
                        client_socket.sendall(json.dumps(response).encode("utf-8"))
                        print(
                            f"Sent job details to {client.name}, waiting for READY..."
                        )

                        # --- Wait for client readiness ---
                        client_socket.settimeout(30.0)
                        ready_signal = client_socket.recv(1024)
                        client_socket.settimeout(None)  # Reset timeout

                        if ready_signal != b"READY":
                            print(
                                f"Client {client.name} did not send READY for blend data (got: {ready_signal!r}), aborting send."
                            )
                            # Re-queue frames
                            job_to_assign.setdefault("unassigned_frames", []).extend(
                                frames_for_client
                            )
                            if client_id in job_to_assign.get("assigned_clients", {}):
                                del job_to_assign["assigned_clients"][client_id]
                            client.status = "idle"
                            client.current_job_id = None
                            client.current_frames = []
                            return

                        # --- Send the actual blend data from memory ---
                        print(
                            f"Sending blend data '{blend_filename}' ({file_size} bytes) from memory to {client.name}..."
                        )
                        try:
                            start_send_time = time.monotonic()
                            # Use sendall to ensure all data is transmitted
                            client_socket.sendall(blend_data)
                            bytes_sent = (
                                file_size  # sendall sends all or raises exception
                            )
                            send_duration = time.monotonic() - start_send_time
                            print(
                                f"Sent {bytes_sent} bytes in {send_duration:.2f}s to {client.name}."
                            )

                            client.status = (
                                "rendering"  # Update status after successful send
                            )

                        except Exception as e:
                            print(
                                f"Error sending blend data from memory to {client.name}: {e}"
                            )
                            # Re-queue frames on send failure
                            job_to_assign.setdefault("unassigned_frames", []).extend(
                                frames_for_client
                            )
                            if client_id in job_to_assign.get("assigned_clients", {}):
                                del job_to_assign["assigned_clients"][client_id]
                            client.status = "idle"
                            client.current_job_id = None
                            client.current_frames = []
                            # Let finally block close socket
                            return  # Exit handler after error

                    else:  # No work available
                        print(f"No work available for client {client.name}")
                        client_socket.sendall(
                            json.dumps({"status": "no_work"}).encode("utf-8")
                        )

            # --- Frame Complete (Client sending result) ---
            elif msg_type == "frame_complete":
                # (Frame complete logic remains the same as the minimal version)
                job_id = message.get("job_id")
                frame = message.get("frame")
                file_name = message.get("file_name")
                file_size = message.get("file_size")

                if not all(
                    [job_id, frame is not None, file_name, file_size is not None]
                ):
                    return

                output_path = None
                with CLIENT_LOCK:
                    job = ACTIVE_JOBS.get(job_id)
                    if not job or job.get("status") != "active":
                        client_socket.sendall(
                            json.dumps(
                                {
                                    "status": "error",
                                    "message": f"Job {job_id} not active",
                                }
                            ).encode("utf-8")
                        )
                        return

                    job_output_dir = os.path.join(output_dir, job_id)
                    try:
                        os.makedirs(job_output_dir, exist_ok=True)
                    except Exception as e:
                        print(f"Error creating output dir {job_output_dir}: {e}")
                        client_socket.sendall(
                            json.dumps(
                                {
                                    "status": "error",
                                    "message": "Server output dir error",
                                }
                            ).encode("utf-8")
                        )
                        return
                    output_path = os.path.join(job_output_dir, file_name)
                    print(
                        f"Receiving frame {frame} for job {job_id} -> {output_path} ({file_size} bytes)"
                    )

                try:
                    client_socket.sendall(b"READY")
                except Exception as e:
                    print(f"Error sending READY for frame {frame}: {e}")
                    return

                bytes_received = 0
                try:
                    with open(output_path, "wb") as f:
                        client_socket.settimeout(60.0)
                        while bytes_received < file_size:
                            chunk_size = min(65536, file_size - bytes_received)
                            if chunk_size <= 0:
                                break
                            chunk = client_socket.recv(chunk_size)
                            if not chunk:
                                raise ConnectionError(
                                    "Client disconnected during frame receive"
                                )
                            f.write(chunk)
                            bytes_received += len(chunk)
                        client_socket.settimeout(None)
                    print(f"Received {bytes_received} bytes for frame {frame}.")
                    if bytes_received != file_size:
                        raise ValueError("Received size mismatch")

                    client_socket.sendall(json.dumps({"status": "ok"}).encode("utf-8"))

                    with CLIENT_LOCK:
                        job = ACTIVE_JOBS.get(job_id)
                        if job and job.get("status") == "active":
                            if frame not in job.get("completed_frames", []):
                                job.setdefault("completed_frames", []).append(frame)
                                if len(job["completed_frames"]) >= len(
                                    job.get("all_frames", [])
                                ):
                                    print(f"Job '{job_id}' completed!")
                                    job["end_time"] = datetime.now()
                                    job["status"] = "completed"
                            else:
                                print(
                                    f"Warning: Received duplicate frame {frame} for job {job_id}"
                                )

                        client = CLIENTS.get(client_id)
                        if client and client.current_job_id == job_id:
                            client.status = "idle"
                            client.current_job_id = None
                            client.current_frames = []

                except Exception as e:
                    print(f"Error receiving frame {frame} from {client_address}: {e}")
                    if output_path and os.path.exists(output_path):
                        try:
                            os.remove(output_path)
                        except OSError:
                            pass
                    try:
                        client_socket.sendall(
                            json.dumps(
                                {"status": "error", "message": "File transfer failed"}
                            ).encode("utf-8")
                        )
                    except:
                        pass
                    # Re-queue frame
                    with CLIENT_LOCK:
                        job = ACTIVE_JOBS.get(job_id)
                        if (
                            job
                            and job.get("status") == "active"
                            and frame not in job.get("completed_frames", [])
                            and frame not in job.get("unassigned_frames", [])
                        ):
                            print(
                                f"Re-queueing frame {frame} for job {job_id} due to receive error."
                            )
                            job.setdefault("unassigned_frames", []).append(frame)
                            if client_id in job.get("assigned_clients", {}):
                                try:
                                    job["assigned_clients"][client_id].remove(frame)
                                except ValueError:
                                    pass

            # --- Unknown Message ---
            else:
                print(f"Unknown message type '{msg_type}' from {client_address}")
                try:
                    client_socket.sendall(
                        json.dumps(
                            {"status": "error", "message": f"Unknown type '{msg_type}'"}
                        ).encode("utf-8")
                    )
                except:
                    pass

        except (
            socket.timeout,
            ConnectionResetError,
            ConnectionAbortedError,
            json.JSONDecodeError,
        ) as e:
            print(
                f"Client connection error ({type(e).__name__}) for {client_address}: {e}"
            )
        except AttributeError as e:
            print(
                f"AttributeError in client handler for {client_address} ({e}), likely server stopping."
            )
        except Exception as e:
            import traceback

            print(f"Unexpected error handling client {client_address}: {e}")
            traceback.print_exc()

        finally:
            print(f"Closing connection handler for {client_address}")
            if client_id:
                with CLIENT_LOCK:
                    client = CLIENTS.get(client_id)
                    if client:
                        # Re-queue logic on unexpected disconnect
                        if client.current_job_id and (
                            client.status == "rendering" or client.status == "assigned"
                        ):
                            job = ACTIVE_JOBS.get(client.current_job_id)
                            if (
                                job
                                and job.get("status") == "active"
                                and client_id in job.get("assigned_clients", {})
                            ):
                                frames_to_reassign = [
                                    f
                                    for f in client.current_frames
                                    if f not in job.get("completed_frames", [])
                                ]
                                if frames_to_reassign:
                                    print(
                                        f"Re-queueing frames {frames_to_reassign} from disconnected/errored client {client.name} for job {client.current_job_id}"
                                    )
                                    job.setdefault("unassigned_frames", []).extend(
                                        frames_to_reassign
                                    )
                                del job["assigned_clients"][client_id]
                        # Reset client state
                        client.status = "error"
                        client.current_job_id = None
                        client.current_frames = []
                        # Optionally remove client: if client_id in CLIENTS: del CLIENTS[client_id]

            if client_socket:
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                except Exception as e_sh:
                    print(f"Error shutting down socket: {e_sh}")
                try:
                    client_socket.close()
                except Exception as e_cl:
                    print(f"Error closing socket: {e_cl}")

    def cancel(self, context):
        """Stops the server thread, closes socket, cleans up."""
        # (Cancel logic remains the same as the minimal version)
        print("Cancel requested, stopping server...")
        self._running = False
        if hasattr(context.scene, "render_server"):
            context.scene.render_server.is_server_running = False

        if self._timer:
            try:
                context.window_manager.event_timer_remove(self._timer)
            except Exception as e:
                print(f"Error removing timer: {e}")
            self._timer = None

        if self._server_socket:
            print("Closing server socket...")
            try:
                self._server_socket.close()
            except Exception as e:
                print(f"Error closing server socket: {e}")
            self._server_socket = None

        if self._server_thread and self._server_thread.is_alive():
            print("Waiting for server thread to join...")
            self._server_thread.join(timeout=2.0)
            if self._server_thread.is_alive():
                print("Server thread did not join cleanly.")
            self._server_thread = None

        print("Server cancel finished.")
        self.report({"INFO"}, "Server stopped.")
        return {"CANCELLED"}


# --- Operator: Stop Server ---
class RENDERSERVER_OT_stop_server(bpy.types.Operator):
    bl_idname = "renderserver.stop_server"
    bl_label = "Stop Server"
    bl_description = "Stop the render server"

    def execute(self, context):
        """Signals the running modal operator to stop."""
        # (Stop server logic remains the same)
        if (
            not hasattr(context.scene, "render_server")
            or not context.scene.render_server.is_server_running
        ):
            self.report({"WARNING"}, "Server is not running.")
            return {"CANCELLED"}

        context.scene.render_server.is_server_running = False
        for window in context.window_manager.windows:
            for area in window.screen.areas:
                if area.type == "PROPERTIES":
                    area.tag_redraw()

        self.report({"INFO"}, "Server stopping...")
        return {"FINISHED"}


# --- Operator: Create Render Job ---
class RENDERSERVER_OT_send_scene(bpy.types.Operator):
    bl_idname = "renderserver.send_scene"
    bl_label = "Create Render Job"
    bl_description = "Prepare scene, load to memory, and create render job"

    _job_id = None

    @classmethod
    def poll(cls, context):
        return (
            hasattr(context.scene, "render_server")
            and context.scene.render_server.is_server_running
        )

    def execute(self, context):
        if (
            not hasattr(context.scene, "render_server")
            or not context.scene.render_server.is_server_running
        ):
            self.report({"ERROR"}, "Server not running or properties missing.")
            return {"CANCELLED"}

        props = context.scene.render_server
        prefs = context.preferences.addons[__name__].preferences

        # Generate unique job ID
        global JOB_COUNTER
        with JOB_COUNTER_LOCK:
            JOB_COUNTER += 1
            timestamp = int(time.time())
            self._job_id = f"job_{JOB_COUNTER:03d}_{timestamp}"

        frame_start = context.scene.frame_start
        frame_end = context.scene.frame_end
        frame_step = context.scene.frame_step
        all_frames = list(range(frame_start, frame_end + 1, frame_step))

        if not all_frames:
            self.report({"ERROR"}, "No frames in scene range.")
            return {"CANCELLED"}

        print(f"Preparing job '{self._job_id}' with {len(all_frames)} frames...")
        self.report({"INFO"}, "Preparing blend file (UI may freeze)...")
        if context.area:
            context.area.tag_redraw()

        context.window_manager.progress_begin(0, 100)
        blend_path = None  # Path to the *temporary* file
        blend_data = None  # Content read from the temp file
        blend_filename = None  # Base filename
        job_added_successfully = False  # Flag for cleanup

        try:
            context.window_manager.progress_update(5)
            # Allow UI to redraw the progress bar
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)
            # Step 1: Create temporary file (handles packing, checks)
            blend_path = self.prepare_blend_file(context)  # This can block UI
            if not blend_path:
                self.report(
                    {"ERROR"}, "Failed to prepare temporary blend file (check console)."
                )
                return {"CANCELLED"}
            print(f"Temporary blend file created: {blend_path}")
            context.window_manager.progress_update(70)
            # Redraw UI to show updated progress
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)

            # Step 2: Read the temporary file into memory
            print(f"Reading temporary blend file into memory...")
            read_start_time = time.monotonic()
            try:
                with open(blend_path, "rb") as f:
                    blend_data = f.read()
                blend_filename = os.path.basename(blend_path)
                read_duration = time.monotonic() - read_start_time
                print(f"Read {len(blend_data)} bytes in {read_duration:.2f}s.")
                if not blend_data:
                    raise ValueError("Read 0 bytes from temporary file.")
            except Exception as e:
                self.report(
                    {"ERROR"}, f"Failed to read temporary blend file {blend_path}: {e}"
                )
                # No need to manually remove blend_path here, finally block below handles it
                return {"CANCELLED"}
            context.window_manager.progress_update(90)
            # Redraw UI to show updated progress
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)

            # --- Create and register the job (using blend_data) ---
            with CLIENT_LOCK:
                if self._job_id in ACTIVE_JOBS:
                    self.report({"ERROR"}, f"Job ID '{self._job_id}' collision.")
                    return {"CANCELLED"}  # Should not happen

                output_format = context.scene.render.image_settings.file_format
                color_mode = context.scene.render.image_settings.color_mode
                color_depth = context.scene.render.image_settings.color_depth
                job_output_format_details = (
                    f"{output_format}_{color_mode}_{color_depth}"
                )

                ACTIVE_JOBS[self._job_id] = {
                    "id": self._job_id,
                    "all_frames": list(all_frames),
                    "unassigned_frames": list(all_frames),
                    "assigned_clients": {},
                    "completed_frames": [],
                    # --- Store data and filename, not path ---
                    "blend_data": blend_data,  # Store raw bytes
                    "blend_filename": blend_filename,  # Store filename for client
                    # ------------------------------------------
                    "output_format": job_output_format_details,
                    "start_time": datetime.now(),
                    "end_time": None,
                    "status": "active",
                }
                job_added_successfully = True  # Mark job as added
                props.active_job_id = self._job_id
                update_job_progress_display(context)

            context.window_manager.progress_update(95)  # Progress after adding job

            # Step 3: Create job-specific output directory (remains the same)
            output_dir = bpy.path.abspath(prefs.output_directory)
            job_output_dir = os.path.join(output_dir, self._job_id)
            try:
                os.makedirs(job_output_dir, exist_ok=True)
                print(f"Created job output directory: {job_output_dir}")
            except Exception as e:
                print(
                    f"Warning: Could not create job output dir '{job_output_dir}': {e}"
                )
                self.report({"WARNING"}, f"Could not create job output directory: {e}")

            self.report(
                {"INFO"},
                f"Job '{self._job_id}' created with {len(all_frames)} frames (blend data in memory).",
            )
            return {"FINISHED"}

        except Exception as e:
            # Catch any unexpected errors during the process
            self.report({"ERROR"}, f"Unexpected error creating job: {e}")
            import traceback

            traceback.print_exc()
            return {"CANCELLED"}
        finally:
            context.window_manager.progress_end()
            # --- Clean up temporary file ---
            # Always try to remove the temp file if it was created,
            # especially if job wasn't added successfully or reading failed.
            if blend_path and os.path.exists(blend_path):
                if job_added_successfully:
                    print(
                        f"Removing temporary blend file (loaded to memory): {blend_path}"
                    )
                else:
                    print(
                        f"Cleaning up unused temporary blend file due to error: {blend_path}"
                    )
                try:
                    os.remove(blend_path)
                except OSError as e:
                    print(f"  Warning: Failed to remove temporary blend file: {e}")

    def prepare_blend_file(self, context):
        """
        Saves a temporary copy of the current scene to disk.
        Handles optional texture packing, checks for missing files.
        Returns the path to the temporary file, or None on error.
        ** This function BLOCKS UI **
        """
        # (This function remains exactly the same as the minimal version)
        temp_dir = tempfile.gettempdir()
        blend_filename = f"renderjob_{self._job_id}.blend"
        temp_blend_path = os.path.join(temp_dir, blend_filename)

        # --- Dependency Check ---
        print("Checking for missing external files...")
        context.window_manager.progress_update(10)
        missing_files = []
        for collection in (bpy.data.images, bpy.data.libraries, bpy.data.movieclips):
            for item in collection:
                filepath = getattr(item, "filepath", None)
                packed = getattr(item, "packed_file", None)
                source = getattr(item, "source", None)
                if item.library:
                    continue
                if filepath and not packed:
                    try:
                        abs_path = bpy.path.abspath(filepath)
                        if not os.path.exists(abs_path) and not bpy.path.is_subdir(
                            abs_path, temp_dir
                        ):
                            missing_files.append(
                                f"{type(item).__name__}: {item.name} ('{filepath}')"
                            )
                    except Exception as e:
                        missing_files.append(
                            f"{type(item).__name__}: {item.name} (Error: {e})"
                        )
                elif (
                    isinstance(item, bpy.types.Image)
                    and source == "FILE"
                    and not filepath
                    and not packed
                    and not item.has_data
                ):
                    missing_files.append(
                        f"Image: {item.name} (Source=FILE, No path/data)"
                    )

        if missing_files:
            error_message = (
                "Missing external files detected:\n - "
                + "\n - ".join(missing_files)
                + "\nCannot create job."
            )
            print(f"ERROR: {error_message}")
            self.report({"ERROR"}, "Missing external files detected (check console).")

            def draw_missing_files_popup(self_popup, context_popup):
                self_popup.layout.label(text="Missing External Files:")
                for line in missing_files[:15]:
                    self_popup.layout.label(text=f"- {line}", icon="FILE_MISSING")
                if len(missing_files) > 15:
                    self_popup.layout.label(
                        text=f"... and {len(missing_files) - 15} more."
                    )
                self_popup.layout.separator()
                self_popup.layout.label(text="Cannot create job.", icon="ERROR")

            bpy.context.window_manager.popup_menu(
                draw_missing_files_popup, title="Missing Files", icon="ERROR"
            )
            return None
        else:
            print("No missing external files detected.")
        context.window_manager.progress_update(20)

        # --- Packing & Saving Copy ---
        pack_requested = context.scene.render_server.pack_textures
        packed_in_current_scene = False
        try:
            if pack_requested:
                print("Packing external files in current scene (will be undone)...")
                context.window_manager.progress_update(30)
                bpy.ops.file.pack_all()  # Blocking
                packed_in_current_scene = True
                print("Packing finished.")
                context.window_manager.progress_update(50)

            save_msg = f"Saving temporary copy ({'packed' if packed_in_current_scene else 'unpacked'}) to {temp_blend_path}..."
            print(save_msg)
            context.window_manager.progress_update(60)
            start_save_time = time.monotonic()
            bpy.ops.wm.save_as_mainfile(
                filepath=temp_blend_path, copy=True, relative_remap=False
            )  # Blocking
            save_duration = time.monotonic() - start_save_time
            print(f"Save finished in {save_duration:.2f} seconds.")
            # Progress update moved to execute method after this returns

        except Exception as e:
            self.report({"ERROR"}, f"Error during packing/saving copy: {e}")
            if os.path.exists(temp_blend_path):
                try:
                    os.remove(temp_blend_path)
                except OSError:
                    pass
            return None
        finally:
            if packed_in_current_scene:
                print("Unpacking files in current scene to restore state...")
                try:
                    bpy.ops.file.unpack_all(method="USE_ORIGINAL")  # Blocking
                    print("Unpacking finished.")
                except Exception as unpack_e:
                    print(f"WARNING: Failed to auto-unpack current scene: {unpack_e}")
                    self.report({"WARNING"}, "Failed to restore unpack state.")
            # Progress update moved to execute method after this returns

        return temp_blend_path


# --- Operator: Clear Completed Job ---
class RENDERSERVER_OT_clear_completed_job(bpy.types.Operator):
    bl_idname = "renderserver.clear_completed_job"
    bl_label = "Clear Completed Job"
    bl_description = "Remove completed job record from memory"

    @classmethod
    def poll(cls, context):
        # (Poll remains the same)
        if not hasattr(context.scene, "render_server"):
            return False
        props = context.scene.render_server
        if props.is_server_running and props.active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(props.active_job_id)
                return job is not None and job.get("status") == "completed"
        return False

    def execute(self, context):
        if not hasattr(context.scene, "render_server"):
            return {"CANCELLED"}
        props = context.scene.render_server
        job_id_to_clear = props.active_job_id
        if job_id_to_clear == "None":
            return {"CANCELLED"}

        print(f"Clearing job '{job_id_to_clear}' data from memory...")
        job_output_dir_log = "N/A"
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.pop(job_id_to_clear, None)
            if job:
                # --- No blend file path to delete anymore ---
                # blend_path = job.get("blend_path")
                # if blend_path and os.path.exists(blend_path):
                #     print(f"Removing temporary blend file: {blend_path}")
                #     try: os.remove(blend_path)
                #     except OSError as e: print(f"Warning: Failed to remove temp blend file {blend_path}: {e}")
                print(f"Removed job data for '{job_id_to_clear}' from memory.")
                # --------------------------------------------

                # Get output dir path for logging where files remain
                prefs = context.preferences.addons[__name__].preferences
                output_dir = bpy.path.abspath(prefs.output_directory)
                job_output_dir_log = os.path.join(output_dir, job_id_to_clear)

            if props.active_job_id == job_id_to_clear:
                props.active_job_id = "None"
                props.job_progress = 0.0
                props.total_frames = 0
                props.completed_frames_count = 0

        self.report(
            {"INFO"},
            f"Cleared job '{job_id_to_clear}'. Files remain in '{job_output_dir_log}'.",
        )

        for window in context.window_manager.windows:
            for area in window.screen.areas:
                if area.type == "PROPERTIES":
                    area.tag_redraw()

        return {"FINISHED"}


# --- Main UI Panel ---
class RENDERSERVER_PT_main_panel(bpy.types.Panel):
    bl_label = "Render Server (In-Memory)"  # Updated label
    bl_idname = "RENDERSERVER_PT_main_panel_minimal_memory"  # Unique ID
    bl_space_type = "PROPERTIES"
    bl_region_type = "WINDOW"
    bl_context = "render"

    def draw(self, context):
        # (Draw logic remains the same as the minimal version)
        layout = self.layout
        if not hasattr(context.scene, "render_server"):
            layout.label(text="Error: Render Server properties missing!", icon="ERROR")
            return

        props = context.scene.render_server
        prefs = context.preferences.addons[__name__].preferences

        # Server Status
        box = layout.box()
        row = box.row(align=True)
        if props.is_server_running:
            row.operator("renderserver.stop_server", icon="PAUSE", text="Stop Server")
            row.label(text=f"Running: Port {SERVER_PORT}", icon="RADIOBUT_ON")
        else:
            row.operator("renderserver.start_server", icon="PLAY", text="Start Server")
            row.label(text=f"Stopped (Port {prefs.server_port})", icon="RADIOBUT_OFF")

        if props.is_server_running:
            try:  # Show IP
                hostname = socket.gethostname()
                ip_address = "?"
                addr_info = socket.getaddrinfo(hostname, None, socket.AF_INET)
                ipv4 = [a[4][0] for a in addr_info if not a[4][0].startswith("127.")]
                ip_address = ipv4[0] if ipv4 else addr_info[0][4][0]
                row = box.row()
                row.label(text=f"Server IP: ~{ip_address}")
            except Exception:
                pass
            row = box.row()
            row.label(text=f"Client Timeout: {CLIENT_TIMEOUT_SECONDS}s")

        # Job Management
        box = layout.box()
        box.label(text="Render Job", icon="RENDER_ANIMATION")

        if props.is_server_running:
            active_job_id = props.active_job_id
            job_status = "None"
            is_completed = False
            job_duration_str = ""
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(active_job_id)
                if job:
                    job_status = job.get("status", "unknown")
                    is_completed = job_status == "completed"
                    if is_completed and job.get("end_time") and job.get("start_time"):
                        duration = job["end_time"] - job["start_time"]
                        job_duration_str = f"Duration: {str(duration).split('.')[0]}"

            if active_job_id == "None" or is_completed:  # Job Creation Area
                if is_completed:
                    col = box.column(align=True)
                    row = col.row(align=True)
                    row.label(text=f"Job '{active_job_id}' Completed", icon="CHECKMARK")
                    row.operator(
                        "renderserver.clear_completed_job", text="Clear", icon="X"
                    )
                    if job_duration_str:
                        col.label(text=job_duration_str)
                    box.separator()
                settings_box = box.box()
                settings_box.label(text="Next Job Settings:")
                settings_box.prop(props, "frame_chunks")
                settings_box.prop(props, "pack_textures")
                row = settings_box.row()
                row.label(
                    text=f"Frame Range: {context.scene.frame_start}-{context.scene.frame_end} (Step: {context.scene.frame_step})"
                )
                settings_box.label(
                    text="Note: Job creation loads blend to RAM, may freeze UI.",
                    icon="INFO",
                )
                row = box.row()
                row.scale_y = 1.3
                row.operator("renderserver.send_scene", icon="ADD")

            elif job:  # Active Job Status Area
                col = box.column(align=True)
                row = col.row()
                row.label(text=f"Active Job: {active_job_id} ({job_status})")
                row = col.row()
                row.prop(props, "job_progress", text="Progress")
                row = col.row()
                row.label(
                    text=f"Frames: {props.completed_frames_count} / {props.total_frames}"
                )
            else:
                box.label(
                    text=f"Error: Job '{active_job_id}' state unknown!", icon="ERROR"
                )
        else:
            box.label(text="Start server to manage jobs.")

        # Connected Clients
        box = layout.box()
        clients_copy_data = []
        with CLIENT_LOCK:
            now = datetime.now()
            for client in CLIENTS.values():
                clients_copy_data.append(
                    {
                        "name": client.name,
                        "ip": client.ip,
                        "status": client.status,
                        "job_id": client.current_job_id,
                        "frames": len(client.current_frames),
                        "ping_sec": (now - client.last_ping).total_seconds(),
                    }
                )
        box.label(text=f"Connected Clients: {len(clients_copy_data)}", icon="COMMUNITY")
        if clients_copy_data:
            col = box.column(align=True)
            clients_copy_data.sort(key=lambda c: c["name"])
            for cd in clients_copy_data:
                row = col.row(align=True)
                icon = "USER"
                if cd["status"] == "rendering":
                    icon = "RENDER_STILL"
                elif cd["status"] == "assigned":
                    icon = "PAUSE"
                elif cd["status"] == "idle":
                    icon = "PLAY"
                elif cd["status"] == "error":
                    icon = "ERROR"
                row.label(text=f"{cd['name']}", icon=icon)
                row.label(text=f"{cd['ip']}")
                st = cd["status"]
                if cd["job_id"]:
                    st += f" ({cd['frames']}f)"
                row.label(text=st)
                row.label(text=f"{cd['ping_sec']:.1f}s ago")
        else:
            box.label(text="No clients connected.")

        # Output Directory
        box = layout.box()
        box.label(text="Output Directory", icon="FILE_FOLDER")
        box.prop(prefs, "output_directory", text="")
        try:
            box.label(text=f"Absolute: {bpy.path.abspath(prefs.output_directory)}")
        except Exception:
            box.label(text="Invalid path", icon="ERROR")


# --- Registration ---
classes = (
    RenderServerPreferences,
    RenderServerProperties,
    RENDERSERVER_OT_start_server,
    RENDERSERVER_OT_stop_server,
    RENDERSERVER_OT_send_scene,
    RENDERSERVER_OT_clear_completed_job,
    RENDERSERVER_PT_main_panel,
)


def register():
    print(f"Registering {bl_info['name']}...")
    unregister_silent()
    for cls in classes:
        try:
            bpy.utils.register_class(cls)
        except Exception as e:
            print(f"  Error registering {cls.__name__}: {e}")
    try:
        bpy.types.Scene.render_server = bpy.props.PointerProperty(
            type=RenderServerProperties
        )
    except Exception as e:
        print(f"  Error registering Scene.render_server: {e}")
    print(f"{bl_info['name']} registered.")


def unregister():
    print(f"Unregistering {bl_info['name']}...")
    context = bpy.context
    if (
        hasattr(context, "scene")
        and hasattr(context.scene, "render_server")
        and context.scene.render_server.is_server_running
    ):
        print("  Server running, attempting stop...")
        try:
            context.scene.render_server.is_server_running = False
        except Exception as e:
            print(f"  Error setting server running flag: {e}")

    for cls in reversed(classes):
        if hasattr(bpy.utils, "unregister_class") and hasattr(bpy.types, cls.__name__):
            try:
                bpy.utils.unregister_class(cls)
            except Exception as e:
                print(f"  Error unregistering {cls.__name__}: {e}")

    if hasattr(bpy.types.Scene, "render_server"):
        try:
            del bpy.types.Scene.render_server
        except Exception as e:
            print(f"  Error deleting Scene.render_server: {e}")

    global CLIENTS, ACTIVE_JOBS
    print("  Cleaning up global data...")
    with CLIENT_LOCK:
        CLIENTS.clear()
        # --- No temp files to clean, data is in ACTIVE_JOBS dict ---
        # for job_id, job in list(ACTIVE_JOBS.items()):
        #     blend_path = job.get("blend_path") # Path no longer stored
        #     if blend_path and os.path.exists(blend_path):
        #         print(f"  Cleaning up temp file from unregister: {blend_path}")
        #         try: os.remove(blend_path)
        #         except OSError as e: print(f"    Error removing: {e}")
        print(f"  Clearing {len(ACTIVE_JOBS)} job(s) data from memory.")
        ACTIVE_JOBS.clear()  # This releases the blend_data bytes for garbage collection
    print(f"{bl_info['name']} unregistered.")


def unregister_silent():
    if hasattr(bpy.types, RENDERSERVER_PT_main_panel.__name__):
        print("Silent unregister...")
        unregister()


if __name__ == "__main__":
    register()

# --- END OF FILE ---
