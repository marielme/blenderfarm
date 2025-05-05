
"""
Minimal Blender Render Server (Simplified - In-Memory Blend Data)
=================================================================
CLEANED AND VERIFIED FOR BLENDER 4.X COMPATIBILITY
A simplified server component for Blender that distributes rendering tasks.
This version loads the blend file content into memory when a job is created
and sends it directly, instead of saving a persistent temp file.

Key Feature: When a job is created, the current scene is saved temporarily,
read into memory, and the temporary file is deleted. The in-memory data
is sent to clients.

Author: Claude (Modified by AI Assistant, Refactored)
License: Apache 2.0
"""

bl_info = {
    "name": "MicroFram: Render Farm Server v1.3", # Version bump for refactor
    "author": "Mariel Martinex, Using AI tools",
    "version": (1, 3, 0),
    # --- Updated for Blender 4.x ---
    "blender": (4, 0, 0),
    # ---
    "location": "Render Properties > Render Server",
    "description": "Minimal Render Farm",
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
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any # For type hinting

# --- Global Variables ---
CLIENTS: Dict[str, 'RenderClient'] = {}
CLIENT_LOCK = threading.RLock()  # Use Re-entrant Lock for shared client/job data
ACTIVE_JOBS: Dict[str, Dict[str, Any]] = {} # Job dict will contain 'blend_data' (bytes)
SERVER_PORT = 9090 # Default, can be overridden by preferences
STATUS_UPDATE_INTERVAL = 2.0 # Seconds for UI refresh timer
CLIENT_TIMEOUT_SECONDS = 180 # Seconds before considering a client disconnected
JOB_COUNTER = 0
JOB_COUNTER_LOCK = threading.Lock()
_registered_classes = set() # Track registered classes for clean unregistration


# --- Helper Functions ---
def get_client_id(ip: str, name: str) -> str:
    """Generates a unique ID for a client based on IP and name."""
    return f"{ip}-{name}"


# --- Data Structures ---
class RenderClient:
    """Stores information about a connected client."""
    def __init__(self, client_id: str, ip: str, name: str, status: str = "idle"):
        self.id: str = client_id
        self.ip: str = ip
        self.name: str = name
        self.status: str = status  # idle, assigned, rendering, error
        self.last_ping: datetime = datetime.now()
        self.current_job_id: Optional[str] = None
        self.current_frames: List[int] = []  # List of frames currently assigned


# --- Blender Integration: Preferences ---
class RenderServerPreferences(bpy.types.AddonPreferences):
    bl_idname = __name__ # Uses the addon module name

    server_port: bpy.props.IntProperty(
        name="Server Port",
        description="Port for the render server listener",
        default=9090,
        min=1024,
        max=65535,
    )

    output_directory: bpy.props.StringProperty(
        name="Output Directory",
        description="Base directory where final rendered frames will be stored (in job-specific subfolders)",
        default="//render_output/", # Default relative to blend file
        subtype="DIR_PATH",
    )

    def draw(self, context):
        layout = self.layout
        layout.prop(self, "server_port")
        layout.prop(self, "output_directory")


# --- Blender Integration: Scene Properties (UI State) ---
class RenderServerProperties(bpy.types.PropertyGroup):
    """Properties stored per-scene to manage the server state and UI display."""
    is_server_running: bpy.props.BoolProperty(
        name="Server Running",
        description="Indicates if the server listener is active",
        default=False
    )

    frame_chunks: bpy.props.IntProperty(
        name="Frames Per Chunk",
        description="Maximum number of frames assigned to a client in one task",
        default=5,
        min=1,
    )
    pack_textures: bpy.props.BoolProperty(
        name="Pack Textures into Temp File (Experimental)",
        description="Pack textures before loading blend to memory. WARNING: Can significantly increase job creation time/freeze and blend data size. Unpacking is NOT handled automatically.",
        default=False,
    )
    create_video: bpy.props.BoolProperty(
        name="Create MP4 Video on Completion",
        description="Create an MP4 video from rendered frames when the job completes",
        default=True,
    )
    video_location: bpy.props.EnumProperty(
        name="MP4 Location",
        description="Where to save the MP4 video file",
        items=[
            ('parent', "Parent Directory", "Save in the parent directory above the frames"),
            ('frames', "Frames Directory", "Save in the same directory as the frames"),
            ('root', "Root Output Dir", "Save in the root output directory")
        ],
        default='parent'
    )
    video_codec: bpy.props.EnumProperty(
        name="Video Codec",
        description="Codec to use for creating MP4 videos",
        items=[
            ('h264', "H.264", "Standard H.264 codec, widely compatible"),
            ('hevc', "H.265/HEVC", "Better compression but less compatible"),
            ('prores', "ProRes", "Higher quality, larger file size")
        ],
        default='h264'
    )
    video_quality: bpy.props.EnumProperty(
        name="Video Quality",
        description="Quality setting for the video output",
        items=[
            ('high', "High", "High quality, larger file size"),
            ('medium', "Medium", "Balanced quality and file size"),
            ('low', "Low", "Lower quality, smaller file size")
        ],
        default='medium'
    )

    active_job_id: bpy.props.StringProperty(
        name="Active Job ID",
        description="The ID of the currently monitored job",
        default="None"
    )
    job_progress: bpy.props.FloatProperty(
        name="Job Progress",
        description="Completion percentage of the active job",
        default=0.0,
        subtype="PERCENTAGE",
        min=0.0,
        max=100.0
    )
    total_frames: bpy.props.IntProperty(
        name="Total Frames",
        description="Total number of frames in the active job",
        default=0
    )
    completed_frames_count: bpy.props.IntProperty(
        name="Completed Frames",
        description="Number of frames completed in the active job",
        default=0
    )


# --- Helper Function to Update UI Job Progress ---
def create_mp4_check(job_id):
    """
    Check if MP4 creation was requested for a job and call the create function.
    This is called from a separate thread after job completion.
    """
    try:
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if not job or not job.get("create_mp4_requested"):
                return
                
            # Get the frames directory
            frames_dir = job.get("mp4_output_dir")
            if not frames_dir or not os.path.isdir(frames_dir):
                print(f"Render Server: Cannot create MP4 for job {job_id}, invalid frames dir: {frames_dir}")
                return
                
            # Find an active scene with render_server properties
            import bpy
            codec = 'h264'  # Default values in case we can't find preferences
            quality = 'medium'
            location = 'parent'  # Default to parent directory
            
            # Try to get preferences from any scene that has render_server properties
            for scene in bpy.data.scenes:
                if hasattr(scene, "render_server"):
                    props = scene.render_server
                    if props.create_video:
                        codec = props.video_codec
                        quality = props.video_quality
                        location = props.video_location
                        print(f"Render Server: Using MP4 settings from scene '{scene.name}': location={location}, codec={codec}, quality={quality}")
                        break
            
            # Start the video creation process
            job["mp4_creation_started"] = True
            job["mp4_location"] = location  # Store the location setting
            
            # Determine the root output directory
            prefs = bpy.context.preferences.addons[__name__].preferences
            root_output_dir = bpy.path.abspath(prefs.output_directory)
    
    except Exception as e:
        print(f"Render Server: Error preparing for MP4 creation: {e}")
        traceback.print_exc()
        return
    
    # Now call the actual ffmpeg function outside the lock
    create_mp4_from_frames(frames_dir, codec, quality, job_id, location, root_output_dir)

def create_mp4_from_frames(frames_dir, codec='h264', quality='medium', job_id='unknown', location='parent', root_dir=None):
    """
    Creates an MP4 video from rendered frames using ffmpeg.
    
    Args:
        frames_dir: Directory containing the PNG frames
        codec: Video codec to use (h264, hevc, prores)
        quality: Quality setting (high, medium, low)
        job_id: Job ID for logging purposes
        location: Where to save the MP4 (parent, frames, or root)
        root_dir: Root output directory (used if location='root')
    """
    try:
        import subprocess
        import glob
        
        print(f"Render Server: Starting MP4 creation for job '{job_id}' in {frames_dir}")
        
        # Find all rendered PNG frames
        frame_pattern = os.path.join(frames_dir, "frame_*.png")
        frames = sorted(glob.glob(frame_pattern))
        
        if not frames:
            print(f"Render Server: No frames found in {frames_dir} for MP4 creation")
            return
            
        # Determine frame rate from the blend file (default to 24 fps if not found)
        fps = 24
        
        # Determine output file path based on location setting
        if location == 'frames':
            # Save in the same directory as the frames
            output_dir = frames_dir
        elif location == 'root' and root_dir:
            # Save in the root output directory
            output_dir = root_dir
        else:
            # Default: Save in the parent directory (one level up)
            output_dir = os.path.dirname(frames_dir)
        
        # Set the full output path
        output_mp4 = os.path.join(output_dir, f"{job_id}_render.mp4")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"Render Server: MP4 will be saved to: {output_mp4}")
        
        # Build ffmpeg command based on codec and quality
        cmd = ['ffmpeg', '-y', '-framerate', str(fps)]
        
        # Input frames pattern
        cmd.extend(['-pattern_type', 'glob', '-i', frame_pattern])
        
        # Set codec-specific options
        if codec == 'h264':
            # H.264 settings
            crf = '18' if quality == 'high' else '23' if quality == 'medium' else '28'
            cmd.extend(['-c:v', 'libx264', '-crf', crf, '-pix_fmt', 'yuv420p'])
        elif codec == 'hevc':
            # H.265/HEVC settings
            crf = '22' if quality == 'high' else '28' if quality == 'medium' else '34'
            cmd.extend(['-c:v', 'libx265', '-crf', crf, '-pix_fmt', 'yuv420p'])
        elif codec == 'prores':
            # ProRes settings
            profile = '3' if quality == 'high' else '2' if quality == 'medium' else '1'
            cmd.extend(['-c:v', 'prores_ks', '-profile:v', profile])
        else:
            # Fallback to H.264
            cmd.extend(['-c:v', 'libx264', '-crf', '23', '-pix_fmt', 'yuv420p'])
        
        # Output file
        cmd.append(output_mp4)
        
        # Execute ffmpeg command
        print(f"Render Server: Running ffmpeg command: {' '.join(cmd)}")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            print(f"Render Server: Successfully created MP4 video for job '{job_id}': {output_mp4}")
        else:
            print(f"Render Server: Error creating MP4 video: {stderr.decode()}")
            
            # Fallback approach if ffmpeg fails - try simpler command
            if process.returncode != 0:
                print("Render Server: Trying fallback ffmpeg command...")
                fallback_cmd = [
                    'ffmpeg', '-y', '-framerate', str(fps),
                    '-pattern_type', 'glob', '-i', frame_pattern,
                    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', output_mp4
                ]
                fallback_process = subprocess.Popen(fallback_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                fallback_stdout, fallback_stderr = fallback_process.communicate()
                
                if fallback_process.returncode == 0:
                    print(f"Render Server: Fallback MP4 creation successful: {output_mp4}")
                else:
                    print(f"Render Server: Fallback MP4 creation failed: {fallback_stderr.decode()}")
    
    except Exception as e:
        print(f"Render Server: Exception during MP4 creation: {e}")
        traceback.print_exc()

def update_job_json(job_id):
    """
    Updates the JSON file for a job with the current status and progress.
    This function should be called whenever a job's status changes.
    The JSON file is stored one level up from the job's output directory.
    """
    try:
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if not job:
                print(f"Render Server: Cannot update JSON for job '{job_id}', job not found")
                return False
                
            # Get the job's output directory
            prefs = bpy.context.preferences.addons[__name__].preferences
            output_dir = bpy.path.abspath(prefs.output_directory)
            
            # Store JSON one level up from the job directory
            json_path = os.path.join(output_dir, f"{job_id}_info.json")
            
            # Make sure directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Create a copy of job data without the binary blend_data for JSON serialization
            job_json_data = {k: v for k, v in job.items() if k != "blend_data"}
            
            # Convert any datetime objects to strings
            for key, value in job_json_data.items():
                if isinstance(value, datetime):
                    job_json_data[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            
            # Add additional progress info
            total_frames = len(job.get("all_frames", []))
            completed_frames = len(job.get("completed_frames", []))
            job_json_data["progress_percentage"] = (completed_frames / total_frames) * 100.0 if total_frames > 0 else 0.0
            job_json_data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Save job data as JSON file
            with open(json_path, 'w') as json_file:
                json.dump(job_json_data, json_file, indent=4)
            print(f"  Job info saved to: {json_path}")
            return True
            
    except Exception as e:
        print(f"Render Server: Error updating JSON for job '{job_id}': {e}")
        traceback.print_exc()
        return False

def check_for_stalled_jobs():
    """Check for jobs that appear to be stalled and might need intervention."""
    with CLIENT_LOCK:
        stalled_jobs = []
        now = datetime.now()
        
        for job_id, job in ACTIVE_JOBS.items():
            if job.get("status") != "active":
                continue  # Only check active jobs
                
            # Check if job has no unassigned frames but isn't complete
            total_frames = len(job.get("all_frames", []))
            completed_frames = len(job.get("completed_frames", []))
            unassigned_frames = len(job.get("unassigned_frames", []))
            assigned_frames = 0
            
            # Count frames currently assigned to clients
            for client_frames in job.get("assigned_clients", {}).values():
                assigned_frames += len(client_frames)
            
            # Check for potentially stalled job:
            # 1. No unassigned frames left
            # 2. Not all frames completed
            # 3. No frames currently assigned OR job has been active for over 1 hour
            job_age = (now - job.get("start_time", now)).total_seconds() if "start_time" in job else 0
            
            if (unassigned_frames == 0 and 
                completed_frames < total_frames and
                (assigned_frames == 0 or job_age > 3600)):
                
                # Mark job as potentially stalled
                if "stalled_detected" not in job:
                    job["stalled_detected"] = True
                    job["stalled_detected_time"] = now
                    print(f"Render Server: Job '{job_id}' appears to be stalled. {completed_frames}/{total_frames} frames completed, {unassigned_frames} unassigned, {assigned_frames} assigned.")
                    stalled_jobs.append(job_id)
                    
                    # Update the JSON file with stalled status
                    update_job_json(job_id)
        
        return stalled_jobs

def update_job_progress_display(context: bpy.types.Context):
    """Safely updates the job progress properties based on ACTIVE_JOBS."""
    if not context or not hasattr(context, "scene") or not context.scene:
        return # Context or scene not available
    if not hasattr(context.scene, "render_server"):
        return # Addon properties not registered/initialized
    props = context.scene.render_server
    job_id = props.active_job_id
    needs_ui_reset = False

    if job_id != "None":
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if job:
                total = len(job.get("all_frames", []))
                completed = len(job.get("completed_frames", []))
                props.total_frames = total
                props.completed_frames_count = completed
                progress = (completed / total) * 100.0 if total > 0 else 0.0
                props.job_progress = progress
                
                # Update JSON file with progress if job is active
                # Only update every 5% change to avoid excessive writes
                if job.get("status") == "active":
                    last_progress = job.get("last_json_progress", 0)
                    # Update JSON if progress has changed by at least 5% or it's the first update
                    if "last_json_progress" not in job or abs(progress - last_progress) >= 5.0:
                        job["last_json_progress"] = progress
                        update_job_json(job_id)
                
                # Check for stalled jobs while we're updating the UI
                check_for_stalled_jobs()
            else:
                # Job ID is set in UI, but job doesn't exist in memory (cleared?)
                needs_ui_reset = True
    else:
        # No active job ID set in UI
        needs_ui_reset = (props.job_progress != 0.0 or
                          props.total_frames != 0 or
                          props.completed_frames_count != 0)

    if needs_ui_reset:
        props.active_job_id = "None"
        props.job_progress = 0.0
        props.total_frames = 0
        props.completed_frames_count = 0


# --- Operator: Start Server ---
class RENDERSERVER_OT_start_server(bpy.types.Operator):
    bl_idname = "renderserver.start_server"
    bl_label = "Start Render Server"
    bl_description = "Start the background render server listener"

    _timer: Optional[bpy.types.Timer] = None
    _server_thread: Optional[threading.Thread] = None
    _server_socket: Optional[socket.socket] = None
    _running: bool = False # Internal flag for thread loop control

    def modal(self, context: bpy.types.Context, event: bpy.types.Event) -> set:
        """Periodic UI update timer for job progress and timeout checks."""
        # Check if the server should still be running (controlled by UI prop)
        props = getattr(context.scene, "render_server", None)
        if not self._running or not props or not props.is_server_running:
            # print("Modal check: Server stop detected, canceling operator.") # Debug
            self.cancel(context) # Calls cleanup
            return {"CANCELLED"}

        if event.type == "TIMER":
            # Update job progress display in the UI
            update_job_progress_display(context)

            # Force UI redraw (specifically the properties panel)
            if context.window_manager and context.window:
                for window in context.window_manager.windows:
                    for area in window.screen.areas:
                        if area.type == "PROPERTIES":
                            area.tag_redraw()

        return {"PASS_THROUGH"} # Continue listening for events

    def execute(self, context: bpy.types.Context) -> set:
        """Starts the server socket and listening thread."""
        props = getattr(context.scene, "render_server", None)
        if not props:
            self.report({"ERROR"}, "Render server properties not found on scene.")
            return {"CANCELLED"}
        if props.is_server_running:
            self.report({"WARNING"}, "Server is already running.")
            return {"CANCELLED"}

        prefs = context.preferences.addons[__name__].preferences
        global SERVER_PORT
        SERVER_PORT = prefs.server_port # Use port from preferences

        output_dir = bpy.path.abspath(prefs.output_directory)
        if not output_dir or output_dir == "//":
            self.report({"ERROR"}, "Output directory preference is not set or invalid.")
            return {"CANCELLED"}
        try:
            os.makedirs(output_dir, exist_ok=True)
            print(f"Render Server: Using output directory: {output_dir}")
        except Exception as e:
            self.report({"ERROR"}, f"Output directory error: '{output_dir}' - {e}")
            return {"CANCELLED"}

        # --- Get frame chunk size here, before starting thread ---
        try:
            frame_chunk_size = props.frame_chunks
            if frame_chunk_size <= 0: raise ValueError("Chunk size must be positive")
        except Exception as e:
             self.report({"ERROR"}, f"Invalid 'Frames Per Chunk' setting: {e}. Using default 5.")
             frame_chunk_size = 5
        # ---

        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allow address reuse
            self._server_socket.bind(("0.0.0.0", SERVER_PORT)) # Bind to all interfaces
            self._server_socket.settimeout(1.0) # Timeout for accept() to keep loop responsive
            self._server_socket.listen(5) # Queue up to 5 incoming connections

            self._running = True # Set internal running flag
            props.is_server_running = True # Update UI property

            # Start the thread that accepts connections
            # Pass necessary data explicitly, avoiding bpy.context access in thread
            self._server_thread = threading.Thread(
                target=self.server_loop,
                args=(output_dir, frame_chunk_size), # Pass needed info
                daemon=True # Allow Blender to exit even if thread is stuck
            )
            self._server_thread.start()

            # Register a modal timer for UI updates
            wm = context.window_manager
            self._timer = wm.event_timer_add(STATUS_UPDATE_INTERVAL, window=context.window)
            wm.modal_handler_add(self) # Add this operator instance as a modal handler

            self.report({"INFO"}, f"Server started on port {SERVER_PORT}")
            print(f"Render Server: Listening on 0.0.0.0:{SERVER_PORT}")
            return {"RUNNING_MODAL"} # Keep the operator running modally for timer

        except Exception as e:
            print(f"Render Server: Error starting server: {e}")
            traceback.print_exc()
            if self._server_socket:
                try: self._server_socket.close()
                except Exception: pass
            self._running = False
            if props: props.is_server_running = False # Reset UI prop
            self.report({"ERROR"}, f"Could not start server: {e}")
            return {"CANCELLED"}

    def server_loop(self, output_dir: str, frame_chunk_size: int):
        """Accepts incoming client connections in a separate thread."""
        print("Render Server: Accept loop thread started...")
        if not self._server_socket:
             print("Render Server Error: server_loop started without a valid server socket.")
             return

        next_timeout_check = time.monotonic() + CLIENT_TIMEOUT_SECONDS / 2.0 # Initial check sooner
        timeout_delta = timedelta(seconds=CLIENT_TIMEOUT_SECONDS)

        while self._running: # Check the flag controlled by the operator's cancel method
            # Perform timeout checks periodically within this loop
            current_time_mono = time.monotonic()
            if current_time_mono >= next_timeout_check:
                # print("Checking for client timeouts...") # Debug
                self.check_timeouts(timeout_delta)
                next_timeout_check = current_time_mono + CLIENT_TIMEOUT_SECONDS / 2.0 # Schedule next check

            try:
                # Wait for a connection (with timeout set on the socket)
                client_socket, client_address = self._server_socket.accept()
                print(f"Render Server: Connection attempt from {client_address}")
                # Set a timeout for initial operations on the new client socket
                client_socket.settimeout(30.0)

                # Handle the client connection in a new thread
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address, output_dir, frame_chunk_size),
                    daemon=True # Allow Blender exit even if client handler is stuck
                )
                client_handler.start()

            except socket.timeout:
                # No connection received within the server socket timeout, just loop again
                continue
            except OSError as e:
                # Handle specific errors like socket being closed during shutdown
                if self._running and ("socket closed" in str(e).lower() or e.errno == 9): # EBADF=9 (socket closed)
                    print("Render Server: Server socket closed, accept loop stopping.")
                    break
                elif self._running: # Log other OS errors if server should be running
                    print(f"Render Server: Accept loop OSError: {e}")
                    time.sleep(0.5) # Avoid busy-looping on persistent OS errors
                else: # If not running, ignore error during shutdown
                    break
            except AttributeError:
                # This might happen if the operator instance is garbage collected during shutdown
                print("Render Server: Accept loop: Operator attributes missing (likely stopping).")
                break
            except Exception as e:
                # Log unexpected errors in the accept loop
                if self._running: # Only log if we expected to be running
                    print(f"Render Server: Unexpected accept loop error: {e}")
                    traceback.print_exc()
                    time.sleep(1) # Avoid busy-looping

        print("Render Server: Accept loop thread finished.")

    def check_timeouts(self, timeout_delta: timedelta):
        """Remove clients that have not pinged recently and re-queue their frames."""
        now = datetime.now()
        clients_to_remove: List[str] = []
        frames_to_requeue: Dict[str, List[int]] = {} # {job_id: [frame1, frame2]}

        with CLIENT_LOCK:
            # Identify inactive clients
            client_ids_to_check = list(CLIENTS.keys()) # Iterate over a copy of keys
            for client_id in client_ids_to_check:
                client = CLIENTS.get(client_id)
                if not client: continue # Client removed concurrently?

                if now - client.last_ping > timeout_delta:
                    print(f"Render Server: Client '{client.name}' ({client.ip}) timed out (Last ping: {client.last_ping}). Removing.")
                    clients_to_remove.append(client_id)

                    # Check if client had an active job assignment that needs re-queueing
                    if client.current_job_id and (client.status == "assigned" or client.status == "rendering"):
                        job_id = client.current_job_id
                        job = ACTIVE_JOBS.get(job_id)

                        # Ensure job exists, is active, and client was actually assigned frames in this job
                        if (job and job.get("status") == "active" and
                                client_id in job.get("assigned_clients", {})):

                            current_assigned_frames = job["assigned_clients"].get(client_id, [])
                            completed_in_job = set(job.get("completed_frames", [])) # Use set for faster lookup

                            # Find frames assigned to this client that are not yet completed
                            requeue_for_job = [f for f in current_assigned_frames if f not in completed_in_job]

                            if requeue_for_job:
                                print(f"   Timeout: Re-queueing frames {requeue_for_job} from job '{job_id}'")
                                frames_to_requeue.setdefault(job_id, []).extend(requeue_for_job)

                            # Remove client from job assignment list (regardless of re-queue)
                            job["assigned_clients"].pop(client_id, None)

            # Remove timed-out clients from the main list
            for client_id in clients_to_remove:
                CLIENTS.pop(client_id, None)

            # Add frames back to unassigned list for the affected jobs
            for job_id, frames in frames_to_requeue.items():
                job = ACTIVE_JOBS.get(job_id)
                # Double-check job is still active before adding frames back
                if job and job.get("status") == "active":
                    current_unassigned = job.setdefault("unassigned_frames", [])
                    for frame in frames:
                        # Avoid adding duplicates if already re-queued by another mechanism
                        if frame not in current_unassigned:
                            current_unassigned.append(frame)
                    # Optional: Sort unassigned frames?
                    # job["unassigned_frames"].sort()


    def handle_client(self, client_socket: socket.socket, client_address: Tuple[str, int], output_dir: str, frame_chunk_size: int):
        """Handles communication with a single connected client. Runs in its own thread."""
        client_ip = client_address[0]
        client_port = client_address[1]
        print(f"Render Server: Handling connection from {client_ip}:{client_port}")
        client_id: Optional[str] = None
        client_name_for_log: str = f"Unknown ({client_ip})"

        # Main loop to handle multiple messages from this client connection
        try: # Wrap the whole loop in try...finally for cleanup
            while True: # Loop until break or exception
                # Flag to determine if the socket should be closed after this message iteration
                close_socket_after_message = True # Assume close unless message handled successfully and requires keep-alive

                try:
                    # Set a reasonable timeout for receiving the next message
                    # (Longer than heartbeat interval but less than full timeout)
                    client_socket.settimeout(CLIENT_TIMEOUT_SECONDS + 30.0) # e.g., 180 + 30 = 210s
                    # print(f"DEBUG: Waiting for message from {client_name_for_log}...") # Debug noise
                    data = client_socket.recv(4096) # Receive header/JSON data

                    if not data:
                        print(f"Render Server: Client {client_name_for_log} disconnected gracefully (received empty data).")
                        break # Exit the while loop, socket will be closed in finally

                    message = json.loads(data.decode("utf-8"))
                    msg_type = message.get("type", "unknown")

                    # --- Initial Message Processing & Client Identification ---
                    if not client_id:
                        client_name = message.get("name")
                        if client_name and msg_type == "register": # Expect register as first message
                            client_id = get_client_id(client_ip, client_name)
                            client_name_for_log = f"'{client_name}' ({client_ip})"
                            print(f"Render Server: Registering client {client_name_for_log} (ID: {client_id})")
                            with CLIENT_LOCK:
                                # Add or update client entry
                                CLIENTS[client_id] = RenderClient(client_id, client_ip, client_name, status="idle")
                                CLIENTS[client_id].last_ping = datetime.now()
                            try:
                                client_socket.sendall(json.dumps({"status": "ok", "message": "Registered"}).encode("utf-8"))
                                close_socket_after_message = False # Keep socket open after successful registration
                            except Exception as e:
                                print(f"Render Server: Error sending registration OK to {client_name_for_log}: {e}")
                                break # Close socket on send error
                        else:
                            print(f"Render Server: Error: First message from {client_ip} was not 'register' or missing name: {message}")
                            try: client_socket.sendall(json.dumps({"status": "error", "message": "Expected 'register' with 'name'"}).encode("utf-8"))
                            except Exception: pass # Ignore send error here, already decided to close
                            break # Exit loop, close socket

                    # --- Subsequent Message Handling (Client ID is known) ---
                    elif client_id: # Ensure client_id is set for subsequent messages
                        # print(f"DEBUG: Received [{msg_type}] from {client_name_for_log}") # Reduce log noise

                        # --- Heartbeat ---
                        if msg_type == "heartbeat":
                            ack_sent = False
                            with CLIENT_LOCK:
                                client = CLIENTS.get(client_id)
                                if client:
                                    client.status = message.get("status", client.status) # Update status from client report
                                    client.last_ping = datetime.now()
                                    # print(f"DEBUG: Updated last_ping for {client_id}") # Debug
                                else:
                                    # Client ID was known but now missing? Might happen if timeout removed it between messages.
                                    print(f"Render Server: Heartbeat from client {client_name_for_log} whose record is missing. Requesting re-register.")
                                    try: client_socket.sendall(json.dumps({"status": "error", "message": "Not registered, please re-register"}).encode("utf-8"))
                                    except Exception as e: print(f"Render Server: Error sending Heartbeat Error (unreg) to {client_name_for_log}: {e}")
                                    break # Close connection if client is gone from records

                            # Send OK only if client was found and updated
                            if client:
                                try:
                                    client_socket.sendall(json.dumps({"status": "ok"}).encode("utf-8"))
                                    close_socket_after_message = False # Keep socket open
                                    ack_sent = True
                                except Exception as e:
                                    print(f"Render Server: Error sending Heartbeat OK to {client_name_for_log}: {e}")
                                    break # Exit loop on send error
                            # if ack_sent: print("DEBUG: Heartbeat Ack sent.") # Debug

                        # --- Job Request ---
                        elif msg_type == "job_request":
                            job_to_assign: Optional[Dict] = None
                            frames_for_client: List[int] = []
                            job_id_assigned: Optional[str] = None
                            client_state_ok_for_job = False

                            # Check client status and find an available job chunk under lock
                            with CLIENT_LOCK:
                                client = CLIENTS.get(client_id)
                                if not client:
                                    print(f"Render Server: Job request from client {client_name_for_log} whose record is missing. Requesting re-register.")
                                    try: client_socket.sendall(json.dumps({"status": "error", "message": "Not registered, please re-register"}).encode("utf-8"))
                                    except Exception as e: print(f"Render Server: Error sending Job Req Error (unreg) to {client_name_for_log}: {e}")
                                    break # Close connection

                                elif client.status == "idle":
                                    client_state_ok_for_job = True
                                    # Find an active job with unassigned frames
                                    active_job_ids = list(ACTIVE_JOBS.keys()) # Iterate copy of keys
                                    for current_job_id in active_job_ids:
                                        job = ACTIVE_JOBS.get(current_job_id) # Re-get job inside loop
                                        if not job: continue # Job removed concurrently?

                                        if (job.get("status") == "active" and
                                                isinstance(job.get("unassigned_frames"), list) and
                                                job["unassigned_frames"]): # Check list is not empty

                                            # Get a chunk of frames
                                            frames_to_take = min(len(job["unassigned_frames"]), frame_chunk_size)
                                            frames_for_client = job["unassigned_frames"][:frames_to_take]
                                            # Remove these frames from unassigned list *under the lock*
                                            job["unassigned_frames"] = job["unassigned_frames"][frames_to_take:]

                                            job_to_assign = job # Keep reference to the job data
                                            job_id_assigned = current_job_id
                                            print(f"Render Server: Found job '{job_id_assigned}' for {client_name_for_log}. Assigning {len(frames_for_client)} frames: {frames_for_client}")
                                            break # Stop searching once a job chunk is found
                                else: # Client not idle
                                    print(f"Render Server: Job request from non-idle client {client_name_for_log} (status: {client.status}). Sending busy.")
                                    try:
                                        client_socket.sendall(json.dumps({"status": "busy", "message": f"Client status is {client.status}"}).encode("utf-8"))
                                        close_socket_after_message = False # Keep open, let client retry later
                                    except Exception as e:
                                        print(f"Render Server: Error sending Job Request Busy to {client_name_for_log}: {e}")
                                        break # Exit loop on send error
                                    # Skip job assignment logic below if client was busy
                                    continue # Go to next iteration of while True loop

                            # --- Job Assignment Response Section (outside lock, but uses data prepared under lock) ---
                            if client_state_ok_for_job:
                                if job_to_assign and frames_for_client and job_id_assigned:
                                    # --- Start Job Assign Logic ---
                                    blend_data: Optional[bytes] = job_to_assign.get("blend_data")
                                    blend_filename: Optional[str] = job_to_assign.get("blend_filename")

                                    if not blend_data or not blend_filename:
                                        print(f"Render Server: Error: Job '{job_id_assigned}' is missing blend data or filename. Cannot assign.")
                                        # Requeue frames under lock
                                        with CLIENT_LOCK:
                                            job = ACTIVE_JOBS.get(job_id_assigned) # Re-get job
                                            if job: job.setdefault("unassigned_frames", []).extend(frames_for_client) # Put frames back
                                        try: client_socket.sendall(json.dumps({"status": "error", "message": "Server error: Job data missing"}).encode("utf-8"))
                                        except Exception as e: print(f"Render Server: Error sending Job Data Error to {client_name_for_log}: {e}")
                                        break # Exit loop

                                    file_size = len(blend_data)
                                    output_format = job_to_assign.get("output_format", "PNG_RGB_8") # Get format for client

                                    # Update client status to 'assigned' under lock BEFORE sending job details
                                    client_updated = False
                                    with CLIENT_LOCK:
                                        client = CLIENTS.get(client_id) # Re-get client under lock
                                        job = ACTIVE_JOBS.get(job_id_assigned) # Re-get job under lock
                                        # Check if client and job still exist and job is active before assignment
                                        if client and job and job.get("status") == "active":
                                            client.status = "assigned"
                                            client.current_job_id = job_id_assigned
                                            client.current_frames = list(frames_for_client) # Store assigned frames
                                            # Add client to the job's assignment list
                                            job.setdefault("assigned_clients", {})[client_id] = list(frames_for_client)
                                            client_updated = True
                                        else:
                                            print(f"Render Server: Client {client_name_for_log} or Job {job_id_assigned} state changed before final assignment. Requeueing frames.")
                                            # Requeue the frames if the assignment couldn't be finalized
                                            if job: job.setdefault("unassigned_frames", []).extend(frames_for_client)
                                            # We will close the socket since the state is inconsistent
                                            break # Exit loop

                                    # If client/job state changed, don't proceed with sending
                                    if not client_updated:
                                        # Error already logged, just break loop which leads to finally block
                                        break

                                    # --- Send Job Details (JSON) ---
                                    response = {
                                        "status": "ok", "job_id": job_id_assigned, "frames": frames_for_client,
                                        "blend_file_name": blend_filename, "output_format": output_format,
                                        "file_size": file_size,
                                    }
                                    try:
                                        client_socket.sendall(json.dumps(response).encode("utf-8"))

                                        # --- Wait for Client Readiness Signal ---
                                        client_socket.settimeout(30.0) # Short timeout for READY signal
                                        ready_signal = client_socket.recv(1024)
                                        client_socket.settimeout(CLIENT_TIMEOUT_SECONDS + 30.0) # Reset longer timeout for data transfer

                                        if ready_signal == b"READY":
                                            # --- Send Blend File Data ---
                                            print(f"Render Server: Sending blend data ({file_size} bytes) for job '{job_id_assigned}' to {client_name_for_log}...")
                                            start_send = time.monotonic()
                                            client_socket.sendall(blend_data)
                                            send_dur = time.monotonic() - start_send
                                            print(f"Render Server: Blend data sent to {client_name_for_log} in {send_dur:.2f}s.")

                                            # Update client status to 'rendering' under lock AFTER successful send
                                            with CLIENT_LOCK:
                                                client = CLIENTS.get(client_id)
                                                # Check job is still current one for this client
                                                if client and client.current_job_id == job_id_assigned:
                                                    client.status = "rendering"
                                                else:
                                                     print(f"Render Server: Warning: Client {client_name_for_log} state changed before render status update (Job: {job_id_assigned}).")

                                            close_socket_after_message = False # Success, keep connection open

                                        else:
                                            print(f"Render Server: Client {client_name_for_log} did not send READY after job details. Signal: {ready_signal!r}")
                                            # Requeue frames and reset client status under lock
                                            with CLIENT_LOCK:
                                                job = ACTIVE_JOBS.get(job_id_assigned)
                                                if job: job.setdefault("unassigned_frames", []).extend(frames_for_client); job.get("assigned_clients", {}).pop(client_id, None)
                                                client = CLIENTS.get(client_id)
                                                if client: client.status = "idle"; client.current_job_id = None; client.current_frames = []
                                            break # Exit loop, close socket

                                    except socket.timeout:
                                         print(f"Render Server: Timeout waiting for READY signal from {client_name_for_log}.")
                                         # Requeue and reset client status
                                         with CLIENT_LOCK:
                                             job = ACTIVE_JOBS.get(job_id_assigned);
                                             if job: job.setdefault("unassigned_frames", []).extend(frames_for_client); job.get("assigned_clients", {}).pop(client_id, None)
                                             client = CLIENTS.get(client_id);
                                             if client: client.status = "idle"; client.current_job_id = None; client.current_frames = []
                                         break # Exit loop
                                    except Exception as e:
                                        print(f"Render Server: Error during job send sequence for {client_name_for_log}: {e}")
                                        traceback.print_exc()
                                        # Requeue frames and reset client status under lock
                                        with CLIENT_LOCK:
                                            job = ACTIVE_JOBS.get(job_id_assigned)
                                            if job: job.setdefault("unassigned_frames", []).extend(frames_for_client); job.get("assigned_clients", {}).pop(client_id, None)
                                            client = CLIENTS.get(client_id)
                                            if client: client.status = "idle"; client.current_job_id = None; client.current_frames = []
                                        break # Exit loop
                                    # --- End Job Assign Logic ---

                                else: # No work available
                                    try:
                                        client_socket.sendall(json.dumps({"status": "no_work"}).encode("utf-8"))
                                        close_socket_after_message = False # Keep socket open for future requests
                                    except Exception as e:
                                        print(f"Render Server: Error sending No Work message to {client_name_for_log}: {e}")
                                        break # Exit loop

                        # --- Frame Complete ---
                        elif msg_type == "frame_complete":
                            job_id = message.get("job_id")
                            frame = message.get("frame")
                            file_name = message.get("file_name")
                            file_size = message.get("file_size")

                            # Validate received metadata
                            if not all([job_id, isinstance(frame, int) and frame >= 0,
                                        file_name, isinstance(file_size, int) and file_size >= 0]):
                                print(f"Render Server: Invalid frame_complete message from {client_name_for_log}: {message}")
                                try: client_socket.sendall(json.dumps({"status": "error", "message": "Invalid frame_complete format"}).encode("utf-8"))
                                except Exception: pass
                                break # Exit loop

                            output_path: Optional[str] = None
                            job_output_dir: Optional[str] = None
                            frame_valid_for_reception = False
                            job_exists_and_active = False

                            # Check job status and prepare output path under lock
                            with CLIENT_LOCK:
                                job = ACTIVE_JOBS.get(job_id)
                                client = CLIENTS.get(client_id) # Check client exists too

                                if job and job.get("status") == "active" and client:
                                    # Basic filename sanitization (prevent directory traversal)
                                    clean_file_name = os.path.basename(file_name)
                                    if not clean_file_name or clean_file_name != file_name: # Ensure basename didn't change it unexpectedly or result in empty
                                         print(f"Render Server: Invalid filename '{file_name}' in frame_complete from {client_name_for_log}. Using safe name.")
                                         # Create a safe fallback name if needed (optional)
                                         # clean_file_name = f"frame_{frame}_client_{client.name}.ext" # Replace ext based on job output format if known
                                         # For now, reject if basename fails validation:
                                         print(f"Render Server: Rejecting potentially unsafe filename: {file_name}")
                                         try: client_socket.sendall(json.dumps({"status": "error", "message": "Invalid filename provided"}).encode("utf-8"))
                                         except Exception: pass
                                         break # Close socket


                                    job_output_dir = os.path.join(output_dir, job_id)
                                    output_path = os.path.join(job_output_dir, clean_file_name)
                                    frame_valid_for_reception = True
                                    job_exists_and_active = True # Set flag to proceed

                                elif not client:
                                     print(f"Render Server: Received frame {frame} for job {job_id} from client {client_name_for_log} whose record is missing.")
                                elif not job:
                                    print(f"Render Server: Received frame {frame} for non-existent job '{job_id}' from {client_name_for_log}.")
                                else: # Job exists but not active
                                    print(f"Render Server: Received frame {frame} for non-active job '{job_id}' (status: {job.get('status')}) from {client_name_for_log}.")

                            # --- Handle Frame Reception based on checks ---
                            if not job_exists_and_active:
                                 # Send error only if client was found but job was bad
                                 if CLIENTS.get(client_id): # Avoid sending if client also gone
                                     try: client_socket.sendall(json.dumps({"status": "error", "message": f"Job '{job_id}' not active or found"}).encode("utf-8"))
                                     except Exception as e: print(f"Render Server: Error sending Frame Comp Job Error to {client_name_for_log}: {e}")
                                 break # Close socket if job isn't active/valid for reception

                            # Ensure output directory exists (do this outside lock to avoid holding lock during I/O)
                            if frame_valid_for_reception and job_output_dir:
                                 try:
                                     os.makedirs(job_output_dir, exist_ok=True)
                                 except Exception as e:
                                     print(f"Render Server: Error creating output dir '{job_output_dir}': {e}")
                                     try: client_socket.sendall(json.dumps({"status": "error", "message": "Server output directory error"}).encode("utf-8"))
                                     except Exception as e_send: print(f"Render Server: Error sending Frame Comp Dir Error to {client_name_for_log}: {e_send}")
                                     break # Close socket on server-side directory error

                            # Proceed with receiving the frame data if path and directory are OK
                            if frame_valid_for_reception and output_path:
                                frame_recv_success = False
                                try:
                                    client_socket.sendall(b"READY")

                                    # --- Receive Frame File Data ---
                                    bytes_received = 0
                                    print(f"Render Server: Receiving frame {frame} ({file_size} bytes) for job '{job_id}' to '{output_path}'...")
                                    with open(output_path, "wb") as f:
                                        transfer_start = time.monotonic()
                                        # Set reasonable timeouts for receiving chunks
                                        max_transfer_time_seconds = 300 # 5 minutes max per frame file
                                        chunk_receive_timeout = 60.0 # Timeout for individual recv calls

                                        client_socket.settimeout(chunk_receive_timeout)
                                        while bytes_received < file_size:
                                            if time.monotonic() - transfer_start > max_transfer_time_seconds:
                                                raise socket.timeout(f"Max frame transfer time ({max_transfer_time_seconds}s) exceeded")

                                            # Request a chunk (adjust size based on typical network buffer sizes)
                                            chunk_size = min(65536, file_size - bytes_received)
                                            chunk = client_socket.recv(chunk_size)
                                            if not chunk:
                                                raise ConnectionAbortedError(f"Client {client_name_for_log} disconnected during frame {frame} reception")
                                            f.write(chunk)
                                            bytes_received += len(chunk)
                                        # Reset longer timeout after successful transfer
                                        client_socket.settimeout(CLIENT_TIMEOUT_SECONDS + 30.0)

                                    # Verify received size
                                    if bytes_received != file_size:
                                        raise ValueError(f"Frame {frame} size mismatch (expected {file_size}, got {bytes_received})")

                                    print(f"Render Server: Received frame {frame} successfully from {client_name_for_log}.")
                                    frame_recv_success = True

                                    # --- Send Final OK for Frame ---
                                    client_socket.sendall(json.dumps({"status": "ok", "message": f"Frame {frame} received"}).encode("utf-8"))
                                    close_socket_after_message = False # Keep connection open for next message/job request

                                    # --- Update Job/Client State (under lock) AFTER successful receive and ack ---
                                    with CLIENT_LOCK:
                                        job = ACTIVE_JOBS.get(job_id) # Re-get job under lock
                                        if job and job.get("status") == "active":
                                            if frame not in job.get("completed_frames", []):
                                                job.setdefault("completed_frames", []).append(frame)
                                                # Optional: Sort completed frames? job["completed_frames"].sort()
                                                print(f"Render Server: Job '{job_id}' progress: {len(job['completed_frames'])} / {len(job.get('all_frames',[]))} frames completed.")

                                                # Check if job is now fully complete
                                                if len(job["completed_frames"]) >= len(job.get("all_frames", [])):
                                                    print(f"Render Server: Job '{job_id}' completed!")
                                                    job["end_time"] = datetime.now()
                                                    job["status"] = "completed"
                                                    
                                                    # Get the output directory
                                                    job_output_dir = os.path.dirname(output_path)
                                                    
                                                    # Remove blend data now that job is done to free memory
                                                    job.pop("blend_data", None)
                                                    
                                                    # Set MP4 info before updating JSON
                                                    job["mp4_output_dir"] = job_output_dir
                                                    
                                                    # Update the JSON file with completed status
                                                    update_job_json(job_id)
                                                    
                                                    # Check if video creation is enabled in preferences
                                                    # We can't access context here, so we use a delayed approach
                                                    job["create_mp4_requested"] = True
                                                    
                                                    # Schedule MP4 creation in a separate thread
                                                    def delayed_mp4_creation():
                                                        time.sleep(2)  # Give a moment for things to settle
                                                        create_mp4_check(job_id)
                                                        
                                                    threading.Thread(
                                                        target=delayed_mp4_creation,
                                                        daemon=True
                                                    ).start()

                                            else:
                                                # This might happen with network retries, usually safe to ignore if file was overwritten.
                                                print(f"Render Server: Warning: Received duplicate completion for frame {frame}, job '{job_id}' from {client_name_for_log}.")

                                        client = CLIENTS.get(client_id) # Get client under lock
                                        # Set client back to idle only if they were rendering *this* specific job/frame
                                        if client and client.current_job_id == job_id:
                                            # Remove the specific frame from the client's list
                                            try: client.current_frames.remove(frame)
                                            except ValueError: pass # Frame might already be removed if error occurred before

                                            # If client has no more frames assigned for this job, set to idle
                                            if not client.current_frames:
                                                print(f"Render Server: Client {client_name_for_log} finished assigned frames for job '{job_id}', setting to idle.")
                                                client.status = "idle"
                                                client.current_job_id = None
                                        # else: Client state mismatch (e.g., already idle or working on different job), log if needed.

                                except Exception as e:
                                    print(f"Render Server: Error receiving/processing frame {frame} from {client_name_for_log}: {e}")
                                    traceback.print_exc()

                                    # Clean up potentially incomplete file if reception failed
                                    if not frame_recv_success and output_path and os.path.exists(output_path):
                                        print(f"Render Server: Removing potentially incomplete file: {output_path}")
                                        try: os.remove(output_path)
                                        except OSError as e_rem: print(f"Render Server: Error removing incomplete file '{output_path}': {e_rem}")

                                    # Try sending error back to client, but proceed to close socket anyway
                                    try: client_socket.sendall(json.dumps({"status": "error", "message": "Frame transfer/processing failed on server"}).encode("utf-8"))
                                    except Exception: pass

                                    # --- Re-queue the frame under lock ONLY if reception failed ---
                                    if not frame_recv_success:
                                         with CLIENT_LOCK:
                                             job = ACTIVE_JOBS.get(job_id) # Re-get job
                                             # Check job still active and frame not already completed/re-queued
                                             if (job and job.get("status") == "active" and
                                                     frame not in job.get("completed_frames", []) and
                                                     frame not in job.get("unassigned_frames", [])):
                                                 print(f"Render Server: Re-queueing frame {frame} for job '{job_id}' after transfer error from {client_name_for_log}.")
                                                 job.setdefault("unassigned_frames", []).append(frame)

                                             # Reset client status as the task failed
                                             client = CLIENTS.get(client_id)
                                             if client and client.current_job_id == job_id:
                                                 print(f"Render Server: Resetting client {client_name_for_log} to idle due to frame error.")
                                                 client.status = "idle"
                                                 # Clear potentially problematic frame assignment
                                                 try: client.current_frames.remove(frame)
                                                 except ValueError: pass
                                                 if not client.current_frames: client.current_job_id = None


                                    break # Exit loop, close socket on frame error

                        # --- Unknown Message Type ---
                        else:
                            print(f"Render Server: Unknown message type '{msg_type}' received from {client_name_for_log}. Message: {message}")
                            try: client_socket.sendall(json.dumps({"status": "error", "message": f"Unknown message type '{msg_type}'"}).encode("utf-8"))
                            except Exception as e: print(f"Render Server: Error sending Unknown Type Msg to {client_name_for_log}: {e}")
                            break # Exit loop, close socket

                    # --- End of Client ID known section ---

                    # If the message was handled successfully and socket should stay open, continue loop
                    if not close_socket_after_message:
                        # print(f"DEBUG: Message handled, continuing loop for {client_name_for_log}") # Debug
                        continue # Go to the start of the while True loop for the next message
                    else:
                        # If handler decided to close (e.g., error, graceful disconnect signal), break the loop
                        # print(f"DEBUG: Message handler decided to close socket for {client_name_for_log}") # Debug
                        break

                # --- Exception Handling for the current message processing block ---
                except json.JSONDecodeError as e:
                    print(f"Render Server: Failed to decode JSON from {client_name_for_log}: {e}. Raw data: {data!r}")
                    break # Exit loop, close socket
                except socket.timeout as e:
                    print(f"Render Server: Socket timeout waiting for message from {client_name_for_log}: {e}")
                    # Let the main timeout checker handle removal, just close this connection handler.
                    break # Exit loop
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
                    # Handle common network errors indicating client disconnection
                    print(f"Render Server: Client {client_name_for_log} connection error ({type(e).__name__}): {e}")
                    break # Exit loop, cleanup will handle re-queueing
                except Exception as e:
                    # Catch any other unexpected errors during message handling
                    print(f"Render Server: Unexpected error in handle_client loop for {client_name_for_log}: {e}")
                    traceback.print_exc()
                    break # Exit loop

        # --- End of while True loop --- (Reached via break)
        finally:
            # This block *always* runs when the try block is exited (normally or via exception/break)
            print(f"Render Server: Cleaning up connection handler for {client_name_for_log}.")

            # --- Re-queue Logic on Disconnect / Error Exit ---
            # This ensures work is not lost if the client disconnects unexpectedly while assigned/rendering
            if client_id:
                with CLIENT_LOCK:
                    client = CLIENTS.get(client_id)
                    if client and client.current_job_id and (client.status == "rendering" or client.status == "assigned"):
                        job_id = client.current_job_id
                        job = ACTIVE_JOBS.get(job_id)
                        if job and job.get("status") == "active":
                            # Retrieve the frames specifically assigned to *this client* from the job record
                            assigned_frames_for_client = job.get("assigned_clients", {}).get(client_id, [])
                            if assigned_frames_for_client: # Check if client was actually in the assignment list
                                completed_in_job = set(job.get("completed_frames", [])) # Use set for efficiency
                                # Find frames assigned to this client that are not yet completed
                                frames_to_reassign = [f for f in assigned_frames_for_client if f not in completed_in_job]

                                if frames_to_reassign:
                                    print(f"Render Server: Connection closed/error. Re-queueing frames {frames_to_reassign} from client {client.name} for job '{job_id}'")
                                    current_unassigned = job.setdefault("unassigned_frames", [])
                                    for frame in frames_to_reassign:
                                        # Avoid duplicates if somehow already re-queued
                                        if frame not in current_unassigned:
                                            current_unassigned.append(frame)
                                    # job["unassigned_frames"].sort() # Optional sort

                                # Remove client from job assignment list *after* potentially re-queueing
                                job["assigned_clients"].pop(client_id, None)
                            # else: Client thought it was assigned, but job didn't list it? Log inconsistency.
                            #      print(f"Render Server: Inconsistency during cleanup: Client {client.name} assigned job {job_id} but not found in job's assigned_clients list.")
                        # else: Job not found or not active during cleanup, frames likely already handled or job finished.
                    # else: Client wasn't assigned/rendering, or client record already removed (e.g., by timeout checker).

                    # --- Remove client from global list ---
                    # Always do this when the handler thread exits
                    removed_client = CLIENTS.pop(client_id, None)
                    if removed_client:
                         print(f"Render Server: Removed client {removed_client.name} ({removed_client.ip}) from active list.")
                    # else: Client likely already removed by timeout checker or another concurrent operation.

            # --- Actual Socket Closing ---
            if client_socket:
                socket_fileno = -1 # For logging purposes
                try: socket_fileno = client_socket.fileno()
                except Exception: pass
                print(f"Render Server: Closing socket fileno {socket_fileno} for {client_name_for_log}...")
                try:
                    # Shutdown signaling can sometimes help release resources faster, but can also raise errors if already closed.
                    client_socket.shutdown(socket.SHUT_RDWR)
                except OSError as e_sh:
                    # Ignore "not connected" errors, which are expected if client already disconnected
                    # errno 107: ENOTCONN on Windows, errno.ENOTCONN/EBADF on Linux/Mac
                    if e_sh.errno not in [107, socket.errno.ENOTCONN, socket.errno.EBADF]:
                         print(f"Render Server: Error shutting down socket {socket_fileno} for {client_name_for_log}: {e_sh}")
                except Exception as e_sh_gen:
                    # Catch other potential errors during shutdown
                    print(f"Render Server: Generic error shutting down socket {socket_fileno} for {client_name_for_log}: {e_sh_gen}")
                finally:
                    # Always try to close the socket
                    try:
                        client_socket.close()
                        # print(f"Render Server: Socket {socket_fileno} for {client_name_for_log} closed.")
                    except Exception as e_cl:
                        print(f"Render Server: Error closing socket {socket_fileno} for {client_name_for_log}: {e_cl}")

            print(f"Render Server: Finished connection handler for {client_name_for_log}.")


    def cancel(self, context: bpy.types.Context):
        """Stops the server thread, closes socket, cleans up resources."""
        print("Render Server: Cancel requested for server operator.")
        # Prevent running cancel logic multiple times
        if not self._running and not self._server_socket and not self._timer:
            print("Render Server: Cancel called but server appears already stopped.")
            return {"CANCELLED"}

        # 1. Signal threads to stop
        self._running = False

        # 2. Remove modal timer
        if self._timer:
            # print("Render Server: Removing modal timer...") # Debug
            wm = getattr(context, "window_manager", None)
            if wm:
                try:
                    wm.event_timer_remove(self._timer)
                    # print("Render Server: Modal timer removed.") # Debug
                except Exception as e:
                    print(f"Render Server: Error removing timer: {e}")
            self._timer = None # Clear reference even if removal failed

        # 3. Close the main server listening socket to stop accepting new connections
        if self._server_socket:
            print("Render Server: Closing server listening socket...")
            sock_fd = -1
            try: 
                sock_fd = self._server_socket.fileno()
            except Exception: 
                pass
            try:
                self._server_socket.close()
                print(f"Render Server: Server listening socket {sock_fd} closed.")
            except Exception as e:
                print(f"Render Server: Error closing server listening socket {sock_fd}: {e}")
            self._server_socket = None # Clear reference

        # 4. Wait briefly for the server accept thread to finish
        if self._server_thread and self._server_thread.is_alive():
            print("Render Server: Waiting for server accept thread to join...")
            self._server_thread.join(timeout=3.0) # Wait up to 3 seconds
            if self._server_thread.is_alive():
                print("Render Server: Warning: Server accept thread did not join cleanly within timeout.")
            else:
                print("Render Server: Server accept thread joined.")
            self._server_thread = None

        # 5. Update UI property (if scene context is still valid)
        props = getattr(context.scene, "render_server", None) if hasattr(context, "scene") else None
        if props:
            props.is_server_running = False
            # Force UI redraw if possible to reflect stopped state
            if context.window_manager and context.window:
                 for window in context.window_manager.windows:
                     for area in window.screen.areas:
                         if area.type == "PROPERTIES": area.tag_redraw()

        print("Render Server: Server cancel method finished.");
        self.report({"INFO"}, "Server stopped.")
        return {"CANCELLED"}


# --- Operator: Stop Server ---
class RENDERSERVER_OT_stop_server(bpy.types.Operator):
    bl_idname = "renderserver.stop_server"
    bl_label = "Stop Render Server"
    bl_description = "Stop the background render server listener"

    @classmethod
    def poll(cls, context: bpy.types.Context) -> bool:
         # Only allow stopping if the server properties exist and indicate it's running
         props = getattr(context.scene, "render_server", None)
         return props is not None and props.is_server_running

    def execute(self, context: bpy.types.Context) -> set:
        """Signals the running modal operator (start_server) to stop by setting the flag."""
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running:
            self.report({"WARNING"}, "Server is not running or properties missing.")
            return {"CANCELLED"}

        # The actual stopping logic (closing sockets, joining threads) is handled
        # by the start_server operator's cancel() method, which is triggered
        # when its modal() method sees this flag change.
        print("Render Server: Sending stop signal to server operator...")
        props.is_server_running = False # Set the flag

        # Force UI redraw to reflect the change immediately (e.g., disable stop button)
        if context.window_manager and context.window:
             for window in context.window_manager.windows:
                 for area in window.screen.areas:
                     if area.type == "PROPERTIES":
                         area.tag_redraw()

        self.report({"INFO"}, "Server stopping signal sent.")
        # The modal operator will detect the flag change in its next timer event
        # and call its cancel() method.
        return {"FINISHED"}


# --- Operator: Create Job ---
class RENDERSERVER_OT_send_scene(bpy.types.Operator):
    bl_idname = "renderserver.send_scene"
    bl_label = "Create Render Job"
    bl_description = "Prepare current scene, load its data to memory, and create a new render job for distribution"

    _job_id: Optional[str] = None
    _timer: Optional[bpy.types.Timer] = None
    _step: int = 0 # Progress step for modal execution

    # Store intermediate data needed across modal steps
    blend_path: Optional[str] = None
    blend_data: Optional[bytes] = None
    blend_filename: Optional[str] = None
    job_added_successfully: bool = False
    all_frames: List[int] = []
    output_dir: str = ""
    job_output_dir: str = ""
    output_format_str: str = "PNG_RGB_8" # Default format

    @classmethod
    def poll(cls, context: bpy.types.Context) -> bool:
        """Allow creating job only if server is running and NO job is currently active."""
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running:
            cls.poll_message_set("Server is not running.")
            return False

        # Check if there's an *active* job that isn't completed or errored
        active_job_id = props.active_job_id
        if active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(active_job_id)
                # Allow creating a new job only if the current job ID points to
                # a non-existent job, or a job marked completed/error.
                if job and job.get("status") == "active":
                    cls.poll_message_set(f"Job '{active_job_id}' is already active.")
                    return False
        return True # Allow if server running and no *active* job

    def _prepare_blend_file(self, context: bpy.types.Context) -> Optional[str]:
        """
        Saves the current scene to a temporary file, potentially packing textures.
        Handles temporary render setting changes. Returns the path to the temp file or None on error.
        WARNING: This operation can freeze Blender's UI.
        """
        if not self._job_id: # Should be set before calling this
             self.report({"ERROR"}, "Internal error: Job ID not set before preparing blend file.")
             return None

        temp_dir = tempfile.gettempdir()
        # Include timestamp in filename for slightly more uniqueness if needed
        timestamp = int(time.time())
        blend_filename = f"renderjob_{self._job_id}_{timestamp}.blend"
        temp_blend_path = os.path.join(temp_dir, blend_filename)
        self.blend_filename = blend_filename # Store the intended filename

        # Prevent accidentally overwriting the currently open file if it's in the temp dir somehow
        if bpy.data.filepath and os.path.normpath(bpy.data.filepath) == os.path.normpath(temp_blend_path):
             self.report({"ERROR"}, "Cannot use temporary file path identical to the currently open file.")
             return None

        # Get packing preference
        pack_textures = False
        props = getattr(context.scene, "render_server", None)
        if props: pack_textures = props.pack_textures
        else: print("Render Server: Warning: Could not access scene properties for pack_textures setting.")

        print(f"Render Server: Preparing temporary blend file for job '{self._job_id}'...")
        print(f"  Saving to: {temp_blend_path}")
        print(f"  Pack Textures: {pack_textures}")

        start_save = time.monotonic()
        try:
            # Store original render settings to restore later
            original_render_filepath = context.scene.render.filepath
            original_use_placeholder = context.scene.render.use_placeholder
            original_use_overwrite = context.scene.render.use_overwrite
            original_image_settings = context.scene.render.image_settings.copy() if hasattr(context.scene.render.image_settings, "copy") else None


            try:
                # Modify settings for the temporary save:
                # - Minimal output path to avoid embedding irrelevant user paths
                context.scene.render.filepath = "//frame_###" # Simple relative placeholder
                # - Disable placeholders/overwrite as client will handle naming
                context.scene.render.use_placeholder = False
                context.scene.render.use_overwrite = True
                 # Ensure image settings format matches what we report (consistency)
                 # Although client should override based on job message, setting it here might help with potential issues
                try:
                     format_parts = self.output_format_str.split('_')
                     context.scene.render.image_settings.file_format = format_parts[0]
                     if len(format_parts) > 1: context.scene.render.image_settings.color_mode = format_parts[1]
                     if len(format_parts) > 2: context.scene.render.image_settings.color_depth = format_parts[2]
                except Exception as e_fmt_set: print(f"Render Server: Warning - Could not set temp image format settings: {e_fmt_set}")


                # Pack textures *before* saving if requested
                if pack_textures:
                    print("  Packing textures in current scene (may take time)...")
                    pack_start_time = time.monotonic()
                    # Ensure we are in object mode for packing operation (can fail otherwise)
                    current_mode = context.mode
                    if current_mode != 'OBJECT': bpy.ops.object.mode_set(mode='OBJECT')
                    try:
                        bpy.ops.file.pack_all()
                    finally: # Restore original mode if changed
                         if context.mode != current_mode: bpy.ops.object.mode_set(mode=current_mode)
                    pack_duration = time.monotonic() - pack_start_time
                    print(f"  Texture packing finished in {pack_duration:.2f}s.")
                    # WARNING: This modifies the *current* scene state. There's no built-in easy undo
                    # for packing after the operator finishes. User needs to be aware.

                # Save the temporary blend file
                print("  Saving temporary blend file...")
                save_start_time = time.monotonic()
                bpy.ops.wm.save_as_mainfile(
                    filepath=temp_blend_path,
                    check_existing=False,
                    relative_remap=True, # Try to make paths relative within the temp file
                    compress=True # Use compression for smaller file size (usually default)
                )
                save_duration = time.monotonic() - save_start_time
                print(f"  Temporary blend file saved in {save_duration:.2f}s")

            finally:
                # Restore original render settings in the main Blender instance REGARDLESS of success/failure
                print("  Restoring original render settings in Blender UI...")
                context.scene.render.filepath = original_render_filepath
                context.scene.render.use_placeholder = original_use_placeholder
                context.scene.render.use_overwrite = original_use_overwrite
                # Restore image settings if possible
                if original_image_settings and hasattr(context.scene.render.image_settings, "copy"):
                    # Need to assign attributes individually as direct assignment might not work
                     try:
                         for key, value in original_image_settings.items():
                              setattr(context.scene.render.image_settings, key, value)
                     except Exception as e_restore: print(f"Render Server: Warning - Error restoring some image settings: {e_restore}")

                # IMPORTANT: If packing was done, the current scene *remains packed*.
                # Add a warning message about this.
                if pack_textures:
                    self.report({"WARNING"}, "Textures were packed for the job. The current scene state remains packed. Save manually if needed.")
                    print("Render Server: WARNING - Textures remain packed in the current scene.")


            total_prep_duration = time.monotonic() - start_save
            print(f"Render Server: Blend file preparation finished in {total_prep_duration:.2f}s")
            return temp_blend_path # Return path on success

        except Exception as e:
            self.report({"ERROR"}, f"Failed to save/prepare temporary blend file: {e}")
            traceback.print_exc()
            # Clean up partial file if it exists
            if os.path.exists(temp_blend_path):
                print(f"  Attempting cleanup of failed temp file: {temp_blend_path}")
                try: os.remove(temp_blend_path)
                except OSError: pass
            self.blend_filename = None # Clear filename if prep failed
            return None # Return None on error


    def execute(self, context: bpy.types.Context) -> set:
        """Initializes the job creation process and starts the modal operator."""
        # Re-check conditions from poll for safety
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running:
            self.report({"ERROR"}, "Server not running or properties missing.")
            return {"CANCELLED"}

        active_job_id = props.active_job_id
        if active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(active_job_id)
                if job and job.get("status") == "active":
                    self.report({"ERROR"}, f"Cannot create new job while job '{active_job_id}' is active.")
                    return {"CANCELLED"}

        # --- Get Job Configuration ---
        prefs = context.preferences.addons[__name__].preferences

        # Generate unique Job ID
        global JOB_COUNTER
        with JOB_COUNTER_LOCK:
            JOB_COUNTER += 1
            timestamp = int(time.time())
            self._job_id = f"job_{JOB_COUNTER:03d}_{timestamp}"

        # Get Frame Range from scene
        frame_start = context.scene.frame_start
        frame_end = context.scene.frame_end
        frame_step = context.scene.frame_step
        try:
            self.all_frames = list(range(frame_start, frame_end + 1, frame_step))
        except Exception as e:
            self.report({"ERROR"}, f"Invalid frame range settings ({frame_start}-{frame_end} step {frame_step}): {e}")
            return {"CANCELLED"}

        if not self.all_frames:
            self.report({"ERROR"}, "No frames selected in scene range.")
            return {"CANCELLED"}

        # Get Output Format string from render settings
        try:
            img_settings = context.scene.render.image_settings
            output_format = getattr(img_settings, "file_format", "PNG")
            color_mode = getattr(img_settings, "color_mode", "RGB") # RGB, RGBA, BW
            color_depth = getattr(img_settings, "color_depth", "8") # 8, 16, 32(float)
            self.output_format_str = f"{output_format}_{color_mode}_{color_depth}"
        except Exception as e_fmt:
            default_format = "PNG_RGB_8"
            print(f"Render Server: Warning - Could not read scene image format settings: {e_fmt}. Using default: {default_format}")
            self.output_format_str = default_format

        # Reset state variables for modal operation
        self._step = 0
        self.job_added_successfully = False
        self.blend_path = None
        self.blend_data = None
        self.blend_filename = None # Will be set in _prepare_blend_file
        self.output_dir = bpy.path.abspath(prefs.output_directory)
        self.job_output_dir = os.path.join(self.output_dir, self._job_id)

        self.report({"INFO"}, f"Creating Job '{self._job_id}' ({len(self.all_frames)} frames). Starting modal process...")
        print(f"Render Server: Job Details - ID: {self._job_id}, Frames: {len(self.all_frames)}, Range: {frame_start}-{frame_end}:{frame_step}, Format: {self.output_format_str}")

        # Start Modal Process using a timer for sequential steps
        wm = context.window_manager
        # Define steps for progress bar: 0=Start, 1=Save Temp, 2=Read Temp, 3=Register Job, 4=Finish/Cleanup
        wm.progress_begin(0, 4)
        self._timer = wm.event_timer_add(0.1, window=context.window) # Short delay between steps
        wm.modal_handler_add(self)
        return {"RUNNING_MODAL"}

    def modal(self, context: bpy.types.Context, event: bpy.types.Event) -> set:
        """Handles the sequential steps of job creation modally."""
        if event.type != "TIMER":
            return {"PASS_THROUGH"} # Ignore non-timer events

        wm = context.window_manager
        modal_step_start_time = time.monotonic() # For step timing debug

        # --- Step 0: Initial step, advance immediately ---
        if self._step == 0:
            wm.progress_update(1)
            self._step = 1
            # Use redraw_timer here to force immediate update of progress bar during potentially blocking ops
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)
            return {"PASS_THROUGH"} # Pass through to allow redraw and next timer event

        # --- Step 1: Save the temporary blend file (Potentially Blocking) ---
        if self._step == 1:
            print("Render Server: Modal Step 1: Preparing temporary blend file (UI may freeze)...")
            self.report({"INFO"}, "Preparing temporary blend file (UI may freeze)...") # Update status bar
            self.blend_path = self._prepare_blend_file(context) # This call can freeze UI
            step_duration = time.monotonic() - modal_step_start_time
            print(f"Render Server: Modal Step 1 finished in {step_duration:.3f}s. Path: {self.blend_path}")

            if not self.blend_path or not self.blend_filename:
                # Error reported by _prepare_blend_file
                wm.progress_end()
                return self._cancel_modal(context) # Calls cleanup

            wm.progress_update(2)
            self._step = 2
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)
            return {"PASS_THROUGH"}

        # --- Step 2: Read the temporary blend file into memory ---
        if self._step == 2:
            print(f"Render Server: Modal Step 2: Reading '{self.blend_filename}' into memory...")
            self.report({"INFO"}, "Reading temporary blend file into memory...")
            try:
                if not self.blend_path: raise FileNotFoundError("Blend path is None") # Safety check
                with open(self.blend_path, 'rb') as f:
                    self.blend_data = f.read()
                if not self.blend_data:
                    raise ValueError("Read 0 bytes from temporary blend file.")
                read_size_mb = len(self.blend_data) / (1024 * 1024)
                print(f"  Read {read_size_mb:.2f} MB into memory.")
            except Exception as e:
                self.report({"ERROR"}, f"Failed to read temporary blend file '{self.blend_path}': {e}")
                traceback.print_exc()
                wm.progress_end()
                return self._cancel_modal(context) # Calls cleanup

            wm.progress_update(3)
            self._step = 3
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)
            return {"PASS_THROUGH"}

        # --- Step 3: Register the job data in ACTIVE_JOBS (under lock) ---
        if self._step == 3:
            print(f"Render Server: Modal Step 3: Registering job '{self._job_id}' in memory...")
            self.report({"INFO"}, "Registering job...")
            try:
                if not self._job_id or not self.blend_data or not self.blend_filename:
                     raise ValueError("Job ID, blend data, or filename missing before registration.")

                with CLIENT_LOCK:
                    # Create job data structure
                    job_data = {
                        "id": self._job_id,
                        "all_frames": list(self.all_frames), # Use copy
                        "unassigned_frames": list(self.all_frames), # Use copy, will be consumed
                        "assigned_clients": {}, # {client_id: [frame1, frame2]}
                        "completed_frames": [],
                        "blend_data": self.blend_data, # Store the actual bytes
                        "blend_filename": self.blend_filename, # Store filename for client info
                        "output_format": self.output_format_str,
                        "start_time": datetime.now(),
                        "end_time": None, # Will be set on completion
                        "status": "active", # Mark as active immediately
                        # Additional metadata for tracking
                        "project_name": os.path.splitext(os.path.basename(bpy.data.filepath))[0] if bpy.data.filepath else "untitled",
                        "frame_range": f"{context.scene.frame_start}-{context.scene.frame_end}:{context.scene.frame_step}",
                        "resolution": f"{context.scene.render.resolution_x}x{context.scene.render.resolution_y} ({context.scene.render.resolution_percentage}%)",
                        "engine": context.scene.render.engine,
                        "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    
                    # Add job to active jobs
                    ACTIVE_JOBS[self._job_id] = job_data
                    self.job_added_successfully = True # Mark success *after* adding

                    # Generate JSON file with job info for tracking/queuing using the helper function
                    try:
                        update_job_json(self._job_id)
                    except Exception as e:
                        print(f"  Warning: Could not save job info JSON file: {e}")
                        traceback.print_exc()
                        # Continue with job creation even if JSON file can't be saved
                    
                    # Update UI property immediately to show the new active job
                    props = getattr(context.scene, "render_server", None)
                    if props:
                        props.active_job_id = self._job_id
                        update_job_progress_display(context) # Update progress display now
                    else:
                        print("Render Server: Error: Could not find scene.render_server properties for UI update during job registration.")

                print(f"  Job '{self._job_id}' registered successfully.")
            except Exception as e:
                print(f"Render Server: Exception during job registration: {e}")
                self.report({"ERROR"}, f"Failed to register job in memory: {e}")
                traceback.print_exc()
                wm.progress_end()
                return self._cancel_modal(context) # Calls cleanup

            wm.progress_update(4)
            self._step = 4
            bpy.ops.wm.redraw_timer(type='DRAW_WIN_SWAP', iterations=1)
            return {"PASS_THROUGH"}

        # --- Step 4: Create output directory and finish ---
        if self._step == 4:
            print(f"Render Server: Modal Step 4: Finalizing job '{self._job_id}'...")
            self.report({"INFO"}, "Finalizing job creation...")
            try:
                # Ensure the job-specific output directory exists
                os.makedirs(self.job_output_dir, exist_ok=True)
                print(f"  Output directory ensured: {self.job_output_dir}")
            except Exception as e:
                # Report as warning, as the server handler might create it later anyway
                self.report({"WARNING"}, f"Could not create job output dir '{self.job_output_dir}': {e}")
                print(f"Render Server: Warning - Could not create job output directory '{self.job_output_dir}': {e}")

            # Report final success
            self.report({"INFO"}, f"Job '{self._job_id}' created with {len(self.all_frames)} frames.")
            print(f"Render Server: Job '{self._job_id}' creation complete.")
            wm.progress_end()
            return self._finish_modal(context) # Calls final cleanup and returns FINISHED

        # --- Fallback: Should not be reached ---
        print(f"Render Server: Warning - Modal handler reached unexpected step: {self._step}")
        wm.progress_end()
        return self._cancel_modal(context)

    def _cleanup_temp_file(self):
        """Safely removes the temporary blend file if it exists."""
        if self.blend_path and os.path.exists(self.blend_path):
            print(f"Render Server: Cleaning up temporary blend file: {self.blend_path}")
            try:
                os.remove(self.blend_path)
                self.blend_path = None # Clear path only on successful removal
            except OSError as e:
                print(f"Render Server: Warning - Failed to remove temp blend file '{self.blend_path}': {e}")
                # Keep self.blend_path set if removal failed, for potential manual cleanup? Or clear anyway? Clear anyway.
                self.blend_path = None
        else:
             # If path wasn't set or file doesn't exist, just ensure var is cleared
             self.blend_path = None


    def _finish_modal(self, context: bpy.types.Context) -> set:
        """Final cleanup steps for successful modal completion."""
        # print("DEBUG: Finishing modal operator successfully.") # Debug
        # Stop the timer
        if self._timer:
             try: context.window_manager.event_timer_remove(self._timer)
             except Exception: pass # Ignore error if timer already gone
             self._timer = None

        # Clean up the temporary *file* (blend data remains in ACTIVE_JOBS)
        self._cleanup_temp_file()

        # Explicitly release large data reference from the operator instance,
        # it's now managed within ACTIVE_JOBS.
        self.blend_data = None

        # Redraw the UI to reflect final state
        if context.area: context.area.tag_redraw()
        return {"FINISHED"} # Signal successful completion

    def _cancel_modal(self, context: bpy.types.Context) -> set:
        """Cleanup steps for modal cancellation or error during execution."""
        print("Render Server: Canceling modal job creation operator.")
        # Ensure progress bar is removed if it was active
        # Check if progress is active before ending (calling end() multiple times can error)
        # Unfortunately, no easy way to check if progress is active. Wrap in try-except.
        try: context.window_manager.progress_end()
        except Exception: pass

        # Stop the timer
        if self._timer:
            try: context.window_manager.event_timer_remove(self._timer)
            except Exception: pass
            self._timer = None

        # Clean up temporary file
        self._cleanup_temp_file()

        # Release potentially large data from memory if job wasn't successfully added
        self.blend_data = None
        self.blend_filename = None # Also clear filename

        # Remove the job entry from ACTIVE_JOBS if it was partially added but failed later
        # Check the flag 'job_added_successfully' which is set *only* after successful insertion
        if self._job_id and not self.job_added_successfully:
            print(f"  Attempting removal of incomplete job entry '{self._job_id}' from ACTIVE_JOBS...")
            with CLIENT_LOCK:
                removed_job = ACTIVE_JOBS.pop(self._job_id, None)
                if removed_job: print(f"  Removed incomplete job '{self._job_id}'.")

        # Ensure UI is reset if the active_job_id was temporarily set to the failing job ID
        props = getattr(context.scene, "render_server", None)
        if props and props.active_job_id == self._job_id:
            print(f"  Resetting UI active_job_id from '{self._job_id}' to None.")
            props.active_job_id = "None"
            update_job_progress_display(context) # Reset progress display

        # Redraw the UI
        if context.area: context.area.tag_redraw()

        # Error/Cancel reason should have been reported before calling this method
        return {"CANCELLED"} # Signal cancellation


# --- Operator: Cancel Active Job ---
class RENDERSERVER_OT_cancel_job(bpy.types.Operator):
    bl_idname = "renderserver.cancel_job"
    bl_label = "Cancel Active Job"
    bl_description = "Cancel the currently active render job"
    
    job_id: bpy.props.StringProperty(
        name="Job ID",
        description="ID of the job to cancel",
        default=""
    )
    
    @classmethod
    def poll(cls, context: bpy.types.Context) -> bool:
        """Allow canceling only if server is running and the job exists and is 'active'."""
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running:
            return False
        if props.active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(props.active_job_id)
                # Allow canceling only if job exists and its status is 'active'
                return job is not None and job.get("status") == "active"
        return False
    
    def execute(self, context: bpy.types.Context) -> set:
        """Cancels the active job by marking it as 'cancelled'."""
        job_id = self.job_id
        if not job_id:
            # If no job_id provided, try to get from active_job_id
            props = getattr(context.scene, "render_server", None)
            if props:
                job_id = props.active_job_id
        
        if not job_id or job_id == "None":
            self.report({"ERROR"}, "No job ID provided or active job found")
            return {"CANCELLED"}
        
        print(f"Render Server: Cancelling job '{job_id}'...")
        cancelled = False
        
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if job and job.get("status") == "active":
                # Mark job as cancelled
                job["status"] = "cancelled"
                job["end_time"] = datetime.now()
                
                # Clean up blend data to free memory
                job.pop("blend_data", None)
                
                # Add a message to the job
                job["cancel_reason"] = "Cancelled by user"
                
                print(f"Render Server: Job '{job_id}' has been cancelled")
                cancelled = True
                
                # Update the JSON file with cancelled status
                update_job_json(job_id)
                
                # Optional: Notify any clients with assigned frames
                # This happens naturally when they try to report frames or request new ones
            else:
                if not job:
                    print(f"Render Server: Cannot cancel job '{job_id}', not found")
                else:
                    print(f"Render Server: Cannot cancel job '{job_id}', status is '{job.get('status')}'")
        
        if cancelled:
            self.report({"INFO"}, f"Job '{job_id}' has been cancelled")
            # Force UI redraw
            for window in context.window_manager.windows:
                for area in window.screen.areas:
                    if area.type == "PROPERTIES":
                        area.tag_redraw()
            return {"FINISHED"}
        else:
            self.report({"ERROR"}, f"Could not cancel job '{job_id}'")
            return {"CANCELLED"}

# --- Operator: Clear Completed Job ---
class RENDERSERVER_OT_clear_completed_job(bpy.types.Operator):
    bl_idname = "renderserver.clear_completed_job"
    bl_label = "Clear Completed Job Record"
    bl_description = "Remove the record of the completed job from memory (rendered files remain on disk)"
    
    job_id: bpy.props.StringProperty(
        name="Job ID",
        description="ID of the job to clear",
        default=""
    )

    @classmethod
    def poll(cls, context: bpy.types.Context) -> bool:
        """Allow clearing only if server is running and the active job exists and is 'completed'."""
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running: # Server must be running conceptually
            return False
        if props.active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(props.active_job_id)
                # Allow clearing only if job exists and its status is 'completed'
                return job is not None and job.get("status") == "completed"
        return False # No active job ID selected

    def execute(self, context: bpy.types.Context) -> set:
        """Removes the completed job data from the ACTIVE_JOBS dictionary."""
        props = getattr(context.scene, "render_server", None)
        if not props: return {"CANCELLED"} # Should not happen if poll passed

        # Get job ID either from operator property or from active job
        job_id_to_clear = self.job_id
        if not job_id_to_clear:
            job_id_to_clear = props.active_job_id
            
        if job_id_to_clear == "None" or not job_id_to_clear:
             self.report({"WARNING"}, "No job selected to clear.")
             return {"CANCELLED"}

        print(f"Render Server: Clearing completed job '{job_id_to_clear}' data from memory...")
        job_output_dir_log = "N/A" # For logging path info
        job_cleared = False

        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id_to_clear)
            if job and job.get("status") == "completed": # Double check status before removing
                 # Get output dir path *before* popping job for the log message
                 try:
                     prefs = context.preferences.addons[__name__].preferences
                     output_dir = bpy.path.abspath(prefs.output_directory)
                     job_output_dir_log = os.path.join(output_dir, job_id_to_clear)
                 except Exception as e_path: print(f"Render Server: Warning - could not determine job output path for log: {e_path}")

                 # Remove job data from memory
                 # The blend_data should have already been removed when status became 'completed'
                 ACTIVE_JOBS.pop(job_id_to_clear)
                 print(f"  Removed job data for '{job_id_to_clear}' from memory.")
                 job_cleared = True
            elif job:
                 # This case shouldn't be reachable if poll() works correctly
                 print(f"Render Server: Cannot clear job '{job_id_to_clear}', status is '{job.get('status')}' (expected 'completed').")
                 self.report({"WARNING"}, f"Cannot clear job, status is '{job.get('status')}'.")
                 return {"CANCELLED"}
            else:
                 # Job ID was set in UI, but job disappeared from memory? Treat as cleared for UI reset.
                 print(f"Render Server: Job '{job_id_to_clear}' not found in active jobs list during clear (already cleared?).")
                 job_cleared = True # Allow UI reset

            # Reset UI properties if the cleared job was the one displayed
            if job_cleared and props.active_job_id == job_id_to_clear:
                print("  Resetting UI active job display.")
                props.active_job_id = "None"
                update_job_progress_display(context) # Update progress display immediately (will reset it)

        if job_cleared:
             self.report({"INFO"}, f"Cleared job '{job_id_to_clear}'. Files remain in '{job_output_dir_log}'.")
             # Force UI redraw
             if context.window_manager and context.window:
                  for window in context.window_manager.windows:
                      for area in window.screen.areas:
                          if area.type == "PROPERTIES":
                              area.tag_redraw()
        # Else: warnings already reported

        return {"FINISHED"}


# --- Main UI Panel ---
class RENDERSERVER_OT_force_job_complete(bpy.types.Operator):
    bl_idname = "renderserver.force_job_complete"
    bl_label = "Force Complete"
    bl_description = "Force an active job to be marked as completed when it's stuck"
    
    job_id: bpy.props.StringProperty(
        name="Job ID",
        description="ID of the job to force complete",
        default=""
    )
    
    @classmethod
    def poll(cls, context: bpy.types.Context) -> bool:
        """Allow forcing completion only if server is running and the job exists and is 'active'."""
        props = getattr(context.scene, "render_server", None)
        if not props or not props.is_server_running:
            return False
        if props.active_job_id != "None":
            with CLIENT_LOCK:
                job = ACTIVE_JOBS.get(props.active_job_id)
                # Allow completion for active jobs
                return job is not None and job.get("status") == "active"
        return False
    
    def execute(self, context: bpy.types.Context) -> set:
        """Forces the active job to be marked as completed."""
        job_id = self.job_id
        if not job_id:
            # If no job_id provided, try to get from active_job_id
            props = getattr(context.scene, "render_server", None)
            if props:
                job_id = props.active_job_id
        
        if not job_id or job_id == "None":
            self.report({"ERROR"}, "No job ID provided or active job found")
            return {"CANCELLED"}
        
        print(f"Render Server: Force completing job '{job_id}'...")
        completed = False
        
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if job and job.get("status") == "active":
                # Get counts for logging
                total_frames = len(job.get("all_frames", []))
                completed_frames = len(job.get("completed_frames", []))
                
                # Mark job as completed
                job["status"] = "completed"
                job["end_time"] = datetime.now()
                
                # Clean up blend data to free memory
                job.pop("blend_data", None)
                
                # Add a message to the job
                job["forced_completion"] = True
                job["completion_note"] = f"Forced complete at {completed_frames}/{total_frames} frames"
                
                print(f"Render Server: Job '{job_id}' has been force-completed with {completed_frames}/{total_frames} frames done")
                completed = True
                
                # Update the JSON file with completed status
                update_job_json(job_id)
            else:
                if not job:
                    print(f"Render Server: Cannot force-complete job '{job_id}', not found")
                else:
                    print(f"Render Server: Cannot force-complete job '{job_id}', status is '{job.get('status')}'")
        
        if completed:
            self.report({"INFO"}, f"Job '{job_id}' has been force-completed")
            # Force UI redraw
            for window in context.window_manager.windows:
                for area in window.screen.areas:
                    if area.type == "PROPERTIES":
                        area.tag_redraw()
            return {"FINISHED"}
        else:
            self.report({"ERROR"}, f"Could not force-complete job '{job_id}'")
            return {"CANCELLED"}

class RENDERSERVER_PT_main_panel(bpy.types.Panel):
    bl_label = "Render Server (In-Memory)"
    bl_idname = "RENDERSERVER_PT_main_panel_minimal_memory"
    bl_space_type = "PROPERTIES"
    bl_region_type = "WINDOW"
    bl_context = "render" # Show in Render Properties tab

    def draw(self, context: bpy.types.Context):
        layout = self.layout
        props = getattr(context.scene, "render_server", None)
        if not props:
            layout.label(text="Error: Render Server properties missing!", icon="ERROR")
            layout.label(text="Try re-registering the addon.")
            return

        try:
            prefs = context.preferences.addons[__name__].preferences
        except KeyError: # Addon might be in process of unregistering or failed registration
             layout.label(text="Error accessing addon preferences.", icon="ERROR")
             layout.label(text="Ensure addon is enabled and registered correctly.")
             return
        except Exception as e:
             layout.label(text=f"Error accessing addon preferences: {e}", icon="ERROR")
             return

        # --- Server Status Section ---
        box = layout.box()
        row = box.row(align=True)
        row.label(text="Server Control", icon="WORLD")
        if props.is_server_running:
            row_op = row.row(align=True)
            row_op.scale_x = 1.5 # Make button wider
            row_op.operator("renderserver.stop_server", icon="PAUSE", text="Stop Server")
            # Display port from global as it's set when server starts
            row.label(text=f"Running: Port {SERVER_PORT}", icon="RADIOBUT_ON")
        else:
            row_op = row.row(align=True)
            row_op.scale_x = 1.5
            # Use invoke context to potentially catch errors during startup better
            row_op.operator("renderserver.start_server", icon="PLAY", text="Start Server")
            # Display port from preferences when stopped
            row.label(text=f"Stopped (Port: {prefs.server_port})", icon="RADIOBUT_OFF")

        if props.is_server_running:
            try:
                hostname = socket.gethostname()
                ip_address = "Checking..." # Placeholder
                # Try a reliable method first (connecting to external dummy IP)
                try:
                     with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                         s.settimeout(0.1) # Short timeout, don't block UI
                         s.connect(('8.8.8.8', 1)) # Google DNS, port 1 (unlikely to be blocked outbound)
                         ip_address = s.getsockname()[0]
                except Exception:
                     # Fallback using hostname resolution (might give 127.0.0.1 or internal IP)
                     try:
                         ip_address = socket.gethostbyname(hostname)
                     except Exception:
                         ip_address = "Could not determine IP"

                row = box.row()
                row.label(text=f"Server IP Address: {ip_address}:{prefs.server_port}")
            except Exception as ip_e:
                 print(f"Render Server: UI Error - Could not determine server IP: {ip_e}")
                 row = box.row()
                 row.label(text="Server IP: Unknown", icon='ERROR')

            row = box.row()
            row.label(text=f"Client Timeout: {CLIENT_TIMEOUT_SECONDS} seconds")

        # --- Job Management Section ---
        job_box = layout.box()
        job_box.label(text="Render Job Management", icon="RENDER_ANIMATION")

        if not props.is_server_running:
            job_box.label(text="Start the server to create or manage jobs.")
        else:
            # --- Get current job state (read under lock) ---
            active_job_id = props.active_job_id
            job_status = "None"
            job_exists = False
            is_completed = False
            is_active = False
            job_duration_str = ""
            job_start_time_str = ""

            if active_job_id != "None":
                with CLIENT_LOCK: # Read job status under lock
                    job = ACTIVE_JOBS.get(active_job_id)
                    if job:
                        job_exists = True
                        job_status = job.get("status", "unknown")
                        is_completed = job_status == "completed"
                        is_active = job_status == "active"

                        start_time = job.get("start_time")
                        if start_time:
                            job_start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

                        if is_completed and job.get("end_time") and start_time:
                            try:
                                duration = job["end_time"] - start_time
                                total_seconds = max(0, duration.total_seconds()) # Ensure non-negative
                                hours, remainder = divmod(total_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                job_duration_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
                            except Exception: job_duration_str = "Error calculating"

            # --- UI Display Logic ---
            if not is_active:
                # Display completed job summary and clear button
                if is_completed and job_exists:
                    col = job_box.column(align=True)
                    row = col.row(align=True)
                    row.label(text=f"Job '{active_job_id}' Completed", icon="CHECKMARK")
                    # Add clear button next to the status
                    clear_op = row.operator("renderserver.clear_completed_job", text="", icon="X")
                    clear_op.job_id = active_job_id

                    col.label(text=f"Started: {job_start_time_str}")
                    col.label(text=f"Duration: {job_duration_str}")
                    col.label(text=f"Frames: {props.completed_frames_count} / {props.total_frames}")
                    job_box.separator() # Separate completed summary from next job settings

                # Display settings for the *next* job if no job is active
                settings_box = job_box.box()
                col = settings_box.column(align=True)
                col.label(text="Next Job Configuration:")
                col.prop(props, "frame_chunks")
                col.prop(props, "pack_textures")
                
                # Video creation options
                video_col = col.column()
                video_col.prop(props, "create_video")
                
                # Only show codec and quality options if video creation is enabled
                if props.create_video:
                    video_options = video_col.column(align=True)
                    video_options.prop(props, "video_location")
                    video_options.prop(props, "video_codec")
                    video_options.prop(props, "video_quality")
                
                # Display scene frame range info
                row = col.row()
                row.label(text=f"Scene Range: {context.scene.frame_start} - {context.scene.frame_end} (Step: {context.scene.frame_step})")
                col.label(text="Note: Job creation loads blend data to RAM.", icon="INFO")
                col.label(text="      UI may freeze during preparation.", icon="BLANK1") # Indent note

                # Create Job Button (poll method enables/disables based on active job state)
                row = job_box.row()
                row.scale_y = 1.5 # Make button prominent
                row.operator("renderserver.send_scene", icon="ADD", text="Create New Render Job")

            # --- Active Job Status Area ---
            elif is_active and job_exists: # Job is active and its data exists
                col = job_box.column(align=True)
                row = col.row()
                # Display status with an icon
                status_icon = "PLAY" if job_status == "active" else "QUESTION" # Adjust icons if more statuses added
                row.label(text=f"Active Job: {active_job_id}", icon=status_icon)
                if job_start_time_str:
                    row.label(text=f"(Started: {job_start_time_str})")

                row = col.row()
                # Show progress bar (updated by modal timer)
                row.prop(props, "job_progress", text="Progress", slider=True)

                row = col.row()
                # Show frame counts (updated by modal timer)
                row.label(text=f"Frames Completed: {props.completed_frames_count} / {props.total_frames}")
                
                # Check if job is potentially stalled
                with CLIENT_LOCK:
                    job = ACTIVE_JOBS.get(active_job_id)
                    if job and job.get("stalled_detected"):
                        stall_row = col.row()
                        stall_row.alert = True  # Make the text red
                        stall_row.label(text="This job appears to be stalled. Consider forcing completion.", icon="ERROR")
                
                # Add buttons for job actions
                buttons_row = col.row(align=True)
                buttons_row.scale_y = 1.2  # Make the buttons slightly taller
                
                # Cancel button
                cancel_op = buttons_row.operator("renderserver.cancel_job", icon="CANCEL", text="Cancel Job")
                cancel_op.job_id = active_job_id # Pass ID to operator
                
                # Force complete button
                complete_op = buttons_row.operator("renderserver.force_job_complete", icon="CHECKMARK", text="Force Complete")
                complete_op.job_id = active_job_id # Pass ID to operator

            # Handle case where UI shows an active_job_id, but the job data is missing
            elif active_job_id != "None" and not job_exists:
                 col = job_box.column(align=True)
                 col.label(text=f"Error: Active job ID '{active_job_id}' set, but job data not found in memory.", icon="ERROR")
                 col.label(text="  Try clearing the job record if it was completed.")
                 # Maybe add a button to force reset props.active_job_id = "None"?
                 # if col.operator("renderserver.reset_ui_job_id", text="Reset UI Job ID"): pass


        # --- Connected Clients Section ---
        client_box = layout.box()
        # Get a thread-safe copy of client data for UI drawing
        clients_copy_data = []
        with CLIENT_LOCK:
            now = datetime.now()
            # Sort clients alphabetically by name for consistent display order
            sorted_clients = sorted(CLIENTS.values(), key=lambda c: c.name.lower())
            for client in sorted_clients:
                try:
                    ping_delta = now - client.last_ping
                    ping_delta_seconds = max(0, ping_delta.total_seconds()) # Ensure non-negative
                except Exception:
                    ping_delta_seconds = -1 # Indicate error

                # Determine assigned frames count (if applicable)
                assigned_frames_count = 0
                if client.status in ["rendering", "assigned"] and client.current_job_id:
                     # Get count directly from client.current_frames if reliable,
                     # or could re-check job.assigned_clients[client.id] under lock if needed (more complex)
                     assigned_frames_count = len(client.current_frames)

                clients_copy_data.append({
                    "name": client.name,
                    "ip": client.ip,
                    "status": client.status,
                    "job_id": client.current_job_id if client.current_job_id else "-",
                    "frames_assigned": assigned_frames_count,
                    "ping_sec": ping_delta_seconds,
                })

        row = client_box.row()
        row.label(text=f"Connected Clients: {len(clients_copy_data)}", icon="COMMUNITY")

        if clients_copy_data:
            # Display client details in columns
            col = client_box.column(align=True)
            # Header Row (Optional)
            # header = col.row(align=True)
            # header.label(text="Name")
            # header.label(text="IP Address")
            # header.label(text="Status")
            # header.label(text="Last Ping")

            for cd in clients_copy_data:
                row = col.row(align=True)
                # Determine icon based on status
                icon = "QUESTION" # Default
                if cd["status"] == "rendering": icon = "RENDER_STILL"
                elif cd["status"] == "assigned": icon = "PAUSE" # Assigned but maybe not started transfer/render
                elif cd["status"] == "idle": icon = "PLAY" # Ready for work
                elif cd["status"] == "error": icon = "ERROR"

                row.label(text=f"{cd['name']}", icon=icon)
                row.label(text=f"{cd['ip']}")

                # Display status, potentially with frame count
                status_display = cd["status"].capitalize()
                if cd["status"] in ["rendering", "assigned"] and cd["frames_assigned"] > 0:
                    status_display += f" ({cd['frames_assigned']}f)"
                elif cd["status"] in ["rendering", "assigned"] and cd["job_id"] != "-":
                     # Show assigned even if frame count is 0 or unknown briefly
                     status_display += " (Assigned)"


                row.label(text=status_display)

                # Display last ping time nicely
                if cd["ping_sec"] < 0:
                     row.label(text="Ping Error", icon='ERROR')
                elif cd["ping_sec"] < 2.0:
                     row.label(text="< 2s ago")
                elif cd["ping_sec"] < 60.0:
                    row.label(text=f"{cd['ping_sec']:.0f}s ago")
                else:
                    row.label(text=f"{cd['ping_sec']/60.0:.1f}m ago")

                # Add a small spacer or adjust column widths if needed for alignment
                # row.label(text="") # Simple spacer

        else: # No clients connected
            client_box.label(text="No clients currently connected.")

        # --- Output Directory Section ---
        output_box = layout.box()
        output_box.label(text="Render Output Base Directory", icon="FILE_FOLDER")
        # Allow editing the preference directly
        output_box.prop(prefs, "output_directory", text="")
        # Display the resolved absolute path and check validity
        try:
            abs_path = bpy.path.abspath(prefs.output_directory)
            # Check if the directory exists or if its parent exists (more lenient check)
            is_valid = os.path.isdir(abs_path) or os.path.isdir(os.path.dirname(abs_path))
            icon = 'CHECKMARK' if is_valid else ('ERROR' if abs_path else 'QUESTION') # Error if path exists but isn't dir, Q if empty
            output_box.label(text=f"Resolved Path: {abs_path}", icon=icon)
            if not is_valid and abs_path:
                output_box.label(text="Warning: Path may not be creatable.", icon="ERROR")
        except Exception as e_path:
            output_box.label(text=f"Invalid path expression: {e_path}", icon="ERROR")


# --- Registration ---
classes = (
    RenderServerPreferences,
    RenderServerProperties,
    RENDERSERVER_OT_start_server,
    RENDERSERVER_OT_stop_server,
    RENDERSERVER_OT_send_scene,
    RENDERSERVER_OT_cancel_job,
    RENDERSERVER_OT_force_job_complete,
    RENDERSERVER_OT_clear_completed_job,
    RENDERSERVER_PT_main_panel,
)

def register():
    print(f"Registering addon: {bl_info['name']}")
    global _registered_classes
    _registered_classes = set()  # Initialize/clear the set
    registration_successful = True
    print(f"  Registering {len(classes)} classes...")
    for cls in classes:
        try:
            bpy.utils.register_class(cls)
            _registered_classes.add(cls)  # Track successfully registered class
            # print(f"    Registered: {cls.__name__}") # Debug noise
        except Exception as e:
            print(f"  ERROR registering class {cls.__name__}: {e}")
            traceback.print_exc()
            registration_successful = False

    if registration_successful:
        # Ensure property group class was registered before assigning pointer
        if RenderServerProperties in _registered_classes:
            try:
                bpy.types.Scene.render_server = bpy.props.PointerProperty(
                    type=RenderServerProperties,
                    name="Render Server Properties" # Add name for clarity in RNA
                )
                print("  Registered Scene.render_server PointerProperty.")
            except Exception as e:
                print(f"  ERROR registering Scene.render_server PointerProperty: {e}")
                traceback.print_exc()
                registration_successful = False
        else:
            print("  Error: RenderServerProperties class failed registration. Skipping Scene property assignment.")
            registration_successful = False
    else:
        print("  Skipping Scene property assignment due to class registration errors.")

    if registration_successful:
        print(f"{bl_info['name']} registered successfully.")
    else:
        print(f"!!! {bl_info['name']} registration encountered errors. Addon may not function correctly. !!!")
        # Attempt cleanup if registration failed mid-way to avoid partial state
        print("!!! Attempting cleanup after failed registration... !!!")
        # Use a direct call to unregister, avoid relying on bpy.ops if registration failed badly
        _unregister_internal()


def _unregister_internal():
    """Internal helper for unregistration logic, called by unregister() and error handler."""
    print(f"Internal unregister logic for {bl_info['name']}...")
    global _registered_classes

    # --- 1. Delete Scene Property First ---
    # Crucial to delete PointerProperty before unregistering the PropertyGroup class it points to.
    if hasattr(bpy.types.Scene, "render_server"):
        print("  Deleting Scene.render_server property...")
        try:
            del bpy.types.Scene.render_server
        except Exception as e:
            print(f"  Error deleting Scene.render_server property: {e}")
            # Continue cleanup attempt even if this fails

    # --- 2. Unregister Classes ---
    # Unregister in reverse order of registration based on the original list 'classes'
    print(f"  Unregistering {len(_registered_classes)} tracked classes...")
    unregistration_errors = False
    for cls in reversed(classes): # Iterate original list in reverse
         if cls in _registered_classes: # Check if it was successfully registered
             try:
                 bpy.utils.unregister_class(cls)
                 # print(f"    Unregistered: {cls.__name__}") # Debug noise
             except Exception as e:
                 print(f"    Error unregistering class {cls.__name__}: {e}")
                 unregistration_errors = True
                 # Continue attempting to unregister others
         # else: Class wasn't in the set, likely not registered or already unregistered (e.g., during error cleanup)

    _registered_classes.clear() # Clear the tracking set after attempting all
    if unregistration_errors: print("  !!! Encountered errors during class unregistration. !!!")

    # --- 3. Clean up Global Data (Sockets handled by operator cancel, focus on data here) ---
    print("  Cleaning up global data (Clients, Jobs)...")
    global CLIENTS, ACTIVE_JOBS, JOB_COUNTER, SERVER_PORT
    cleaned_jobs_count = 0
    try:
        with CLIENT_LOCK: # Use RLock
             # Clear client list
             client_count = len(CLIENTS)
             CLIENTS.clear()
             print(f"    Cleared {client_count} client record(s).")

             # Clear active job list (release blend_data references)
             job_ids_to_clear = list(ACTIVE_JOBS.keys())
             cleaned_jobs_count = len(job_ids_to_clear)
             for job_id in job_ids_to_clear:
                  job_data = ACTIVE_JOBS.pop(job_id, None)
                  # Explicitly dereference blend_data if it exists, just in case
                  if job_data and "blend_data" in job_data: del job_data["blend_data"]
             print(f"    Cleared {cleaned_jobs_count} active job record(s) from memory.")

    except Exception as e_clear:
         print(f"  Error during global data cleanup: {e_clear}")
         # Force reset containers in case of error during clear
         CLIENTS = {}
         ACTIVE_JOBS = {}

    # Reset job counter and server port? Optional, but safer for clean state.
    JOB_COUNTER = 0
    # SERVER_PORT = 9090 # Keep last used port or reset? Resetting is safer.
    # Let's keep SERVER_PORT as it might reflect user preference, just reset counter.

    print(f"Internal unregister logic finished.")


def unregister():
    """Main unregistration function called by Blender."""
    print(f"Unregistering addon: {bl_info['name']}")
    context = bpy.context # Get context once

    # --- Attempt Graceful Server Stop ---
    # Check if context and scene are available (might not be during some shutdown phases)
    render_server_props = None
    if hasattr(context, "scene") and context.scene:
         render_server_props = getattr(context.scene, "render_server", None)

    if render_server_props and getattr(render_server_props, "is_server_running", False):
        print("  Server is running, attempting graceful stop via operator...")
        try:
            # Use INVOKE_DEFAULT to run the operator properly
            bpy.ops.renderserver.stop_server('INVOKE_DEFAULT')
            # Give the operator's modal loop a moment to react and call cancel()
            time.sleep(0.2) # Short delay
            print("  Stop signal sent. Cancel logic should handle socket/thread cleanup.")
        except Exception as e:
             print(f"  Could not call stop_server operator ({type(e).__name__}: {e}).")
             print(f"  Server socket/thread might not be cleaned up automatically if operator failed.")
             # As a fallback, manually set the flag, though the operator's cancel is preferred for cleanup
             try:
                  if render_server_props.is_server_running: render_server_props.is_server_running = False
             except Exception as e_flag: print(f"    Error setting running flag manually: {e_flag}")
    else:
        print("  Server not running or properties inaccessible.")

    # --- Call internal unregistration logic ---
    _unregister_internal()

    print(f"{bl_info['name']} unregistered.")


# Standard Blender addon execution guard for testing/running directly
if __name__ == "__main__":
    # This block allows running the script directly in Blender's text editor
    # Be cautious, as it might conflict if the addon is also installed normally.
    # It's generally better to install the addon through Blender's preferences.
    print(f"Running {__file__} directly...")
    try:
        # Ensure clean state before registering if run directly
        try: unregister()
        except Exception: pass # Ignore errors if not previously registered
        register()
    except Exception as e:
         print(f"Error running script directly: {e}")
         traceback.print_exc()

