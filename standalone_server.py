#!/usr/bin/env python3
"""
Standalone Blender Render Farm Server
=====================================

This standalone server provides a web interface for the Blender Render Farm system.
It allows uploading blend files and managing render jobs without running Blender.

Key features:
- Uses the same socket server logic as the Blender plugin
- Provides a web interface for job management
- Supports job queuing and management
- Compatible with existing render farm clients

Usage:
    python standalone_server.py [--port PORT] [--web-port WEB_PORT] [--output-dir OUTPUT_DIR]

Author: Claude
License: APACHE 2.0
"""

import os
import sys
import socket
import json
import threading
import tempfile
import time
import traceback
import argparse
import queue
import logging
import glob
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any, Set
from pathlib import Path
import shutil

# Web server imports
from flask import Flask, request, jsonify, render_template, send_from_directory
from werkzeug.utils import secure_filename

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BlenderFarmServer")

# --- Global Variables ---
CLIENTS: Dict[str, 'RenderClient'] = {}
CLIENT_LOCK = threading.RLock()  # Use Re-entrant Lock for shared client/job data
ACTIVE_JOBS: Dict[str, Dict[str, Any]] = {}  # Job dict will contain 'blend_data' (bytes)
JOB_QUEUE: queue.Queue = queue.Queue()  # Queue for pending jobs
SERVER_PORT = 9090  # Default, can be overridden by commandline
WEB_PORT = 8080  # Default web interface port
CLIENT_TIMEOUT_SECONDS = 180  # Seconds before considering a client disconnected
JOB_COUNTER = 0
JOB_COUNTER_LOCK = threading.Lock()
OUTPUT_DIR = os.path.join(os.getcwd(), "render_output")  # Default output directory
STATUS_UPDATE_INTERVAL = 2.0  # Seconds for UI refresh timer

# --- Flask app ---
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024  # 1GB max upload
app.config['UPLOAD_FOLDER'] = os.path.join(tempfile.gettempdir(), "blender_farm_uploads")
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


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


# --- Helper Functions ---
def get_client_id(ip: str, name: str) -> str:
    """Generates a unique ID for a client based on IP and name."""
    return f"{ip}-{name}"


def get_next_job_id() -> str:
    """Generates a unique job ID with timestamp."""
    global JOB_COUNTER
    with JOB_COUNTER_LOCK:
        JOB_COUNTER += 1
        timestamp = int(time.time())
        return f"job_{JOB_COUNTER:03d}_{timestamp}"


def update_job_json(job_id: str) -> bool:
    """
    Updates the JSON file for a job with the current status and progress.
    This function should be called whenever a job's status changes.
    The JSON file is stored one level up from the job's output directory.
    """
    try:
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if not job:
                logger.error(f"Cannot update JSON for job '{job_id}', job not found")
                return False
                
            # Store JSON in the output directory
            json_path = os.path.join(OUTPUT_DIR, f"{job_id}_info.json")
            
            # Make sure directory exists
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            
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
            logger.info(f"Job info saved to: {json_path}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating JSON for job '{job_id}': {e}")
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
                    logger.warning(f"Job '{job_id}' appears to be stalled. {completed_frames}/{total_frames} frames completed, {unassigned_frames} unassigned, {assigned_frames} assigned.")
                    stalled_jobs.append(job_id)
                    
                    # Update the JSON file with stalled status
                    update_job_json(job_id)
        
        return stalled_jobs


def update_job_progress():
    """Updates job progress stats and files."""
    with CLIENT_LOCK:
        for job_id, job in ACTIVE_JOBS.items():
            if job.get("status") == "active":
                total = len(job.get("all_frames", []))
                completed = len(job.get("completed_frames", []))
                progress = (completed / total) * 100.0 if total > 0 else 0.0
                
                # Update JSON if progress has changed significantly 
                last_progress = job.get("last_json_progress", 0)
                if "last_json_progress" not in job or abs(progress - last_progress) >= 5.0:
                    job["last_json_progress"] = progress
                    update_job_json(job_id)


def check_timeouts(timeout_delta: timedelta):
    """Checks for client timeouts and reassigns their work."""
    frames_to_requeue = {}  # {job_id: [frames]}
    clients_to_remove = []

    with CLIENT_LOCK:
        now = datetime.now()
        logger.debug("Checking for client timeouts...")
        
        # Log all current clients first
        logger.debug(f"Current clients during timeout check: {len(CLIENTS)}")
        for client_id, client in CLIENTS.items():
            logger.debug(f"  Client: {client_id} - {client.name} - Last ping: {client.last_ping}")
        
        for client_id, client in CLIENTS.items():
            try:
                time_since_ping = now - client.last_ping
                
                if time_since_ping > timeout_delta:
                    logger.warning(f"Client timeout: {client.name} ({client.ip}) - Last ping: {client.last_ping}")
                    clients_to_remove.append(client_id)
                    
                    # Check if client had an active job assignment that needs re-queueing
                    if client.current_job_id and (client.status == "assigned" or client.status == "rendering"):
                        job_id = client.current_job_id
                        job = ACTIVE_JOBS.get(job_id)
                        
                        # Ensure job exists, is active, and client was actually assigned frames
                        if (job and job.get("status") == "active" and 
                                client_id in job.get("assigned_clients", {})):
                            
                            current_assigned_frames = job["assigned_clients"].get(client_id, [])
                            completed_in_job = set(job.get("completed_frames", []))
                            
                            # Find frames assigned to this client that aren't yet completed
                            requeue_for_job = [f for f in current_assigned_frames if f not in completed_in_job]
                            
                            if requeue_for_job:
                                logger.info(f"Timeout: Re-queueing frames {requeue_for_job} from job '{job_id}'")
                                frames_to_requeue.setdefault(job_id, []).extend(requeue_for_job)
                            
                            # Remove client from job assignment list
                            job["assigned_clients"].pop(client_id, None)
            except Exception as e:
                logger.error(f"Error checking timeout for client {client_id}: {e}")
                # Add to remove list if there was an error
                clients_to_remove.append(client_id)
        
        # Remove timed-out clients from the main list
        for client_id in clients_to_remove:
            try:
                CLIENTS.pop(client_id, None)
                logger.info(f"Removed timed-out client: {client_id}")
            except Exception as e:
                logger.error(f"Error removing client {client_id}: {e}")
        
        # Re-queue frames from timed-out clients
        for job_id, frames in frames_to_requeue.items():
            job = ACTIVE_JOBS.get(job_id)
            if job and job.get("status") == "active":
                for frame in frames:
                    if frame not in job.get("unassigned_frames", []):
                        job.setdefault("unassigned_frames", []).append(frame)


# --- Server Functions ---
def start_server_socket(port: int = SERVER_PORT) -> Optional[socket.socket]:
    """Starts the server socket for client connections."""
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Set timeout for socket operations
        server_socket.settimeout(1.0)  # 1 second timeout for accept
        
        # Bind to all interfaces
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(5)  # Allow up to 5 queued connections
        
        logger.info(f"Server socket started on port {port}")
        return server_socket
    except Exception as e:
        logger.error(f"Error starting server socket: {e}")
        traceback.print_exc()
        return None


def handle_client(client_socket: socket.socket, client_address: Tuple[str, int]):
    """Handles communication with a single connected client. Runs in its own thread."""
    client_ip = client_address[0]
    client_port = client_address[1]
    logger.info(f"Handling connection from {client_ip}:{client_port}")
    client_id: Optional[str] = None
    client_name_for_log: str = f"Unknown ({client_ip})"
    close_socket_after_message = True  # Default: close after handling one message
    
    # Debug log of current clients
    with CLIENT_LOCK:
        logger.info(f"Current clients before handling: {len(CLIENTS)} - {list(CLIENTS.keys())}")

    # Main loop to handle multiple messages from this client connection
    try:
        while True:  # Loop until break or exception
            try:
                # Read message (with timeout set)
                client_socket.settimeout(30.0)  # Timeout for receiving a message
                message_data = client_socket.recv(65536)  # 64KB buffer should be enough for messages (not files)
                client_socket.settimeout(None)  # Reset timeout
                
                if not message_data:
                    logger.info(f"Client {client_name_for_log} disconnected (clean close).")
                    break  # Client closed connection
                
                # Try to decode as JSON
                try:
                    message = json.loads(message_data.decode('utf-8'))
                    message_type = message.get('type', 'unknown')
                    
                    # --- Handle Message Types ---
                    if message_type == 'register':
                        # Client registration
                        client_name = message.get('name', f"Unknown_{int(time.time())}")
                        client_name_for_log = f"{client_name} ({client_ip})"
                        client_id = get_client_id(client_ip, client_name)
                        
                        logger.info(f"Registration from {client_name_for_log}")
                        
                        # Create/update client record under lock
                        with CLIENT_LOCK:
                            # Create new client object
                            new_client = RenderClient(
                                client_id=client_id,
                                ip=client_ip,
                                name=client_name,
                                status="idle"
                            )
                            CLIENTS[client_id] = new_client
                            logger.info(f"Client registered: {client_id} - {client_name} - Current client count: {len(CLIENTS)}")
                            
                            # Debug - list all clients
                            for cid, c in CLIENTS.items():
                                logger.debug(f"  Client: {cid} - {c.name} - {c.status}")
                        
                        # Send confirmation
                        response = {'status': 'ok', 'message': 'Registration successful'}
                        client_socket.sendall(json.dumps(response).encode('utf-8'))
                        close_socket_after_message = False  # Keep connection open
                    
                    elif message_type == 'heartbeat':
                        # Client heartbeat
                        client_name = message.get('name', 'unknown')
                        client_name_for_log = f"{client_name} ({client_ip})"
                        client_id = get_client_id(client_ip, client_name)
                        client_status = message.get('status', 'idle')
                        
                        # Update client record under lock
                        client_existed = False
                        with CLIENT_LOCK:
                            client = CLIENTS.get(client_id)
                            if client:
                                client_existed = True
                                client.last_ping = datetime.now()
                                client.status = client_status
                        
                        if client_existed:
                            # Send confirmation
                            response = {'status': 'ok'}
                            client_socket.sendall(json.dumps(response).encode('utf-8'))
                            # logger.debug(f"Heartbeat from {client_name_for_log}, status: {client_status}")
                        else:
                            # Client not registered
                            response = {'status': 'error', 'message': 'Client not registered'}
                            client_socket.sendall(json.dumps(response).encode('utf-8'))
                            logger.warning(f"Heartbeat from unregistered client {client_name_for_log}")
                        
                        close_socket_after_message = False  # Keep connection open
                    
                    elif message_type == 'job_request':
                        # Client requesting a job
                        client_name = message.get('name', 'unknown')
                        client_name_for_log = f"{client_name} ({client_ip})"
                        client_id = get_client_id(client_ip, client_name)
                        
                        logger.info(f"Job request from {client_name_for_log}")
                        
                        # Check if client is registered and not busy
                        client_ok = False
                        job_id_assigned = None
                        with CLIENT_LOCK:
                            client = CLIENTS.get(client_id)
                            if client:
                                # Update ping time, but check status
                                client.last_ping = datetime.now()
                                if client.status == "idle":
                                    client_ok = True
                                else:
                                    logger.warning(f"Client {client_name_for_log} requested job but status is {client.status}")
                            else:
                                logger.warning(f"Job request from unregistered client {client_name_for_log}")
                        
                        if not client_ok:
                            # Send error response
                            response = {'status': 'busy', 'message': 'Client is not registered or not idle'}
                            client_socket.sendall(json.dumps(response).encode('utf-8'))
                            close_socket_after_message = True  # Close after error
                            break
                        
                        # Find a job with unassigned frames for this client
                        job_assigned = False
                        frames_assigned = []
                        blend_filename = None
                        blend_data = None
                        file_size = 0
                        output_format = "PNG"
                        
                        with CLIENT_LOCK:
                            # First, try to find an active job with unassigned frames
                            active_job_ids = []
                            for job_id, job in ACTIVE_JOBS.items():
                                if job.get("status") == "active" and job.get("unassigned_frames"):
                                    active_job_ids.append(job_id)
                            
                            if active_job_ids:
                                # Sort by job ID (which includes timestamp) to prioritize oldest
                                active_job_ids.sort()
                                job_id_assigned = active_job_ids[0]
                                
                                # Get the job and assign frames
                                job = ACTIVE_JOBS.get(job_id_assigned)
                                
                                # Determine frame chunk size (default to 3 if not specified)
                                frame_chunk_size = job.get("frame_chunk_size", 3)
                                
                                # Assign up to chunk_size frames
                                frames_to_assign = job.get("unassigned_frames", [])[0:frame_chunk_size]
                                if frames_to_assign:
                                    # Remove from unassigned list
                                    for frame in frames_to_assign:
                                        job["unassigned_frames"].remove(frame)
                                    
                                    # Add to client's assigned frames
                                    job.setdefault("assigned_clients", {})[client_id] = frames_to_assign
                                    frames_assigned = frames_to_assign
                                    blend_filename = job.get("blend_filename", "job.blend")
                                    blend_data = job.get("blend_data")
                                    file_size = len(blend_data) if blend_data else 0
                                    output_format = job.get("output_format", "PNG")
                                    
                                    # Update client record
                                    client = CLIENTS.get(client_id)
                                    if client:
                                        client.status = "assigned"
                                        client.current_job_id = job_id_assigned
                                        client.current_frames = frames_assigned
                                    
                                    job_assigned = True
                                    logger.info(f"Assigned job {job_id_assigned} frames {frames_assigned} to {client_name_for_log}")
                            
                            # If we couldn't find an active job with frames, but there's something in the queue,
                            # activate the next queued job
                            elif not JOB_QUEUE.empty() and not job_assigned:
                                try:
                                    next_job_id = JOB_QUEUE.get_nowait()
                                    if next_job_id in ACTIVE_JOBS:
                                        job = ACTIVE_JOBS[next_job_id]
                                        # Set job to active
                                        job["status"] = "active"
                                        logger.info(f"Activating queued job {next_job_id}")
                                        # We don't assign it in this round, the client will request again
                                    else:
                                        logger.warning(f"Queued job {next_job_id} not found in ACTIVE_JOBS")
                                except queue.Empty:
                                    pass  # Queue was empty (unlikely given our check, but possible due to threading)
                        
                        if job_assigned:
                            # Send job assignment to client
                            response = {
                                'status': 'ok',
                                'job_id': job_id_assigned,
                                'frames': frames_assigned,
                                'blend_file_name': blend_filename,
                                'output_format': output_format,
                                'file_size': file_size,
                                'camera': job.get('camera', 'Camera'),  # Include camera information
                            }
                            client_socket.sendall(json.dumps(response).encode('utf-8'))
                            
                            # Client should send "READY" to receive blend file
                            ready_received = False
                            try:
                                client_socket.settimeout(30.0)
                                ready_signal = client_socket.recv(1024)
                                if ready_signal == b"READY":
                                    ready_received = True
                                else:
                                    logger.warning(f"Expected READY signal, got: {ready_signal}")
                            except Exception as e:
                                logger.error(f"Error receiving READY signal: {e}")
                            
                            if ready_received and blend_data:
                                # Send blend file data
                                try:
                                    logger.info(f"Sending {file_size} bytes of blend data to {client_name_for_log}")
                                    client_socket.sendall(blend_data)
                                    # Keep connection open for future requests
                                    close_socket_after_message = False
                                except Exception as e:
                                    logger.error(f"Error sending blend data: {e}")
                                    close_socket_after_message = True
                            else:
                                logger.error(f"Did not receive READY signal or no blend data for job {job_id_assigned}")
                                close_socket_after_message = True
                        else:
                            # No jobs available
                            response = {'status': 'no_work', 'message': 'No jobs available'}
                            client_socket.sendall(json.dumps(response).encode('utf-8'))
                            close_socket_after_message = False  # Keep connection open
                    
                    elif message_type == 'frame_complete':
                        # Client reporting a completed frame
                        client_name = "unknown"  # We'll extract from the client record
                        job_id = message.get('job_id')
                        frame = message.get('frame')
                        file_name = message.get('file_name')
                        file_size = message.get('file_size', 0)
                        
                        # Detailed logging
                        logger.info(f"Received frame_complete message from {client_ip}:{client_port} for job {job_id}, frame {frame}, size {file_size}")
                        
                        # Lookup client using job and reconnect info
                        with CLIENT_LOCK:
                            # First try exact client lookup
                            if client_id and client_id in CLIENTS:
                                client = CLIENTS[client_id]
                                client_name = client.name
                                client.last_ping = datetime.now()  # Update ping time
                            else:
                                # Try to find by IP and job ID
                                for cid, client in CLIENTS.items():
                                    if client.ip == client_ip and client.current_job_id == job_id:
                                        client_id = cid
                                        client_name = client.name
                                        client.last_ping = datetime.now()  # Update ping time
                                        break
                                
                                # If still not found, create a new temporary client record
                                if client_id is None:
                                    temp_name = f"TempClient-{client_ip}"
                                    client_id = get_client_id(client_ip, temp_name)
                                    logger.warning(f"Creating temporary client {client_id} for frame receipt")
                                    CLIENTS[client_id] = RenderClient(
                                        client_id=client_id,
                                        ip=client_ip,
                                        name=temp_name,
                                        status="rendering"
                                    )
                                    client_name = temp_name
                        
                        client_name_for_log = f"{client_name} ({client_ip})"
                        logger.info(f"Frame complete notification from {client_name_for_log}: job {job_id}, frame {frame}")
                        
                        # Create output directory regardless of job validation to avoid errors
                        job_output_dir = os.path.join(OUTPUT_DIR, job_id)
                        try:
                            os.makedirs(job_output_dir, exist_ok=True)
                            output_path = os.path.join(job_output_dir, file_name)
                        except Exception as e:
                            logger.error(f"Error creating output directory {job_output_dir}: {e}")
                            output_path = os.path.join(tempfile.gettempdir(), file_name)
                            logger.warning(f"Using fallback output path: {output_path}")
                        
                        # Check if job exists and is active
                        job_valid = False
                        frame_valid = False
                        
                        with CLIENT_LOCK:
                            job = ACTIVE_JOBS.get(job_id)
                            if job:
                                if job.get("status") == "active":
                                    job_valid = True
                                    
                                    # More permissive frame validation - accept even if not in assigned frames
                                    # This helps recover from network issues where a client might be
                                    # reconnecting but still has the frame
                                    client_assigned_frames = job.get("assigned_clients", {}).get(client_id, [])
                                    if frame in client_assigned_frames:
                                        frame_valid = True
                                    elif frame in job.get("all_frames", []) and frame not in job.get("completed_frames", []):
                                        # Frame is valid for the job but not assigned - accept it anyway
                                        logger.warning(f"Accepting frame {frame} from {client_name_for_log} even though not assigned")
                                        frame_valid = True
                                else:
                                    logger.warning(f"Frame complete for inactive job {job_id} (status: {job.get('status')}) from {client_name_for_log}")
                            else:
                                logger.warning(f"Frame complete for unknown job {job_id} from {client_name_for_log}")
                        
                        # Always accept frames to prevent client errors, even if job isn't valid
                        # This way the client can move on to the next frame without getting stuck
                        logger.info(f"Sending READY signal for frame {frame} (job_valid={job_valid}, frame_valid={frame_valid})")
                        try:
                            client_socket.sendall(b"READY")
                            logger.debug("READY signal sent successfully")
                        except Exception as e:
                            logger.error(f"Error sending READY signal: {e}")
                            # Skip the rest of this message processing
                            continue
                        
                        # Receive the frame file
                        frame_recv_success = False
                        try:
                            received_size = 0
                            with open(output_path, 'wb') as f:
                                client_socket.settimeout(120.0)  # Longer timeout for file transfer (2 minutes)
                                
                                # Log start of file transfer
                                logger.info(f"Starting to receive frame {frame} ({file_size} bytes)")
                                transfer_start_time = time.time()
                                
                                while received_size < file_size:
                                    # Try to receive with error handling
                                    try:
                                        chunk = client_socket.recv(65536)  # 64KB chunks
                                        if not chunk:
                                            logger.error("Connection closed during frame transfer - empty chunk received")
                                            raise ConnectionError("Connection closed during file transfer - empty chunk")
                                        chunk_size = len(chunk)
                                        f.write(chunk)
                                        received_size += chunk_size
                                        logger.debug(f"Received chunk: {chunk_size} bytes, total: {received_size}/{file_size}")
                                    except socket.timeout:
                                        logger.error(f"Timeout receiving frame chunk after {received_size}/{file_size} bytes")
                                        raise
                                    
                                    # Occasional progress log for large files
                                    if file_size > 1000000 and received_size % 1000000 < 65536:  # Log roughly every MB
                                        logger.debug(f"Received {received_size}/{file_size} bytes ({received_size/file_size*100:.1f}%)")
                                
                                client_socket.settimeout(None)  # Reset timeout
                                transfer_time = time.time() - transfer_start_time
                            
                            if received_size == file_size:
                                logger.info(f"Received frame {frame} for job {job_id} ({file_size} bytes) in {transfer_time:.2f}s")
                                frame_recv_success = True
                            else:
                                logger.warning(f"Incomplete frame data received: {received_size}/{file_size} bytes")
                                # Remove incomplete file
                                try:
                                    os.remove(output_path)
                                except Exception as e_rm:
                                    logger.error(f"Error removing incomplete file: {e_rm}")
                        except Exception as e:
                            logger.error(f"Error receiving frame file: {e}")
                            # Remove incomplete file if it exists
                            try:
                                if os.path.exists(output_path):
                                    os.remove(output_path)
                            except Exception as e_rm:
                                logger.error(f"Error removing incomplete file: {e_rm}")
                        
                        # If frame was received successfully, update job status
                        if frame_recv_success:
                            try:
                                logger.info(f"Sending acknowledgment for frame {frame}")
                                response = {'status': 'ok', 'message': f"Frame {frame} received"}
                                client_socket.sendall(json.dumps(response).encode('utf-8'))
                                logger.debug("Frame acknowledgment sent successfully")
                            except Exception as e:
                                logger.error(f"Error sending frame acknowledgment: {e}")
                                # Continue processing anyway since we have the frame
                            
                            # Update job state
                            with CLIENT_LOCK:
                                job = ACTIVE_JOBS.get(job_id)
                                if job and job.get("status") == "active":
                                    if frame not in job.get("completed_frames", []):
                                        job.setdefault("completed_frames", []).append(frame)
                                        logger.info(f"Job '{job_id}' progress: {len(job['completed_frames'])} / {len(job.get('all_frames', []))} frames completed.")
                                        
                                        # Check if job is now complete
                                        if len(job["completed_frames"]) >= len(job.get("all_frames", [])):
                                            logger.info(f"Job '{job_id}' completed!")
                                            job["end_time"] = datetime.now()
                                            job["status"] = "completed"
                                            
                                            # Set MP4 info
                                            job["mp4_output_dir"] = os.path.dirname(output_path)
                                            
                                            # Update the JSON file with completed status
                                            update_job_json(job_id)
                                            
                                            # Clean up blend data to free memory
                                            job.pop("blend_data", None)
                                            
                                            # Start MP4 creation in a separate thread
                                            mp4_thread = threading.Thread(
                                                target=create_mp4_for_job,
                                                args=(job_id, os.path.dirname(output_path)),
                                                daemon=True
                                            )
                                            mp4_thread.start()
                                            logger.info(f"Started MP4 creation thread for job {job_id}")
                                
                                # Update client status
                                client = CLIENTS.get(client_id)
                                if client and client.current_job_id == job_id:
                                    # Remove frame from client's current frames
                                    if frame in client.current_frames:
                                        client.current_frames.remove(frame)
                                    
                                    # If client has no more frames, set status to idle
                                    if not client.current_frames:
                                        client.status = "idle"
                                        client.current_job_id = None
                                        logger.info(f"Client {client.name} finished all assigned frames, setting to idle")
                            
                            # Keep the connection open even if there's an error with the acknowledgment
                            close_socket_after_message = False
                        else:
                            # Send error
                            response = {'status': 'error', 'message': 'Failed to receive frame file'}
                            try:
                                client_socket.sendall(json.dumps(response).encode('utf-8'))
                            except Exception:
                                pass  # Ignore errors here, we're closing anyway
                            
                            # Re-queue the frame if it failed
                            with CLIENT_LOCK:
                                job = ACTIVE_JOBS.get(job_id)
                                if (job and job.get("status") == "active" and
                                        frame not in job.get("completed_frames", []) and
                                        frame not in job.get("unassigned_frames", [])):
                                    logger.info(f"Re-queueing frame {frame} for job '{job_id}' after transfer error.")
                                    job.setdefault("unassigned_frames", []).append(frame)
                                
                                # Reset client status
                                client = CLIENTS.get(client_id)
                                if client and client.current_job_id == job_id:
                                    # Remove frame from client's current frames
                                    if frame in client.current_frames:
                                        client.current_frames.remove(frame)
                                    
                                    # If client has no more frames, set status to idle
                                    if not client.current_frames:
                                        client.status = "idle"
                                        client.current_job_id = None
                            
                            close_socket_after_message = True  # Close after error
                    
                    else:
                        # Unknown message type
                        logger.warning(f"Unknown message type from {client_name_for_log}: {message_type}")
                        response = {'status': 'error', 'message': 'Unknown message type'}
                        client_socket.sendall(json.dumps(response).encode('utf-8'))
                        close_socket_after_message = True  # Close after error
                
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from {client_name_for_log}: {e}")
                    close_socket_after_message = True  # Close after error
                
                # Break the loop if we're closing the socket
                if close_socket_after_message:
                    break
            
            except socket.timeout:
                logger.warning(f"Timeout reading from client {client_name_for_log}")
                break
            
            except Exception as e:
                logger.error(f"Error handling client {client_name_for_log}: {e}")
                traceback.print_exc()
                break
        
        # End of main client loop - cleanup
        logger.info(f"Client handler ending for {client_name_for_log}")
    
    finally:
        # Clean up client record and release any assigned frames
        if client_id:
            with CLIENT_LOCK:
                client = CLIENTS.get(client_id)
                if client and client.current_job_id and (client.status == "rendering" or client.status == "assigned"):
                    job_id = client.current_job_id
                    job = ACTIVE_JOBS.get(job_id)
                    if job and job.get("status") == "active":
                        # Get frames assigned to this client
                        assigned_frames = job.get("assigned_clients", {}).get(client_id, [])
                        if assigned_frames:
                            completed_frames = set(job.get("completed_frames", []))
                            # Re-queue any frames not completed
                            frames_to_reassign = [f for f in assigned_frames if f not in completed_frames]
                            if frames_to_reassign:
                                logger.info(f"Re-queueing frames {frames_to_reassign} from client {client.name} for job '{job_id}'")
                                for frame in frames_to_reassign:
                                    if frame not in job.get("unassigned_frames", []):
                                        job.setdefault("unassigned_frames", []).append(frame)
                            
                            # Remove client from job assignment list
                            job["assigned_clients"].pop(client_id, None)
        
        # Close socket
        try:
            client_socket.close()
        except Exception:
            pass


def server_loop(server_socket: socket.socket):
    """Main server loop that accepts incoming connections."""
    logger.info("Server loop started")
    
    next_timeout_check = time.monotonic() + CLIENT_TIMEOUT_SECONDS / 2.0
    next_client_count_check = time.monotonic() + 30.0  # Log client count every 30 seconds
    timeout_delta = timedelta(seconds=CLIENT_TIMEOUT_SECONDS)
    
    # Create a test client for debugging
    with CLIENT_LOCK:
        test_client_id = "test-localhost"
        CLIENTS[test_client_id] = RenderClient(
            client_id=test_client_id,
            ip="127.0.0.1",
            name="TestClient",
            status="idle"
        )
        logger.info(f"Added test client: {test_client_id}")
    
    running = True
    while running:
        # Check for timeouts periodically
        current_time = time.monotonic()
        if current_time >= next_timeout_check:
            check_timeouts(timeout_delta)
            next_timeout_check = current_time + CLIENT_TIMEOUT_SECONDS / 2.0
        
        # Periodic client count logging
        if current_time >= next_client_count_check:
            with CLIENT_LOCK:
                logger.info(f"PERIODIC CHECK - Current client count: {len(CLIENTS)}")
                for cid, client in CLIENTS.items():
                    logger.info(f"  Client: {cid} - {client.name} - Status: {client.status}")
            next_client_count_check = current_time + 30.0
        
        # Update job progress
        update_job_progress()
        
        try:
            # Wait for connection with timeout
            client_socket, client_address = server_socket.accept()
            logger.info(f"New connection accepted from {client_address[0]}:{client_address[1]}")
            
            # Handle client in a separate thread
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True
            )
            client_thread.start()
        
        except socket.timeout:
            # This is expected due to our timeout on accept - allows clean shutdown
            pass
        
        except KeyboardInterrupt:
            logger.info("Server loop interrupted by user")
            running = False
        
        except Exception as e:
            logger.error(f"Error in server loop: {e}")
            traceback.print_exc()
            # Continue running despite errors


def create_job(blend_file_path: str, frame_start: int, frame_end: int, frame_step: int = 1,
               project_name: str = None, frame_chunk_size: int = 3,
               queue_immediately: bool = True, camera: str = "Camera") -> Tuple[str, Dict[str, Any]]:
    """
    Creates a new render job from a blend file.
    
    Args:
        blend_file_path: Path to the blend file
        frame_start: Start frame number
        frame_end: End frame number
        frame_step: Frame step
        project_name: Optional project name (defaults to blend filename)
        frame_chunk_size: Number of frames to assign per client
        queue_immediately: Whether to add the job to the active queue
        
    Returns:
        Tuple of (job_id, job_data)
    """
    # Generate job ID
    job_id = get_next_job_id()
    
    # Extract project name from blend file if not provided
    if not project_name:
        project_name = os.path.splitext(os.path.basename(blend_file_path))[0]
    
    # Read blend file data
    try:
        with open(blend_file_path, 'rb') as f:
            blend_data = f.read()
        
        if not blend_data:
            raise ValueError(f"Empty blend file: {blend_file_path}")
        
        logger.info(f"Read {len(blend_data)} bytes from {blend_file_path}")
    except Exception as e:
        logger.error(f"Error reading blend file: {e}")
        traceback.print_exc()
        return None, None
    
    # Generate list of frames
    all_frames = list(range(frame_start, frame_end + 1, frame_step))
    
    # Create job data structure
    job_data = {
        "id": job_id,
        "all_frames": all_frames,
        "unassigned_frames": all_frames.copy(),  # Copy for consumption
        "assigned_clients": {},  # Will be {client_id: [frames]}
        "completed_frames": [],
        "blend_data": blend_data,
        "blend_filename": os.path.basename(blend_file_path),
        "output_format": "PNG",  # Default format
        "camera": camera,  # Camera to use for rendering
        "start_time": datetime.now(),
        "end_time": None,
        "status": "queued" if not queue_immediately else "active",
        "frame_chunk_size": frame_chunk_size,
        
        # Additional metadata
        "project_name": project_name,
        "frame_range": f"{frame_start}-{frame_end}:{frame_step}",
        "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    
    # Add job to active jobs
    with CLIENT_LOCK:
        ACTIVE_JOBS[job_id] = job_data
    
    # Create output directory
    job_output_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(job_output_dir, exist_ok=True)
    
    # Save job info JSON
    update_job_json(job_id)
    
    # Add to queue if not immediately active
    if not queue_immediately:
        JOB_QUEUE.put(job_id)
        logger.info(f"Job {job_id} added to queue")
    else:
        logger.info(f"Job {job_id} created as active job")
    
    return job_id, job_data


def cancel_job(job_id: str) -> bool:
    """Cancels an active job."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            logger.warning(f"Cannot cancel job {job_id}, not found")
            return False
        
        if job.get("status") != "active" and job.get("status") != "queued":
            logger.warning(f"Cannot cancel job {job_id}, status is {job.get('status')}")
            return False
        
        # Mark job as cancelled
        job["status"] = "cancelled"
        job["end_time"] = datetime.now()
        
        # Clean up blend data to free memory
        job.pop("blend_data", None)
        
        # Add cancel message to job
        job["cancel_reason"] = "Cancelled by user"
        
        # Update the JSON file with cancelled status
        update_job_json(job_id)
        
        logger.info(f"Job {job_id} has been cancelled")
        
        # Remove from queue if it was queued
        # Note: This is not a perfect solution since we can't reliably remove from Queue
        # without analyzing all elements. For a production system, consider a different queue implementation.
        if job.get("status") == "queued":
            try:
                # Create a new queue without this job
                with JOB_QUEUE.mutex:
                    new_queue = queue.Queue()
                    for q_job_id in list(JOB_QUEUE.queue):
                        if q_job_id != job_id:
                            new_queue.put(q_job_id)
                    JOB_QUEUE.queue = new_queue.queue
            except Exception as e:
                logger.error(f"Error removing job from queue: {e}")
        
        return True


def create_mp4_for_job(job_id: str, frames_dir: str, codec='h264', quality='medium'):
    """
    Creates an MP4 video from rendered frames using ffmpeg.
    
    Args:
        job_id: The job ID
        frames_dir: Directory containing the PNG frames
        codec: Video codec to use (h264, hevc, prores)
        quality: Quality setting (high, medium, low)
    """
    try:
        # Get job info
        with CLIENT_LOCK:
            job = ACTIVE_JOBS.get(job_id)
            if not job:
                logger.error(f"Cannot create MP4 for job {job_id}, job not found")
                return
                
            if job.get("mp4_creation_started", False):
                logger.info(f"MP4 creation already started for job {job_id}")
                return
                
            job["mp4_creation_started"] = True
            job["mp4_status"] = "in_progress"
            update_job_json(job_id)
        
        logger.info(f"Starting MP4 creation for job {job_id} in {frames_dir}")
        
        # Ensure output directory exists
        os.makedirs(frames_dir, exist_ok=True)
        
        # Find all rendered PNG frames
        frame_pattern = os.path.join(frames_dir, "frame_*.png")
        frames = sorted(glob.glob(frame_pattern))
        
        if not frames:
            logger.error(f"No frames found in {frames_dir} for MP4 creation")
            with CLIENT_LOCK:
                if job_id in ACTIVE_JOBS:
                    ACTIVE_JOBS[job_id]["mp4_status"] = "failed"
                    ACTIVE_JOBS[job_id]["mp4_error"] = "No frames found"
                    update_job_json(job_id)
            return
        
        # Determine frame rate (default to 24 fps)
        fps = 24
        
        # Set output file path
        output_mp4 = os.path.join(os.path.dirname(frames_dir), f"{job_id}_render.mp4")
        
        # Build ffmpeg command based on codec and quality
        cmd = ['ffmpeg', '-y', '-framerate', str(fps)]
        
        # Input frames pattern
        cmd.extend(['-pattern_type', 'glob', '-i', frame_pattern])
        
        # Set codec-specific options
        if codec == 'hevc':
            # H.265/HEVC settings
            crf = '22' if quality == 'high' else '28' if quality == 'medium' else '34'
            cmd.extend(['-c:v', 'libx265', '-crf', crf, '-pix_fmt', 'yuv420p'])
        elif codec == 'prores':
            # ProRes settings (if supported)
            profile = '3' if quality == 'high' else '2' if quality == 'medium' else '1'
            cmd.extend(['-c:v', 'prores_ks', '-profile:v', profile])
        else:
            # Default to H.264
            crf = '18' if quality == 'high' else '23' if quality == 'medium' else '28'
            cmd.extend(['-c:v', 'libx264', '-crf', crf, '-pix_fmt', 'yuv420p'])
        
        # Output file
        cmd.append(output_mp4)
        
        # Execute ffmpeg command
        logger.info(f"Running ffmpeg command: {' '.join(cmd)}")
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                logger.info(f"Successfully created MP4 video for job {job_id}: {output_mp4}")
                with CLIENT_LOCK:
                    if job_id in ACTIVE_JOBS:
                        ACTIVE_JOBS[job_id]["mp4_status"] = "completed"
                        ACTIVE_JOBS[job_id]["mp4_path"] = output_mp4
                        ACTIVE_JOBS[job_id]["mp4_created"] = True
                        ACTIVE_JOBS[job_id]["mp4_creation_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        update_job_json(job_id)
            else:
                logger.error(f"Error creating MP4 video: {stderr.decode()}")
                # Try a fallback approach
                logger.info("Trying fallback ffmpeg command...")
                fallback_cmd = [
                    'ffmpeg', '-y', '-framerate', str(fps),
                    '-pattern_type', 'glob', '-i', frame_pattern,
                    '-c:v', 'libx264', '-pix_fmt', 'yuv420p', output_mp4
                ]
                fallback_process = subprocess.Popen(fallback_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                fallback_stdout, fallback_stderr = fallback_process.communicate()
                
                if fallback_process.returncode == 0:
                    logger.info(f"Fallback MP4 creation successful: {output_mp4}")
                    with CLIENT_LOCK:
                        if job_id in ACTIVE_JOBS:
                            ACTIVE_JOBS[job_id]["mp4_status"] = "completed"
                            ACTIVE_JOBS[job_id]["mp4_path"] = output_mp4
                            ACTIVE_JOBS[job_id]["mp4_created"] = True
                            ACTIVE_JOBS[job_id]["mp4_creation_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            update_job_json(job_id)
                else:
                    logger.error(f"Fallback MP4 creation failed: {fallback_stderr.decode()}")
                    with CLIENT_LOCK:
                        if job_id in ACTIVE_JOBS:
                            ACTIVE_JOBS[job_id]["mp4_status"] = "failed"
                            ACTIVE_JOBS[job_id]["mp4_error"] = fallback_stderr.decode()[:200]  # Truncate long error messages
                            update_job_json(job_id)
        except Exception as e:
            logger.error(f"Exception running ffmpeg: {e}")
            with CLIENT_LOCK:
                if job_id in ACTIVE_JOBS:
                    ACTIVE_JOBS[job_id]["mp4_status"] = "failed"
                    ACTIVE_JOBS[job_id]["mp4_error"] = str(e)
                    update_job_json(job_id)
    
    except Exception as e:
        logger.error(f"Exception during MP4 creation: {e}")
        traceback.print_exc()
        with CLIENT_LOCK:
            if job_id in ACTIVE_JOBS:
                ACTIVE_JOBS[job_id]["mp4_status"] = "failed"
                ACTIVE_JOBS[job_id]["mp4_error"] = str(e)
                update_job_json(job_id)


def force_complete_job(job_id: str) -> bool:
    """Forces a job to be marked as completed."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            logger.warning(f"Cannot force-complete job {job_id}, not found")
            return False
        
        if job.get("status") != "active":
            logger.warning(f"Cannot force-complete job {job_id}, status is {job.get('status')}")
            return False
        
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
        
        # Set the job's output directory if it exists
        job_output_dir = os.path.join(OUTPUT_DIR, job_id)
        if os.path.exists(job_output_dir):
            job["mp4_output_dir"] = job_output_dir
        
        # Update the JSON file with completed status
        update_job_json(job_id)
        
        logger.info(f"Job {job_id} has been force-completed with {completed_frames}/{total_frames} frames done")
        return True


# --- Web Interface Routes ---
@app.route('/')
def index():
    """Main web interface page."""
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    """Returns server status as JSON."""
    status_data = {
        "server_running": True,
        "server_port": SERVER_PORT,
        "output_dir": OUTPUT_DIR,
        "clients": [],
        "active_jobs": [],
        "queued_jobs": [],
        "completed_jobs": []
    }
    
    with CLIENT_LOCK:
        # Log current client count
        logger.info(f"Status API - Current client count: {len(CLIENTS)}")
        
        # Clients
        for client_id, client in CLIENTS.items():
            try:
                # Convert client object to dict
                client_data = {
                    "id": client_id,
                    "name": client.name,
                    "ip": client.ip,
                    "status": client.status,
                    "last_ping": client.last_ping.strftime("%Y-%m-%d %H:%M:%S"),
                    "current_job_id": client.current_job_id,
                    "current_frames": client.current_frames
                }
                status_data["clients"].append(client_data)
                logger.debug(f"Added client to status: {client_id} - {client.name}")
            except Exception as e:
                logger.error(f"Error processing client {client_id}: {e}")
                # Add a basic client entry to avoid missing data
                status_data["clients"].append({
                    "id": client_id,
                    "name": "Error",
                    "ip": "Unknown",
                    "status": "error",
                    "last_ping": "Unknown",
                    "current_job_id": None,
                    "current_frames": []
                })
        
        # Jobs
        for job_id, job in ACTIVE_JOBS.items():
            job_info = {
                "id": job_id,
                "status": job.get("status", "unknown"),
                "project_name": job.get("project_name", "unknown"),
                "frame_range": job.get("frame_range", ""),
                "total_frames": len(job.get("all_frames", [])),
                "completed_frames": len(job.get("completed_frames", [])),
                "start_time": job.get("start_time").strftime("%Y-%m-%d %H:%M:%S") if isinstance(job.get("start_time"), datetime) else str(job.get("start_time")),
                "mp4_created": job.get("mp4_created", False),
                "mp4_path": job.get("mp4_path", ""),
                "mp4_status": job.get("mp4_status", "not_started")
            }
            
            # Add end time if completed
            if job.get("end_time"):
                job_info["end_time"] = job.get("end_time").strftime("%Y-%m-%d %H:%M:%S") if isinstance(job.get("end_time"), datetime) else str(job.get("end_time"))
            
            # Calculate progress
            total = len(job.get("all_frames", []))
            completed = len(job.get("completed_frames", []))
            job_info["progress"] = (completed / total) * 100.0 if total > 0 else 0.0
            
            # Check if the job is truly active - has client assignments
            is_really_active = False
            if job.get("status") == "active":
                # Check if any clients are assigned to this job
                for client_id, client in CLIENTS.items():
                    if client.current_job_id == job_id and (client.status == "assigned" or client.status == "rendering"):
                        is_really_active = True
                        break
                
                # Also check if job has assigned clients in its own structure
                if job.get("assigned_clients") and len(job.get("assigned_clients")) > 0:
                    is_really_active = True
            
            # Add to appropriate list
            if job.get("status") == "active" and is_really_active:
                # Only treat as active if clients are working on it
                status_data["active_jobs"].append(job_info)
            elif job.get("status") == "active" and not is_really_active:
                # Job is marked active but no clients are working on it - treat as queued
                job_info["display_status"] = "pending"  # Special status for UI display
                status_data["queued_jobs"].append(job_info)
            elif job.get("status") == "pending_restart":
                # Previous jobs that were active when server restarted - treat as queued and need restart
                job_info["display_status"] = "restart"
                status_data["queued_jobs"].append(job_info)
            elif job.get("status") == "queued":
                status_data["queued_jobs"].append(job_info)
            elif job.get("status") in ["completed", "cancelled"]:
                status_data["completed_jobs"].append(job_info)
    
    return jsonify(status_data)


@app.route('/api/upload', methods=['POST'])
def api_upload():
    """Handles blend file uploads."""
    if 'blend_file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    blend_file = request.files['blend_file']
    
    if blend_file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not blend_file.filename.lower().endswith('.blend'):
        return jsonify({"error": "Only .blend files are supported"}), 400
    
    # Get form data
    frame_start = int(request.form.get('frame_start', 1))
    frame_end = int(request.form.get('frame_end', 10))
    frame_step = int(request.form.get('frame_step', 1))
    frame_chunk_size = int(request.form.get('frame_chunk_size', 3))
    project_name = request.form.get('project_name', os.path.splitext(blend_file.filename)[0])
    queue_immediately = request.form.get('queue_immediately', 'false').lower() == 'true'
    camera = request.form.get('camera', 'Camera')  # Get camera name with default "Camera"
    
    # Save the uploaded file temporarily
    filename = secure_filename(blend_file.filename)
    temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    blend_file.save(temp_file_path)
    
    # Create job
    try:
        job_id, job_data = create_job(
            temp_file_path, 
            frame_start, 
            frame_end,
            frame_step,
            project_name,
            frame_chunk_size,
            queue_immediately,
            camera
        )
        
        if not job_id:
            return jsonify({"error": "Failed to create job"}), 500
        
        # Return success response
        return jsonify({
            "success": True, 
            "job_id": job_id,
            "status": job_data.get("status"),
            "frames": len(job_data.get("all_frames", []))
        })
    
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Error creating job: {str(e)}"}), 500
    
    finally:
        # Clean up temporary file (optionally, based on policy)
        try:
            os.remove(temp_file_path)
        except Exception as e:
            logger.warning(f"Failed to remove temporary file: {e}")


@app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
def api_cancel_job(job_id):
    """Cancels a job."""
    result = cancel_job(job_id)
    if result:
        return jsonify({"success": True})
    else:
        return jsonify({"error": f"Failed to cancel job {job_id}"}), 400


@app.route('/api/jobs/<job_id>/force_complete', methods=['POST'])
def api_force_complete_job(job_id):
    """Forces a job to complete."""
    result = force_complete_job(job_id)
    if result:
        return jsonify({"success": True})
    else:
        return jsonify({"error": f"Failed to force-complete job {job_id}"}), 400


@app.route('/api/jobs/<job_id>/activate', methods=['POST'])
def api_activate_job(job_id):
    """Activates a queued job."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            return jsonify({"error": f"Job {job_id} not found"}), 404
        
        if job.get("status") != "queued" and job.get("status") != "pending_restart":
            return jsonify({"error": f"Job {job_id} cannot be activated (status: {job.get('status')})"}), 400
        
        # Update status
        job["status"] = "active"
        
        # If this is a restarted job that has no frames in the unassigned_frames list,
        # repopulate it with frames that haven't been completed
        if not job.get("unassigned_frames") or len(job.get("unassigned_frames", [])) == 0:
            all_frames = job.get("all_frames", [])
            completed_frames = job.get("completed_frames", [])
            
            # Find frames that need to be rendered
            frames_to_render = [f for f in all_frames if f not in completed_frames]
            
            if frames_to_render:
                job["unassigned_frames"] = frames_to_render
                logger.info(f"Job {job_id} restarted with {len(frames_to_render)} frames to render")
            else:
                # If all frames are already completed, mark as completed
                job["status"] = "completed"
                job["end_time"] = datetime.now()
                logger.info(f"Job {job_id} has no frames to render, marking as completed")
        
        update_job_json(job_id)
        
        logger.info(f"Job {job_id} activated manually")
        return jsonify({"success": True})


@app.route('/api/jobs/<job_id>/create_mp4', methods=['POST'])
def api_create_mp4(job_id):
    """Manually triggers MP4 creation for a job."""
    # Get codec and quality settings from request
    codec = request.json.get('codec', 'h264')
    quality = request.json.get('quality', 'medium')
    
    # Validate codec
    valid_codecs = ['h264', 'hevc', 'prores']
    if codec not in valid_codecs:
        return jsonify({"error": f"Invalid codec. Must be one of: {', '.join(valid_codecs)}"}), 400
    
    # Validate quality
    valid_qualities = ['high', 'medium', 'low']
    if quality not in valid_qualities:
        return jsonify({"error": f"Invalid quality. Must be one of: {', '.join(valid_qualities)}"}), 400
    
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            return jsonify({"error": f"Job {job_id} not found"}), 404
        
        # Can only create MP4 for completed jobs
        if job.get("status") != "completed" and job.get("status") != "cancelled":
            return jsonify({"error": f"Can only create MP4 for completed or cancelled jobs. Current status: {job.get('status')}"}), 400
        
        # Check if MP4 creation is already in progress
        if job.get("mp4_status") == "in_progress":
            return jsonify({"message": f"MP4 creation already in progress for job {job_id}"}), 200
        
        # Check if frames directory exists
        job_output_dir = os.path.join(OUTPUT_DIR, job_id)
        if not os.path.exists(job_output_dir):
            return jsonify({"error": f"Output directory for job {job_id} not found: {job_output_dir}"}), 404
        
        # Set the job's output directory
        job["mp4_output_dir"] = job_output_dir
        
        # Start MP4 creation in a separate thread
        mp4_thread = threading.Thread(
            target=create_mp4_for_job,
            args=(job_id, job_output_dir, codec, quality),
            daemon=True
        )
        mp4_thread.start()
        
        # Update job status
        job["mp4_creation_requested"] = True
        update_job_json(job_id)
        
        logger.info(f"MP4 creation started for job {job_id} with codec {codec}, quality {quality}")
        return jsonify({"success": True, "message": f"MP4 creation started for job {job_id}"})


@app.route('/api/jobs/<job_id>/mp4_status', methods=['GET'])
def api_mp4_status(job_id):
    """Gets the status of MP4 creation for a job."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            return jsonify({"error": f"Job {job_id} not found"}), 404
        
        mp4_status = {
            "status": job.get("mp4_status", "not_started"),
            "error": job.get("mp4_error", None),
            "mp4_path": job.get("mp4_path", None)
        }
        
        return jsonify(mp4_status)


@app.route('/api/jobs/<job_id>/preview', methods=['GET'])
def api_job_preview(job_id):
    """Returns a preview image (first frame) or HTML video preview for a job."""
    # Check if we have an MP4 file first
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if job and job.get("mp4_created") and job.get("mp4_path") and os.path.exists(job.get("mp4_path")):
            # Create a preview HTML page with a video player
            mp4_path = job.get("mp4_path")
            mp4_url = f"/api/jobs/{job_id}/mp4"
            
            # Return an HTML page with a video player (this is more user-friendly than direct MP4)
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>MP4 Preview for Job {job_id}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #222; }}
                    .video-container {{ width: 90%; max-width: 1200px; }}
                    video {{ width: 100%; border-radius: 5px; box-shadow: 0 0 30px rgba(0,0,0,0.7); background-color: #000; }}
                    .info {{ color: white; text-align: center; margin-top: 20px; }}
                    .download-link {{ display: inline-block; background-color: #4caf50; color: white; text-decoration: none; padding: 10px 20px; border-radius: 5px; margin-top: 15px; font-weight: bold; transition: background-color 0.3s; }}
                    .download-link:hover {{ background-color: #45a049; }}
                    h3 {{ margin-bottom: 5px; }}
                </style>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {{
                        const video = document.querySelector('video');
                        
                        // Add error handling for the video
                        video.addEventListener('error', function(e) {{
                            console.error('Video error:', e);
                            document.querySelector('.error-message').style.display = 'block';
                        }});
                        
                        // Show loading message
                        video.addEventListener('loadstart', function() {{
                            document.querySelector('.loading').style.display = 'block';
                        }});
                        
                        // Hide loading when video can play
                        video.addEventListener('canplay', function() {{
                            document.querySelector('.loading').style.display = 'none';
                        }});
                    }});
                </script>
            </head>
            <body>
                <div class="video-container">
                    <video controls autoplay loop>
                        <source src="{mp4_url}" type="video/mp4">
                        Your browser does not support the video tag.
                    </video>
                    <div class="loading" style="display: none; color: white; text-align: center; margin-top: 10px;">Loading video...</div>
                    <div class="error-message" style="display: none; color: #ff5555; text-align: center; margin-top: 10px;">
                        Error loading video. Please try refreshing the page or download the MP4 directly.
                    </div>
                    <div class="info">
                        <h3>Job {job_id}</h3>
                        <p>{job.get("project_name", "Unknown project")} - MP4 created on {job.get("mp4_creation_time", "Unknown date")}</p>
                        <a class="download-link" href="{mp4_url}" download>Download MP4</a>
                    </div>
                </div>
            </body>
            </html>
            """
            return html_content
    
    # If no MP4 found, fall back to PNG preview
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    
    # Check if directory exists
    if not os.path.exists(job_dir):
        return jsonify({"error": f"Job directory not found for {job_id}"}), 404
        
    # Look for PNG files
    frame_files = sorted(glob.glob(os.path.join(job_dir, "frame_*.png")))
    
    if not frame_files:
        return jsonify({"error": "No frame files found"}), 404
    
    # Get the first frame
    first_frame = frame_files[0]
    
    # Serve the file
    return send_from_directory(os.path.dirname(first_frame), os.path.basename(first_frame))


@app.route('/api/jobs/<job_id>/thumbnail', methods=['GET'])
def api_job_thumbnail(job_id):
    """Returns a thumbnail image for the job (always a PNG, even for MP4s)."""
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    
    # Check if directory exists
    if not os.path.exists(job_dir):
        return jsonify({"error": f"Job directory not found for {job_id}"}), 404
        
    # Look for PNG files
    frame_files = sorted(glob.glob(os.path.join(job_dir, "frame_*.png")))
    
    if not frame_files:
        return jsonify({"error": "No frame files found"}), 404
    
    # Get the first frame
    first_frame = frame_files[0]
    
    # Serve the file
    return send_from_directory(os.path.dirname(first_frame), os.path.basename(first_frame))


@app.route('/api/jobs/<job_id>/files', methods=['GET'])
def api_job_files(job_id):
    """Lists files for a job."""
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    
    # Check if directory exists
    if not os.path.exists(job_dir):
        return jsonify({"error": f"Job directory not found for {job_id}"}), 404
    
    # Get a list of files
    file_list = []
    for filename in os.listdir(job_dir):
        file_path = os.path.join(job_dir, filename)
        if os.path.isfile(file_path):
            file_list.append({
                "name": filename,
                "size": os.path.getsize(file_path),
                "modified": datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
            })
    
    # Generate a simple HTML page
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Files for Job {job_id}</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .file-link {{ text-decoration: none; color: #2196f3; }}
        </style>
    </head>
    <body>
        <h1>Files for Job {job_id}</h1>
        <p>Directory: {job_dir}</p>
        <table>
            <tr>
                <th>Filename</th>
                <th>Size</th>
                <th>Modified</th>
            </tr>
    """
    
    for file_info in sorted(file_list, key=lambda x: x["name"]):
        html_content += f"""
            <tr>
                <td><a class="file-link" href="/api/jobs/{job_id}/files/{file_info['name']}" target="_blank">{file_info['name']}</a></td>
                <td>{file_info['size'] // 1024} KB</td>
                <td>{file_info['modified']}</td>
            </tr>
        """
    
    html_content += """
        </table>
    </body>
    </html>
    """
    
    return html_content


@app.route('/api/jobs/<job_id>/files/<path:filename>', methods=['GET'])
def api_job_file_download(job_id, filename):
    """Downloads a specific file from a job."""
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    
    # Check if directory exists
    if not os.path.exists(job_dir):
        return jsonify({"error": f"Job directory not found for {job_id}"}), 404
    
    return send_from_directory(job_dir, filename)


@app.route('/api/jobs/<job_id>/mp4', methods=['GET'])
def api_job_mp4_download(job_id):
    """Downloads the MP4 file for a job."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            return jsonify({"error": f"Job {job_id} not found"}), 404
            
        mp4_path = job.get("mp4_path")
        logger.info(f"Requested MP4 for job {job_id}, path: {mp4_path}")
        
        if not mp4_path or not os.path.exists(mp4_path):
            logger.error(f"MP4 file not found for job {job_id}. Path: {mp4_path}, Exists: {os.path.exists(mp4_path) if mp4_path else False}")
            return jsonify({"error": "MP4 file not found"}), 404
        
        # Serve the file with correct MIME type
        response = send_from_directory(os.path.dirname(mp4_path), os.path.basename(mp4_path))
        response.headers['Content-Type'] = 'video/mp4'
        response.headers['Content-Disposition'] = f'inline; filename="{job_id}_render.mp4"'
        
        logger.info(f"Serving MP4 file: {mp4_path}")
        return response


@app.route('/api/jobs/<job_id>/delete', methods=['POST'])
def api_delete_job(job_id):
    """Deletes a job and its files."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            return jsonify({"error": f"Job {job_id} not found"}), 404
        
        if job.get("status") != "completed" and job.get("status") != "cancelled":
            return jsonify({"error": f"Can only delete completed or cancelled jobs. Current status: {job.get('status')}"}), 400
        
        # Delete the job directory
        job_dir = os.path.join(OUTPUT_DIR, job_id)
        if os.path.exists(job_dir):
            try:
                shutil.rmtree(job_dir)
                logger.info(f"Deleted job directory: {job_dir}")
            except Exception as e:
                logger.error(f"Error deleting job directory {job_dir}: {e}")
                return jsonify({"error": f"Error deleting job files: {str(e)}"}), 500
        
        # Delete the job JSON file
        json_path = os.path.join(OUTPUT_DIR, f"{job_id}_info.json")
        if os.path.exists(json_path):
            try:
                os.remove(json_path)
                logger.info(f"Deleted job JSON file: {json_path}")
            except Exception as e:
                logger.error(f"Error deleting job JSON file {json_path}: {e}")
                return jsonify({"error": f"Error deleting job JSON: {str(e)}"}), 500
        
        # Remove from active jobs
        ACTIVE_JOBS.pop(job_id, None)
        logger.info(f"Removed job {job_id} from active jobs")
        
        return jsonify({"success": True, "message": f"Job {job_id} deleted successfully"})


@app.route('/static/<path:path>')
def serve_static(path):
    """Serves static files."""
    return send_from_directory('static', path)


# --- Templates Directory ---
@app.route('/templates/<path:path>')
def serve_template(path):
    """Serves template files."""
    return send_from_directory('templates', path)


# --- Main Function ---
def load_previous_jobs():
    """Load previous job data from JSON files in the output directory."""
    logger.info(f"Loading previous jobs from {OUTPUT_DIR}")
    job_files = glob.glob(os.path.join(OUTPUT_DIR, "*_info.json"))
    loaded_count = 0
    pending_restart_jobs = []
    
    for job_file in job_files:
        try:
            # Extract job ID from filename (e.g., "job_001_1620000000_info.json" -> "job_001_1620000000")
            job_id = os.path.basename(job_file).replace("_info.json", "")
            
            # Skip if already in active jobs
            if job_id in ACTIVE_JOBS:
                logger.debug(f"Job {job_id} already loaded, skipping")
                continue
                
            # Load JSON data
            with open(job_file, 'r') as f:
                job_data = json.load(f)
                
            # Add missing fields that wouldn't be in the JSON
            job_data["blend_data"] = None  # Don't load the binary data
            
            # Convert string dates back to datetime objects
            if "start_time" in job_data and isinstance(job_data["start_time"], str):
                try:
                    job_data["start_time"] = datetime.strptime(job_data["start_time"], "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    logger.warning(f"Error parsing start_time for job {job_id}: {e}")
                    
            if "end_time" in job_data and isinstance(job_data["end_time"], str):
                try:
                    job_data["end_time"] = datetime.strptime(job_data["end_time"], "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    logger.warning(f"Error parsing end_time for job {job_id}: {e}")
            
            # Add to active jobs
            with CLIENT_LOCK:
                ACTIVE_JOBS[job_id] = job_data
                loaded_count += 1
                logger.info(f"Loaded previous job: {job_id} (status: {job_data.get('status', 'unknown')})")
                
                # If job is in "active" state but was from a previous session, mark as pending
                if job_data.get("status") == "active":
                    job_data["status"] = "pending_restart"
                    update_job_json(job_id)
                    
                    # Add to list of pending restart jobs
                    pending_restart_jobs.append(job_id)
                
        except Exception as e:
            logger.error(f"Error loading job from {job_file}: {e}")
            traceback.print_exc()
    
    logger.info(f"Loaded {loaded_count} previous jobs from {len(job_files)} JSON files")
    
    # Auto-activate the first pending restart job if any exist
    if pending_restart_jobs:
        # Sort by job ID to get the oldest one first (based on timestamp in ID)
        pending_restart_jobs.sort()
        auto_activate_job = pending_restart_jobs[0]
        
        logger.info(f"Auto-activating pending restart job: {auto_activate_job}")
        try:
            activate_pending_job(auto_activate_job)
        except Exception as e:
            logger.error(f"Error auto-activating job {auto_activate_job}: {e}")
            traceback.print_exc()


def activate_pending_job(job_id: str) -> bool:
    """Activates a pending restart job automatically."""
    with CLIENT_LOCK:
        job = ACTIVE_JOBS.get(job_id)
        if not job:
            logger.warning(f"Cannot activate job {job_id}, not found")
            return False
        
        if job.get("status") != "pending_restart":
            logger.warning(f"Cannot auto-activate job {job_id}, status is {job.get('status')}")
            return False
        
        # Update status
        job["status"] = "active"
        
        # If this is a restarted job that has no frames in the unassigned_frames list,
        # repopulate it with frames that haven't been completed
        if not job.get("unassigned_frames") or len(job.get("unassigned_frames", [])) == 0:
            all_frames = job.get("all_frames", [])
            completed_frames = job.get("completed_frames", [])
            
            # Find frames that need to be rendered
            frames_to_render = [f for f in all_frames if f not in completed_frames]
            
            if frames_to_render:
                job["unassigned_frames"] = frames_to_render
                logger.info(f"Job {job_id} auto-restarted with {len(frames_to_render)} frames to render")
            else:
                # If all frames are already completed, mark as completed
                job["status"] = "completed"
                job["end_time"] = datetime.now()
                logger.info(f"Job {job_id} has no frames to render, marking as completed")
        
        update_job_json(job_id)
        
        logger.info(f"Job {job_id} activated automatically")
        return True


def main():
    """Main application entry point."""
    global SERVER_PORT, WEB_PORT, OUTPUT_DIR
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Standalone Blender Render Farm Server')
    parser.add_argument('--port', type=int, default=SERVER_PORT, help='Port for the render server')
    parser.add_argument('--web-port', type=int, default=WEB_PORT, help='Port for the web interface')
    parser.add_argument('--output-dir', type=str, default=OUTPUT_DIR, help='Directory for render output')
    args = parser.parse_args()
    
    SERVER_PORT = args.port
    WEB_PORT = args.web_port
    OUTPUT_DIR = args.output_dir
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Create static and templates directories if they don't exist
    os.makedirs('static', exist_ok=True)
    os.makedirs('templates', exist_ok=True)
    
    # Load previous jobs from JSON files
    load_previous_jobs()
    
    # Create basic HTML template for the web interface
    # create_templates()
    
    # Start the server socket
    server_socket = start_server_socket(SERVER_PORT)
    if not server_socket:
        logger.error("Failed to start server socket")
        return 1
    
    # Start server loop in a separate thread
    server_thread = threading.Thread(
        target=server_loop,
        args=(server_socket,),
        daemon=True
    )
    server_thread.start()
    
    # Start the web server
    logger.info(f"Starting web interface on port {WEB_PORT}")
    app.run(host='0.0.0.0', port=WEB_PORT, debug=False, threaded=True)
    
    # When the Flask app exits, clean up
    logger.info("Shutting down server...")
    
    # Close server socket
    try:
        server_socket.close()
    except Exception as e:
        logger.error(f"Error closing server socket: {e}")
    
    # Wait for server thread to exit
    try:
        server_thread.join(timeout=3.0)
    except Exception as e:
        logger.error(f"Error joining server thread: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
