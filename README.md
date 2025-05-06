# MicroFarm: Blender Render Farm

A lightweight distributed rendering system for Blender, featuring a server plugin and client script to distribute rendering tasks across multiple computers.

## Overview

This project provides a simple yet effective way to distribute Blender rendering tasks across multiple machines.

### Blender Plugin 

You can use the Blender Plugin to run a server and Client Script:

- **Server Plugin (`blender-render-farm-plugin.py`)**: A Blender addon that turns a Blender instance into a render server
- **Client Script (`blender-client-script.py`)**: A Python script that connects to the server and renders assigned frames

### Python Server

Using a Stand Alone python server (with web interface) and run the client again

- **Standalone Server (`standalone_server.py`)**: A Python-based server with web interface that doesn't require Blender 
- **Client Script (`blender-client-script.py`)**: A Python script that connects to the server and renders assigned frames


The system is designed to be:
- Easy to set up and use
- Works with Blender 4.x
- Handles frame distribution automatically
- Creates MP4 videos from rendered frames
- Provides job monitoring and management through the Blender UI or web interface

## Features

- **Blend File Handling**: Efficient transfer of blend data
- **Automatic Frame Distribution**: Distributes frames evenly across clients
- **Job Management**: Create, monitor, cancel, and complete jobs
- **Client Auto-Discovery**: Clients automatically connect to the server
- **Fault Tolerance**: Automatically reassigns frames from disconnected clients
- **MP4 Creation**: Automatically creates videos from rendered frames
- **Flexible Output Organization**: Configurable directory structure for frames and videos
- **Web Interface**: Modern web UI for job management (standalone server only)
- **Job Persistence**: Jobs persist across server restarts (standalone server only)
- **File Browser**: Browse and download rendered files and videos (standalone server only)

## Installation

### Blender Server Setup

1. Open Blender (4.0+)
2. Go to Edit > Preferences > Add-ons > Install
3. Select the `blender-render-farm-plugin.py` file and click 'Install Add-on'
4. Enable the add-on by checking the box next to "Render: Minimal Render Server (In-Memory)"
5. Configure the addon settings:
   - Set the server port (default: 9090)
   - Set the output directory for rendered frames

### Standalone Server Setup

The standalone server requires Python 3.7+ with the following packages:
- Flask
- Werkzeug
- FFmpeg (for MP4 creation)

#### Installation:

1. Install Python 3.7+ if not already installed
2. Install required packages:
   ```bash
   pip install flask werkzeug
   ```
3. Install FFmpeg for your platform if not already installed

#### Running the Server:

```bash
python standalone_server.py --port 9090 --web-port 8080 --output-dir /path/to/output
```

Options:
- `--port`: Socket server port for client connections (default: 9090)
- `--web-port`: Web interface port (default: 8080)
- `--output-dir`: Directory for rendered frames and job data (default: ./render_output)

### Client Setup

The client requires Blender and Python 3.7+ to run.

#### Basic Client Usage:

```bash
blender --background --python blender-client-script.py -- --server SERVER_IP --port 9090 --name CLIENT_NAME
```

#### Windows Example:

```batch
"C:\Program Files\Blender Foundation\Blender 4.0\blender.exe" --background --python blender-client-script.py -- --server 192.168.1.100 --port 9090 --name WindowsClient1
```

#### macOS Example:

```bash
/Applications/Blender.app/Contents/MacOS/Blender --background --python blender-client-script.py -- --server 192.168.1.100 --port 9090 --name MacClient1
```

#### Linux Example:

```bash
blender --background --python blender-client-script.py -- --server 192.168.1.100 --port 9090 --name LinuxClient1
```

## Usage

### Blender Server UI

1. Open Blender on the computer that will act as the render server
2. Go to the "Render Properties" tab
3. Find the "Render Server (In-Memory)" panel
4. Click "Start Server" to begin listening for clients
5. Configure job settings:
   - Set "Frames Per Chunk" (how many frames to assign to a client at once)
   - Enable/disable "Pack Textures" 
   - Configure MP4 video creation settings

### Standalone Server Web Interface

1. Start the standalone server as described in the setup section
2. Open a web browser and navigate to `http://localhost:8080` (or whatever port you configured)
3. The web interface has the following sections:
   - **Upload Blend File**: Upload .blend files and configure rendering options
   - **Active Jobs**: Shows currently rendering jobs with progress information
   - **Queued Jobs**: Jobs waiting to be processed or paused jobs
   - **Completed Jobs**: Finished jobs with preview images/videos and download options
   - **Connected Clients**: Shows all connected render clients

### Creating a Render Job

#### Using Blender Server:
1. Open your Blender file with the animation you want to render
2. Set up your render settings (resolution, samples, etc.) as you normally would
3. In the "Render Server" panel, click "Create New Render Job"
4. The job will be distributed to any connected clients

#### Using Standalone Server:
1. In the web interface, use the "Upload Blend File" section
2. Configure job settings (frame range, frames per chunk, etc.)
3. Click "Upload and Create Job"
4. The job will be queued or distributed to available clients

### Managing Jobs

#### Blender Server:
- **Monitor Progress**: The server UI shows job progress, including frame count and percentage
- **Cancel Job**: Click "Cancel Job" to stop an active job
- **Force Complete**: Click "Force Complete" to mark a stalled job as complete
- **Clear Completed Job**: Remove completed job data from memory

#### Standalone Server:
- **Monitor Progress**: The web UI displays real-time progress bars and frame counts
- **Cancel Job**: Click "Cancel" to stop an active or queued job
- **Force Complete**: Click "Force Complete" to mark a job as finished
- **Activate Job**: Start or restart a queued/pending job
- **Delete Job**: Remove completed jobs and their files
- **Create MP4**: Convert rendered frames to an MP4 video with selectable codec and quality
- **View Files**: Browse all files associated with a job

### MP4 Video Creation

When a job completes, an MP4 video is automatically created from the rendered frames. You can configure:

- **Video Codec**: H.264, H.265/HEVC, or ProRes
- **Video Quality**: High, Medium, or Low
- **MP4 Location**: Parent Directory, Frames Directory, or Root Output Directory

## Directory Structure

Both server types organize rendered files in a similar structure:

```
output_directory/
├── job_001_1234567890/                # Job-specific directory
│   ├── frame_0001_.png                # Individual frame files
│   ├── frame_0002_.png
│   ├── ...
│   └── job_001_1234567890_render.mp4  # Optional: MP4 in frames directory
│
├── job_001_1234567890_render.mp4      # Optional: MP4 in parent directory
├── job_001_1234567890_info.json       # Standalone server: Job metadata (status, progress, etc.)
└── ...
```

Client-side files use this structure:

```
/tmp/blender_farm/                     # Base directory 
├── client_name/                       # Per-client directory
│   └── project_name/                  # Project-specific directory
│       └── job_id/                    # Job-specific directory
│           ├── blend_file.blend       # Job blend file
│           └── temp files             # Rendering temporary files
```

## Requirements

- **Blender Server**: Blender 4.0 or higher
- **Standalone Server**: Python 3.7+, Flask, Werkzeug
- **Client**: Blender 4.0 or higher + Python 3.7+
- **Network**: Clients must be able to connect to the server's IP address and port
- **MP4 Creation**: FFmpeg installed on the server machine (for video creation)
- **Web Interface**: Modern web browser (Chrome, Firefox, Safari, Edge)

## Troubleshooting

### Common Issues

- **Connection Issues**: Make sure the server IP is correct and the port is open in firewalls
- **Stalled Jobs**: Use the "Force Complete" button if jobs get stuck
- **MP4 Creation Fails**: Verify FFmpeg is installed on the server machine
- **Client Disconnects**: Check network stability and increase timeout settings if needed

### Logs

- Blender server logs are visible in the Blender console
- Standalone server logs are displayed in the terminal where the server is running
- Client logs are printed to the console/terminal where the client is running

## License

APACHE 2.0 License - See LICENSE file for details.

## Acknowledgments
- Generated using AI tools such as Claude Code and Gemini 2.5
- Built for Blender 4.x
- Uses FFmpeg for video encoding
- Uses Flask for the standalone server's web interface
- Thanks to everyone in the Blender community

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
