# MicroFram: Blender Render Farm

A lightweight distributed rendering system for Blender, featuring a server plugin and client script to distribute rendering tasks across multiple computers.

## Overview

This project provides a simple yet effective way to distribute Blender rendering tasks across multiple machines. It consists of:

1. **Server Plugin (`blender-render-farm-plugin.py`)**: A Blender addon that turns a Blender instance into a render server
2. **Client Script (`blender-client-script.py`)**: A Python script that connects to the server and renders assigned frames

The system is designed to be:
- Easy to set up and use
- Works with Blender 4.x
- Handles frame distribution automatically
- Creates MP4 videos from rendered frames
- Provides job monitoring and management through the Blender UI

## Features

- **Blend File Handling**: Efficient transfer of blend data
- **Automatic Frame Distribution**: Distributes frames evenly across clients
- **Job Management**: Create, monitor, cancel, and complete jobs
- **Client Auto-Discovery**: Clients automatically connect to the server
- **Fault Tolerance**: Automatically reassigns frames from disconnected clients
- **MP4 Creation**: Automatically creates videos from rendered frames
- **Flexible Output Organization**: Configurable directory structure for frames and videos

## Installation

### Server Setup

1. Open Blender (4.0+)
2. Go to Edit > Preferences > Add-ons > Install
3. Select the `blender-render-farm-plugin.py` file and click 'Install Add-on'
4. Enable the add-on by checking the box next to "Render: Minimal Render Server (In-Memory)"
5. Configure the addon settings:
   - Set the server port (default: 9090)
   - Set the output directory for rendered frames

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

### Server (Blender UI)

1. Open Blender on the computer that will act as the render server
2. Go to the "Render Properties" tab
3. Find the "Render Server (In-Memory)" panel
4. Click "Start Server" to begin listening for clients
5. Configure job settings:
   - Set "Frames Per Chunk" (how many frames to assign to a client at once)
   - Enable/disable "Pack Textures" 
   - Configure MP4 video creation settings

### Creating a Render Job

1. Open your Blender file with the animation you want to render
2. Set up your render settings (resolution, samples, etc.) as you normally would
3. In the "Render Server" panel, click "Create New Render Job"
4. The job will be distributed to any connected clients

### Managing Jobs

- **Monitor Progress**: The server UI shows job progress, including frame count and percentage
- **Cancel Job**: Click "Cancel Job" to stop an active job
- **Force Complete**: Click "Force Complete" to mark a stalled job as complete
- **Clear Completed Job**: Remove completed job data from memory

### MP4 Video Creation

When a job completes, an MP4 video is automatically created from the rendered frames. You can configure:

- **Video Codec**: H.264, H.265/HEVC, or ProRes
- **Video Quality**: High, Medium, or Low
- **MP4 Location**: Parent Directory, Frames Directory, or Root Output Directory

## Directory Structure

The system organizes rendered files in the following structure:

```
output_directory/
├── job_001_1234567890/                # Job-specific directory
│   ├── frame_0001_.png                # Individual frame files
│   ├── frame_0002_.png
│   ├── ...
│   └── job_001_1234567890_render.mp4  # Optional: MP4 in frames directory
│
├── job_001_1234567890_render.mp4      # Optional: MP4 in parent directory
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

- **Server**: Blender 4.0 or higher
- **Client**: Blender 4.0 or higher + Python 3.7+
- **Network**: Clients must be able to connect to the server's IP address and port
- **MP4 Creation**: FFmpeg installed on the server machine (for video creation)

## Troubleshooting

### Common Issues

- **Connection Issues**: Make sure the server IP is correct and the port is open in firewalls
- **Stalled Jobs**: Use the "Force Complete" button if jobs get stuck
- **MP4 Creation Fails**: Verify FFmpeg is installed on the server machine
- **Client Disconnects**: Check network stability and increase timeout settings if needed

### Logs

- Server logs are visible in the Blender console
- Client logs are printed to the console/terminal where the client is running

## License

APACHE 2.0 License - See LICENSE file for details.

## Acknowledgments
- Generated usign AI tools such as Claud Code and Gimini 2.5
- Built for Blender 4.x
- Uses FFmpeg for video encoding
- Thanks to everyone in the Blender community

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
