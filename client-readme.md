# Blender Distributed Render Farm

This package contains the client scripts for connecting to a Blender render farm server.

## Overview

The Blender Distributed Render Farm system consists of two components:

1. **Server Plugin**: Runs within Blender on the machine that will coordinate rendering tasks and collect the final renders.
2. **Client Scripts**: Run on any machine that will perform rendering tasks.

## Server Setup

1. Install the addon in Blender:
   - Go to `Edit > Preferences > Add-ons > Install`
   - Select the `server.py` file
   - Enable the addon by checking the box

2. Configure server options in the Render Properties panel:
   - Set server port (default: 9090)
   - Configure output directory for rendered frames
   - Adjust frames per chunk to control how many frames each client receives

3. Start the server from the Render Properties panel

## Client Setup

1. Copy the client files to each render node:
   - `client.py` - Main client script
   - `start_client.sh` (Linux/Mac) or `start_client.bat` (Windows) - Launcher scripts

2. Make sure Blender is installed on the client machine

### Starting on Linux/Mac:

```bash
# Start client with default name
./start_client.sh

# Start client with custom name
./start_client.sh MyCustomClient
```

### Starting on Windows:

```
# Start client with default name
start_client.bat

# Start client with custom name
start_client.bat MyCustomClient
```

### Manual Configuration

If you need to specify a different server IP or port:

```bash
blender --background --python client.py -- --server SERVER_IP --port SERVER_PORT --name CLIENT_NAME --output OUTPUT_DIR
```

## How It Works

1. The server runs inside Blender on the main workstation where scenes are created
2. When a render is started, the server:
   - Packs textures into the .blend file if requested
   - Divides the frame range into chunks
   - Distributes these chunks to available clients
   - Collects rendered frames from clients
   - Assembles frames in the output directory

3. Clients:
   - Connect to the server and register themselves
   - Request work when idle
   - Render assigned frames
   - Send completed frames back to the server
   - Repeat until all frames are rendered

## Troubleshooting

- Make sure the server is running before starting clients
- Check that the server IP and port are correct
- Ensure firewall rules allow connections to the server port
- If Blender isn't found automatically, edit the start script to point to your Blender installation
- Check server logs in Blender's console for errors
- Verify that clients can reach the server (try pinging the server IP)

## Network Requirements

- Server must be accessible from all clients (fixed IP or hostname)
- Port 9090 (or your custom port) must be open on the server
- Sufficient network bandwidth for transferring .blend files and rendered images

## Best Practices

- Use a wired network connection for better performance
- Keep texture sizes reasonable to minimize .blend file transfer time
- Split very large renders into multiple jobs
- Test with a small frame range before starting a large render job
- Consider using relative paths for textures in your Blender project
- Back up your original .blend files before rendering