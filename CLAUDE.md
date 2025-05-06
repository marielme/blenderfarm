# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Run Commands
- Run blender server: `blender --python blender_add-on_microfarm_server.py:`
- Run client: `blender --background --python microfarm_client.py -- --server SERVER_IP --port 9090 --name CLIENT_NAME`
- Test with demo file: `blender demo.blend`

## Code Style Guidelines
- **Formatting**: 4-space indentation, 88-char line length
- **Imports**: Group imports (standard lib, third-party, local) with blank lines between groups
- **Naming**: snake_case for variables/functions, CamelCase for classes, UPPER_CASE for constants
- **Types**: Use Python type hints (List, Dict, Optional, etc.)
- **Error Handling**: Use try/except with specific exceptions, log errors with traceback
- **Logging**: Use print statements with descriptive prefixes (e.g., "Render Server:")
- **Documentation**: Docstrings for classes and functions ("""triple quotes""")
- **Threading**: Use RLock for shared resources, mark critical sections with comments

## Project Structure
- blender_add-on_microfarm_server.py: Blender add-on for server functionality
- blender_add-on_microfarm_client.py: Blender add-on for client functionality
- microfarm_client.py: Client script for worker nodes
- microfarm_server.py: Standalone server script with web interface
- demo.blend: Example Blender file for testing