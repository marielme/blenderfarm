# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Run Commands
- Run blender server: `blender --python blender-render-farm-plugin.py`
- Run client: `blender --background --python blender-client-script.py -- --server SERVER_IP --port 9090 --name CLIENT_NAME`
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
- blender-render-farm-plugin.py: Blender add-on for server functionality
- blender-client-script.py: Client script for worker nodes
- demo.blend: Example Blender file for testing