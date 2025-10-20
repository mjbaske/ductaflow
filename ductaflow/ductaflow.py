# %% [markdown]
# # ductaflow - Core Pipeline Framework


# %%
import json
import subprocess
from pathlib import Path
from datetime import datetime
import shutil
import pandas as pd
import os
import jupytext
import logging
import re
from typing import Dict, Any, Optional, Union

# %%
try:
    import papermill as pm
except ImportError:
    print("Warning: papermill not installed. Install with: pip install papermill")
    pm = None

# %% [markdown]
# ## Simple Logging Approach
# 
# ductaflow uses simple print statements for logging.
# All output automatically gets saved to .txt files in execution directories.
# No complex logging setup needed - just print what you need to see.

# %% [markdown]
# ## Configuration Display Functions

# %%
def generate_config_markdown(config: Dict[str, Any], flow_name: str = None) -> str:
    """
    Generate markdown representation of configuration dictionary.
    
    Args:
        config: Configuration dictionary to display
        flow_name: Optional step name for the header
    
    Returns:
        Markdown formatted string
    """
    if not config:
        return "**No configuration provided**\n"
    
    # Start with header if step name provided
    markdown_lines = []
    if flow_name:
        markdown_lines.append(f"## Configuration for {flow_name}\n")
    else:
        markdown_lines.append("## Configuration\n")
    
    # Separate flat and nested items
    flat_items = {}
    nested_items = {}
    
    for key, value in config.items():
        if isinstance(value, dict) and value:  # Non-empty dict
            nested_items[key] = value
        else:
            flat_items[key] = value
    
    # Display flat items as table if any exist
    if flat_items:
        markdown_lines.append("| Parameter | Value |")
        markdown_lines.append("|-----------|-------|")
        
        for key, value in flat_items.items():
            # Format value for display
            if isinstance(value, (list, tuple)):
                display_value = ", ".join(str(v) for v in value)
            elif isinstance(value, bool):
                display_value = "✓" if value else "✗"
            elif value is None:
                display_value = "*None*"
            elif isinstance(value, (int, float)):
                display_value = str(value)
            else:
                # Escape pipes and limit length for table display
                display_value = str(value).replace("|", "\\|")
                if len(display_value) > 50:
                    display_value = display_value[:47] + "..."
            
            markdown_lines.append(f"| {key} | {display_value} |")
        
        markdown_lines.append("")  # Empty line after table
    
    # Display nested items with headers and tables
    for section_key, section_value in nested_items.items():
        markdown_lines.append(f"### {section_key.replace('_', ' ').title()}")
        markdown_lines.append("")
        
        if isinstance(section_value, dict):
            markdown_lines.append("| Parameter | Value |")
            markdown_lines.append("|-----------|-------|")
            
            for key, value in section_value.items():
                # Format value for display
                if isinstance(value, (list, tuple)):
                    display_value = ", ".join(str(v) for v in value)
                elif isinstance(value, bool):
                    display_value = "✓" if value else "✗"
                elif value is None:
                    display_value = "*None*"
                elif isinstance(value, dict):
                    display_value = f"*{len(value)} items*"
                elif isinstance(value, (int, float)):
                    display_value = str(value)
                else:
                    # Escape pipes and limit length for table display
                    display_value = str(value).replace("|", "\\|")
                    if len(display_value) > 50:
                        display_value = display_value[:47] + "..."
                
                markdown_lines.append(f"| {key} | {display_value} |")
        else:
            # Handle non-dict nested values
            markdown_lines.append(f"```")
            markdown_lines.append(str(section_value))
            markdown_lines.append(f"```")
        
        markdown_lines.append("")  # Empty line after section
    
    return "\n".join(markdown_lines)


# %%
def display_config_summary(config: Dict[str, Any], flow_name: str = None):
    """
    Display configuration summary in notebook.
    Simple function for notebooks to import and use.
    
    Parameters:
    -----------
    config : dict
        Configuration dictionary (from papermill injection)
    flow_name : str, optional
        Name of current processing step
    """
    try:
        from IPython.display import Markdown, display
        markdown_content = generate_config_markdown(config, flow_name)
        display(Markdown(markdown_content))
    except Exception as e:
        print(f"Error displaying config summary: {e}")


# %% [markdown]
# ## Core Pipeline Functions


# %% [markdown]
# ## HTML Export Functions

# %%
def convert_notebook_to_html(notebook_path: Union[str, Path], 
                           html_path: Union[str, Path],
                           template: str = 'lab') -> None:
    """
    Convert executed notebook to HTML using nbconvert
    
    Args:
        notebook_path: Path to the executed .ipynb notebook
        html_path: Path where HTML file should be saved
        template: nbconvert template to use ('lab', 'classic', 'reveal', etc.)
    """
    try:
        import nbconvert
        from nbconvert import HTMLExporter
        
        # Create HTML exporter with specified template
        exporter = HTMLExporter()
        exporter.template_name = template
        
        # Read and convert notebook
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook_content = f.read()
        
        (body, resources) = exporter.from_filename(str(notebook_path))
        
        # Write HTML file
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(body)
            
    except ImportError:
        # Fallback to command line nbconvert if library import fails
        try:
            subprocess.run([
                'jupyter', 'nbconvert', '--to', 'html', 
                '--template', template,
                '--output', str(html_path),
                str(notebook_path)
            ], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError(f"HTML conversion failed: {e}")
    except Exception as e:
        raise RuntimeError(f"HTML conversion failed: {e}")



# %% [markdown]
# ## CLI Helper Function

# %%
def is_notebook_execution() -> bool:
    """
    Check if we're in notebook/papermill execution vs CLI.
    
    Returns:
        True if notebook/papermill execution, False if CLI
    """
    import sys
    
    # Method 1: Check for IPython/Jupyter environment (most reliable)
    try:
        # This will exist in both interactive notebooks and papermill execution
        from IPython import get_ipython
        if get_ipython() is not None:
            return True
    except ImportError:
        pass
    
    # Method 2: Check for CLI arguments (fallback)
    # If we have CLI args like --config, we're definitely in CLI mode
    if len(sys.argv) > 1 and any(arg.startswith('--') for arg in sys.argv[1:]):
        return False
    
    # Method 3: Default to notebook execution if uncertain
    return True

def load_cli_config(default_config_path: str, description: str = 'Run ductaflow analysis') -> Dict[str, Any]:
    """
    Handle command-line argument parsing and config loading for flow CLI mode.
    
    Parses --config and --output-dir arguments, loads JSON config file, and optionally 
    changes to execution directory to match ductaflow behavior.
    
    Args:
        default_config_path: Default path to config file if --config not provided
        description: Description for the argument parser
    
    Returns:
        Dictionary containing loaded configuration
    """
    import argparse
    import json
    import os
    import sys
    from pathlib import Path
    
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', type=str, default=default_config_path,
                       help=f'Path to JSON config file (default: {default_config_path})')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory to execute in (matches ductaflow behavior)')
    parser.add_argument('--no-execute', action='store_true',
                       help='Set up execution environment but do not run - for testing/debugging')
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    print(f"📊 Loaded config from {args.config}")
    
    # Simple project root injection for CLI mode
    if '_project_root' not in config:
        config['_project_root'] = str(Path.cwd().resolve())
    print(f"📁 Project context: {config['_project_root']}")
    
    # Handle --no-execute mode
    if args.no_execute:
        print("🔍 --no-execute mode: Setting up environment without execution")
        config['_no_execute'] = True
    
    # Change to output directory if specified (matches ductaflow execution_dir behavior)
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        os.chdir(output_dir)
        print(f"📁 Changed to execution directory: {output_dir}")
        print(f"🚀 Running as CLI script in: {os.getcwd()}")
        
        # Save config to output directory for reproducibility
        flow_name = Path(sys.argv[0]).stem if sys.argv else "flow"
        config_filename = f"{flow_name}_config.json"
        with open(config_filename, 'w') as f:
            json.dump(config, f, indent=2, default=str)
        print(f"💾 Saved config to: {config_filename}")
        
        # Set up CLI execution output capture
        execution_log = Path(f"{flow_name}_execution_output.txt")
        
        # Simple tee functionality for CLI mode
        class CLITeeOutput:
            def __init__(self, console, file_path):
                self.console = console
                self.file_path = file_path
                self.file = open(file_path, 'w', encoding='utf-8')
                
                # Write CLI execution header
                self.file.write(f"🚀 CLI Execution started: {datetime.now().isoformat()}\n")
                self.file.write(f"📁 Working directory: {os.getcwd()}\n")
                self.file.write(f"📋 Config: {config_filename}\n")
                self.file.write("-" * 60 + "\n")
                self.file.flush()
            
            def write(self, text):
                self.console.write(text)
                self.file.write(text)
                self.file.flush()
            
            def flush(self):
                self.console.flush()
                self.file.flush()
            
            def close(self):
                self.file.write("-" * 60 + "\n")
                self.file.write(f"✅ CLI Execution completed: {datetime.now().isoformat()}\n")
                self.file.close()
        
        # Set up output capture for CLI mode
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        try:
            # Create tee outputs
            stdout_tee = CLITeeOutput(original_stdout, execution_log)
            stderr_tee = CLITeeOutput(original_stderr, execution_log.with_suffix('.err.txt'))
            
            sys.stdout = stdout_tee
            sys.stderr = stderr_tee
            
            print(f"📝 CLI output being captured to: {execution_log}")
            
            # Register cleanup function to restore stdout/stderr and close files
            import atexit
            def cleanup():
                sys.stdout = original_stdout
                sys.stderr = original_stderr
                try:
                    stdout_tee.close()
                    stderr_tee.close()
                except:
                    pass
            atexit.register(cleanup)
            
        except Exception as e:
            # If tee setup fails, restore original and continue
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            print(f"⚠️ Could not set up CLI output capture: {e}")
    else:
        print(f"🚀 Running as CLI script in: {os.getcwd()}")
    
    return config

def unpack_config(config: Optional[Dict] = None, 
                 flow_name: Optional[str] = None,
                 caller_locals: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Standardized config unpacking for flows and builds.
    
    Extracts all config parameters as variables and handles fundamental patterns:
    - Flattens nested dictionaries
    - Converts common path strings to Path objects
    - Displays config summary
    
    Args:
        config: Configuration dictionary
        flow_name: Name for display in config summary
        caller_locals: locals() dict from calling scope to update
        
    Returns:
        Dictionary of unpacked variables
        
    Usage:
        # Standard pattern at top of flows/builds
        from ductaflow import unpack_config
        
        # Extract all config as local variables
        vars().update(unpack_config(config, "My Flow Name", locals()))
    """
    if not config:
        print("⚠️ No config found - run with --config file.json or via papermill injection")
        return {}
    
    # Display config summary if flow name provided
    if flow_name:
        display_config_summary(config, flow_name)
    
    unpacked_vars = {}
    
    print("📋 Unpacking config variables:")
    
    # Extract config variables
    for key, value in config.items():
        if isinstance(value, dict):
            # Add the dict itself
            unpacked_vars[key] = value
            print(f"📋 {key} = {{{len(value)} items}}")
            
            # Flatten nested dict keys
            for sub_key, sub_value in value.items():
                unpacked_vars[sub_key] = sub_value
                print(f"📋   {sub_key} = {sub_value}")
        else:
            unpacked_vars[key] = value
            print(f"📋 {key} = {value}")
    
    # Convert common path variables to Path objects
    path_keys = ['model_runs_dir', 'run_folder', 'output_dir', 'data_dir', 
                 'input_dir', 'results_dir', '_project_root']
    
    for key in path_keys:
        if key in unpacked_vars and unpacked_vars[key] is not None:
            unpacked_vars[key] = Path(unpacked_vars[key])
            print(f"🗂️  Converted {key} to Path object")
    
    
    # Update caller's locals if provided
    if caller_locals is not None:
        caller_locals.update(unpacked_vars)
    
    print(f"✅ Unpacked {len(unpacked_vars)} config variables")
    
    return unpacked_vars

# %% [markdown]
# ## Core Notebook Execution Function

# %%
def run_notebook(notebook_file: Union[str, Path], 
                notebooks_dir: Optional[Union[str, Path]] = None,
                config: Dict[str, Any] = {'param1': 'value1'},
                output_suffix: str = "_executed",
                kernel_name: str = "python3",
                timeout: Optional[int] = None,
                export_html: bool = True,
                execution_dir: Optional[Union[str, Path]] = None,
                project_root: Optional[Union[str, Path]] = None,
                no_execute: bool = False) -> Path:
    """
    Execute a Jupytext .py file as a notebook with configuration parameters
    
    Args:
        notebook_file: Path to the .py notebook file to execute
        config: Dictionary of config vars 
        notebooks_dir: Directory containing notebook files (defaults to current working directory)
        output_suffix: Suffix for output notebook filename
        kernel_name: Jupyter kernel to use for execution
        timeout: Execution timeout in seconds (None for unlimited)
        export_html: Whether to also export an HTML version of the executed notebook
        execution_dir: Directory where notebook should be executed (changes working directory)
        project_root: Path to project root (auto-injects _project_root, _flows_dir, _builds_dir into config)
        no_execute: If True, set up environment but don't execute notebook (for testing)
    
    Returns:
        Path to the executed notebook file
        
    Raises:
        FileNotFoundError: If notebook or config file not found
        PapermillExecutionError: If notebook execution fails
    """
    if pm is None:
        raise ImportError("papermill is required for notebook execution. Install with: pip install papermill")
    
    # Setup paths
    notebook_file = Path(notebook_file)
    
    if notebooks_dir:
        source_notebook = Path(notebooks_dir) / notebook_file
    else:
        source_notebook = notebook_file
    
    if not source_notebook.exists():
        raise FileNotFoundError(f"Notebook file not found: {source_notebook}")
    
    # Setup output path
    # Always just put the executed notebook in the run folder
    output_notebook = Path(f"./{notebook_file.stem}{output_suffix}.ipynb")
    
    # Convert .py to .ipynb if needed
    temp_ipynb = None
    if source_notebook.suffix == '.py':
        # Read the jupytext file
        nb = jupytext.read(source_notebook)
        
        # Add kernel specification if missing
        if 'kernelspec' not in nb.metadata:
            nb.metadata['kernelspec'] = {
                "display_name": "Python 3",
                "language": "python",
                "name": kernel_name
            }
        
        # Write temporary .ipynb file
        temp_ipynb = source_notebook.with_suffix('.ipynb')
        jupytext.write(nb, temp_ipynb)
        source_notebook = temp_ipynb
    
    try:       
        # Handle execution directory change if specified
        original_cwd = None
        if execution_dir:
            execution_dir = Path(execution_dir)
            execution_dir.mkdir(parents=True, exist_ok=True)
            original_cwd = os.getcwd()
            os.chdir(execution_dir)
            print(f"📁 Changed to execution directory: {execution_dir}")
        
        # Simple project root injection
        enhanced_config = config.copy()
        if project_root:
            enhanced_config['_project_root'] = str(Path(project_root).resolve())
        elif '_project_root' not in enhanced_config:
            # Auto-detect project root - simple version
            enhanced_config['_project_root'] = str(Path.cwd().resolve())
        
        # Save enhanced config to output directory for reproducibility
        config_filename = f"{notebook_file.stem}_config.json"
        with open(config_filename, 'w') as f:
            json.dump(enhanced_config, f, indent=2, default=str)
        print(f"💾 Saved config to: {config_filename}")
        print(f"📁 Project context: {enhanced_config.get('_project_root', 'current directory')}")
        
        # Check for no-execute mode
        if no_execute or config.get('_no_execute', False):
            print(f"🔍 No-execute mode: Environment set up for {notebook_file}")
            print(f"📁 Working directory: {os.getcwd()}")
            print(f"📋 Config available: {config_filename}")
            print(f"🚀 To debug: Open {notebook_file} as notebook and run cells interactively")
            
            # Create a placeholder executed notebook for consistency
            placeholder_notebook = Path(f"./{notebook_file.stem}{output_suffix}.ipynb")
            if source_notebook.suffix == '.py':
                # Convert to notebook format for interactive use
                nb = jupytext.read(source_notebook)
                jupytext.write(nb, placeholder_notebook)
                print(f"📓 Interactive notebook ready: {placeholder_notebook}")
            
            return placeholder_notebook
        
        # Set up execution output capture
        execution_log = Path(f"{notebook_file.stem}_execution_output.txt")
        
        # Set up papermill execution parameters
        execute_params = {
            "input_path": str(source_notebook),
            "output_path": str(output_notebook),
            "parameters": {"config": enhanced_config},  # Use enhanced config with project context
            "kernel_name": kernel_name,
            "request_save_on_cell_execute": True,
            "progress_bar": True
        }
        
        # Only add timeout if it's specified (not None)
        if timeout is not None:
            execute_params["execution_timeout"] = timeout
        
        # Capture stdout/stderr to execution log file
        import sys
        import io
        from contextlib import redirect_stdout, redirect_stderr
        
        # Create a combined output capture
        log_capture = io.StringIO()
        
        try:
            # Capture both stdout and stderr while still showing in console
            with open(execution_log, 'w', encoding='utf-8') as log_file:
                # Custom print function that writes to both console and file
                original_stdout = sys.stdout
                original_stderr = sys.stderr
                
                class TeeOutput:
                    def __init__(self, console, file):
                        self.console = console
                        self.file = file
                    
                    def write(self, text):
                        self.console.write(text)
                        self.file.write(text)
                        self.file.flush()
                    
                    def flush(self):
                        self.console.flush()
                        self.file.flush()
                
                # Redirect both stdout and stderr to tee to both console and file
                sys.stdout = TeeOutput(original_stdout, log_file)
                sys.stderr = TeeOutput(original_stderr, log_file)
                
                # Write execution start to log
                print(f"🚀 Execution started: {datetime.now().isoformat()}")
                print(f"📁 Working directory: {os.getcwd()}")
                print(f"📋 Config: {config_filename}")
                print("-" * 60)
                
                # Execute notebook
                pm.execute_notebook(**execute_params)
                
                print("-" * 60)
                print(f"✅ Execution completed: {datetime.now().isoformat()}")
                
        finally:
            # Always restore original stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
        
        print(f"✓ Successfully executed: {notebook_file}")
        
        # Export to HTML if requested
        if export_html:
            try:
                html_output = output_notebook.with_suffix('.html')
                convert_notebook_to_html(output_notebook, html_output)
                print(f"✓ HTML export created: {html_output}")
            except Exception as e:
                print(f"⚠️ HTML export failed: {e}")
        
        return output_notebook
        
    except pm.PapermillExecutionError as e:
        print(f"✗ Notebook execution failed: {notebook_file}")
        print(f"Error: {e}")
        # Papermill automatically saves the failed notebook with error state
        raise
    
    finally:
        # Restore original working directory if changed
        if original_cwd:
            os.chdir(original_cwd)
            print(f"📁 Restored working directory: {original_cwd}")
        
        # Clean up temporary files
        if temp_ipynb and temp_ipynb.exists():
            temp_ipynb.unlink()
    
    return output_notebook

# %%
def debug_flow(flow_path: str, execution_dir: Union[str, Path], config: Dict[str, Any] = None, force: bool = False) -> Path:
    """
    Set up a flow for traditional IDE debugging using --no-execute to prepare the environment.
    
    Uses run_notebook with --no-execute to create the exact execution environment,
    then provides VS Code launch.json configuration for IDE debugging.
    
    Args:
        flow_path: Path to the .py flow file (e.g., "flows/my_flow.py", "builds/build_shell.py")
        execution_dir: Full path to execution directory (e.g., "runs/step/instance", 
                      "runs/build_instance/execution/setup/flow_name", 
                      "runs/conductor_runs/Model_Runs/Scenario_Enhanced")
        config: Configuration dictionary (if None, tries to load from existing run)
        force: If True, delete existing run directory and recreate
    
    Returns:
        Path to the execution directory
        
    Usage:
        # Debug a simple flow
        debug_flow("flows/flow_shell1.py", "runs/debug/test_scenario", {"param1": 100})
        
        # Debug a build with nested structure
        debug_flow("builds/build_shell.py", "runs/build_instance/execution", config)
        
        # Debug an existing failed run (auto-loads config)
        debug_flow("flows/flow_shell1.py", "runs/conductor_runs/Model_Runs/Scenario_Enhanced")
        
        # Debug a flow within a build's nested structure
        debug_flow("flows/flow_shell1.py", "runs/build_instance/execution/setup/flow_shell1", config)
    """
    from pathlib import Path
    import json
    import shutil
    
    execution_dir = Path(execution_dir)
    flow_name = Path(flow_path).stem
    
    # Handle existing directory
    if execution_dir.exists():
        if force:
            print(f"🗑️ Removing existing directory: {execution_dir}")
            shutil.rmtree(execution_dir)
        elif config is None:
            # Try to load existing config - check multiple possible config file names
            possible_configs = [
                execution_dir / f"{flow_name}_config.json",
                execution_dir / "config.json",
                execution_dir / f"{execution_dir.name}_config.json"  # For build instances
            ]
            
            existing_config = None
            for config_path in possible_configs:
                if config_path.exists():
                    existing_config = config_path
                    break
            
            if existing_config:
                with open(existing_config, 'r') as f:
                    config = json.load(f)
                print(f"📋 Using existing config from: {existing_config}")
            else:
                raise ValueError(f"Directory {execution_dir} exists but no config provided and no existing config found. Use force=True to recreate.")
        else:
            print(f"📁 Using existing directory: {execution_dir}")
    
    # Ensure we have a config
    if config is None:
        raise ValueError("No config provided and couldn't load from existing run")
    
    # Use run_notebook with --no-execute to set up the environment
    print(f"🔍 Setting up debug environment with --no-execute...")
    config_with_no_execute = {**config, '_no_execute': True}
    
    result_path = run_notebook(
        notebook_file=flow_path,
        config=config_with_no_execute,
        execution_dir=execution_dir,
        no_execute=True
    )
    
    # Generate VS Code launch.json configuration
    config_filename = f"{flow_name}_config.json"
    
    # Create a clean debug name from the execution path
    debug_name_parts = []
    if "conductor_runs" in str(execution_dir):
        debug_name_parts.append("Conductor")
    if "build" in str(execution_dir).lower():
        debug_name_parts.append("Build")
    debug_name_parts.extend([flow_name, execution_dir.name])
    debug_name = " - ".join(debug_name_parts)
    
    launch_config = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": f"Debug {debug_name}",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/" + flow_path,
                "args": ["--config", config_filename],
                "cwd": "${workspaceFolder}/" + str(execution_dir),
                "console": "integratedTerminal",
                "justMyCode": False,
                "stopOnEntry": False
            }
        ]
    }
    
    print(f"""
            🎯 VS Code Debug Setup Complete!

            📁 Execution directory: {execution_dir}
            📋 Config file: {execution_dir}/{config_filename}
            📓 Notebook ready: {execution_dir}/{flow_name}_executed.ipynb

            🚀 Add this to .vscode/launch.json:
            """)
                print(json.dumps(launch_config, indent=2))
                
                print(f"""
            💡 Next steps:
            1. Set breakpoints in {flow_path}
            2. Press F5 in VS Code
            3. Select "Debug {debug_name}"
            4. Step through the actual flow execution!

            🔍 The flow will execute in {execution_dir} with the exact config that was saved.

            📂 File System Schema Support:
            ✓ Simple: runs/step/instance/
            ✓ Nested: runs/build_instance/execution/phase/flow_name/
            ✓ Conductor: runs/conductor_runs/Model_Runs/Scenario_Name/
            ✓ Custom: any/path/structure/you/need/
            """)
    
    return execution_dir

# %%
def run_step_flow(notebook_path: str, step_name: str, instance_name: str, config: Dict[str, Any], suffix: str = "") -> Path:
    """
    Execute a flow with automatic directory management (default ductaflow pattern).
    
    Args:
        notebook_path: Path to the .py flow file
        step_name: Name of the step (creates runs/{step_name}/ directory)
        instance_name: Name of this instance (creates runs/{step_name}/{instance_name}/)
        config: Configuration dictionary to inject as parameters
        suffix: Optional suffix for multiple calls to same flow (e.g., iteration number)
    
    Returns:
        Path to executed notebook
    """
    import time
    
    # Build full instance name with suffix if provided
    full_instance_name = f"{instance_name}{suffix}" if suffix else instance_name
    
    # Create output directory structure
    output_dir = Path(f"runs/{step_name}/{full_instance_name}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Simple logging - just print statements
    flow_name = Path(notebook_path).stem
    print(f"🚀 Starting {flow_name} | Instance: {full_instance_name}")
    
    try:
        start_time = time.time()
        
        result = run_notebook(
            notebook_file=notebook_path,
            config=config,
            execution_dir=output_dir,
            no_execute=config.get('_no_execute', False)
        )
        
        # Simple completion logging
        execution_time = time.time() - start_time
        print(f"✅ Completed {flow_name} | Instance: {full_instance_name} | Time: {execution_time:.1f}s")
        
        return result
        
    except Exception as e:
        # Simple error logging
        print(f"❌ Failed {flow_name} | Instance: {full_instance_name} | Error: {str(e)}")
        raise

# %% [markdown]
# ## Status Analysis Functions

# %%
def analyze_execution_logs(output_dir: Union[str, Path]) -> str:
    """
    Analyze execution logs to determine build and flow status.
    
    Scans *_execution_output.txt files for status indicators in the first 15 characters
    of each line to avoid false positives from data content.
    
    Args:
        output_dir: Directory containing execution logs
        
    Returns:
        - 'success': No warnings or errors
        - 'warning': Contains warning indicators but completed
        - 'error': Contains error indicators or failed to complete
        
    Usage:
        status = analyze_execution_logs("runs/my_build/scenario_1")
        print(f"Build status: {status}")  # 'success', 'warning', or 'error'
    """
    output_dir = Path(output_dir)
    log_files = list(output_dir.rglob("*_execution_output.txt"))
    
    has_warnings = False
    has_errors = False
    
    # Status indicators to look for in first 15 characters of lines
    warning_indicators = ['⚠️', 'warning:', 'Warning:', 'WARNING:']
    error_indicators = ['❌', 'error:', 'Error:', 'ERROR:', 'Exception:', 'Failed:', 'failed:']
    
    for log_file in log_files:
        try:
            content = log_file.read_text(encoding='utf-8')
            for line in content.split('\n'):
                # Only check first 15 characters to avoid false positives in data
                line_start = line[:15].lower()
                
                # Check for warnings
                if any(indicator.lower() in line_start for indicator in warning_indicators):
                    has_warnings = True
                
                # Check for errors
                if any(indicator.lower() in line_start for indicator in error_indicators):
                    has_errors = True
                    
        except Exception:
            # If we can't read a log file, treat as error
            has_errors = True
            continue
    
    if has_errors:
        return 'error'
    elif has_warnings:
        return 'warning'
    else:
        return 'success'

def generate_status_report(results: list, model_root: Union[str, Path]) -> str:
    """
    Generate nested HTML status report showing build and flow-level status.
    
    Creates an expandable HTML report with:
    - Build-level status (red/orange/green)
    - Expandable sections showing individual flow status
    - Summary statistics
    
    Args:
        results: List of result dictionaries from conductor execution
        model_root: Root directory containing execution outputs
        
    Returns:
        HTML string for the status report
        
    Usage:
        # After conductor execution
        report_html = generate_status_report(results, model_root)
        with open("status_report.html", 'w') as f:
            f.write(report_html)
    """
    model_root = Path(model_root)
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Conductor Execution Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .scenario {{ margin: 10px 0; border: 1px solid #ddd; border-radius: 5px; }}
            .scenario-header {{ padding: 10px; cursor: pointer; font-weight: bold; }}
            .scenario-content {{ padding: 10px; display: none; background: #f9f9f9; }}
            .success {{ background-color: #d4edda; color: #155724; }}
            .warning {{ background-color: #fff3cd; color: #856404; }}
            .error {{ background-color: #f8d7da; color: #721c24; }}
            .flow {{ margin: 5px 0; padding: 5px; border-radius: 3px; }}
            .expand-btn {{ float: right; }}
            .summary {{ background: #e9ecef; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        </style>
        <script>
            function toggleScenario(id) {{
                var content = document.getElementById(id);
                var btn = document.getElementById(id + '_btn');
                if (content.style.display === 'none') {{
                    content.style.display = 'block';
                    btn.innerHTML = '▼';
                }} else {{
                    content.style.display = 'none';
                    btn.innerHTML = '▶';
                }}
            }}
        </script>
    </head>
    <body>
        <h1>🎯 Conductor Execution Report</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <div class="summary">
            <h2>📊 Summary</h2>
            <p><strong>Total Scenarios:</strong> {len(results)}</p>
            <p><strong>Successful:</strong> {len([r for r in results if r['status'] == 'success'])}</p>
            <p><strong>Failed:</strong> {len([r for r in results if r['status'] == 'failed'])}</p>
        </div>
        
        <h2>📋 Scenario Details</h2>
    """
    
    for i, result in enumerate(results):
        scenario_name = result['scenario']
        status = result['status']
        
        # Analyze logs for detailed status if successful
        if status == 'success':
            output_dir = model_root / "Model_Runs" / scenario_name
            detailed_status = analyze_execution_logs(output_dir)
        else:
            detailed_status = 'error'
        
        status_class = detailed_status
        status_icon = {'success': '✅', 'warning': '⚠️', 'error': '❌'}[detailed_status]
        
        html += f"""
        <div class="scenario">
            <div class="scenario-header {status_class}" onclick="toggleScenario('scenario_{i}')">
                {status_icon} {scenario_name}
                <span class="expand-btn" id="scenario_{i}_btn">▶</span>
            </div>
            <div class="scenario-content" id="scenario_{i}">
        """
        
        if status == 'success':
            # Show flow-level details
            output_dir = model_root / "Model_Runs" / scenario_name
            flow_dirs = [d for d in output_dir.rglob("runs/*") if d.is_dir()]
            
            html += "<h4>Flow Status:</h4>"
            for flow_dir in flow_dirs:
                flow_name = flow_dir.name
                flow_status = analyze_execution_logs(flow_dir)
                flow_icon = {'success': '✅', 'warning': '⚠️', 'error': '❌'}[flow_status]
                
                html += f'<div class="flow {flow_status}">{flow_icon} {flow_name}</div>'
            
            if 'duration_minutes' in result:
                html += f"<p><strong>Duration:</strong> {result['duration_minutes']:.1f} minutes</p>"
        else:
            html += f"<p><strong>Error:</strong> {result.get('error', 'Unknown error')}</p>"
        
        html += "</div></div>"
    
    html += """
        </body>
    </html>
    """
    
    return html

# %% [markdown]
# ## Flow Dependency Management

# %%


