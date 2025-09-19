# %% [markdown]
# # ductacore - Core Pipeline Framework


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


# %%
def get_param(param_path: str, config: Dict[str, Any], default=None):
    """
    Get parameter with simple section/param syntax or just param name.
    
    Parameters:
    -----------
    param_path : str
        Parameter path like 'section/param' or just 'param'
    config : dict
        Configuration dictionary
    default : any
        Default value if parameter not found
        
    Returns:
    --------
    any
        Parameter value or default
        
    Examples:
    ---------
    get_param('use_adjusted_profiles', config)  # searches all sections
    get_param('processing_options/use_adjusted_profiles', config)  # specific section
    """
    if '/' in param_path:
        section, param = param_path.split('/', 1)
        return config.get(section, {}).get(param, default)
    else:
        # Search all sections for the parameter
        for section_data in config.values():
            if isinstance(section_data, dict) and param_path in section_data:
                return section_data[param_path]
        return default

# %% [markdown]
# ## Core Pipeline Functions

# %%
def setup_logging(run_dir: Path) -> logging.Logger:
    """Setup logging for pipeline execution"""
    log_file = run_dir / 'pipeline_execution.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('pipeline')

# %%
def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """Load configuration from JSON file with validation"""
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file {config_path}: {e}")

# %%
def save_config(config: Dict[str, Any], config_path: Union[str, Path]) -> None:
    """Save configuration to JSON file"""
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2, default=str)

# %%
def setup_run_config(base_config: Dict[str, Any], 
                    overrides: Optional[Dict[str, Any]] = None,
                    config_processors: Optional[Dict[str, callable]] = None) -> Dict[str, Any]:
    """
    Setup run configuration with overrides and custom processing
    
    Args:
        base_config: Base configuration dictionary
        overrides: Configuration overrides to apply
        config_processors: Dict of {key: processor_function} for custom config processing
    
    Returns:
        Processed configuration dictionary
    """
    config = base_config.copy()
    
    # Apply overrides
    if overrides:
        config.update(overrides)
    
    # Apply custom processors
    if config_processors:
        for key, processor in config_processors.items():
            if key in config:
                config[key] = processor(config[key])
    
    # Standard type conversions
    for key, val in config.items():
        if isinstance(val, str):
            # Convert string booleans
            if val.lower() == 'true':
                config[key] = True
            elif val.lower() == 'false':
                config[key] = False
            # Convert string paths to Path objects if they end with common path patterns
            elif any(val.endswith(ext) for ext in ['.json', '.csv', '.py', '.ipynb']) or '/' in val or '\\' in val:
                try:
                    config[key] = Path(val)
                except:
                    pass  # Keep as string if Path conversion fails
    
    return config

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
# ## Project Setup Functions

# %%
def make_new_ductaflow_instance(ductaflow_name: str, target_dir: Union[str, Path] = ".") -> Path:
    """
    Copy current ductaflow template to a new project directory.
    
    Args:
        ductaflow_name: Name for the new project
        target_dir: Where to create the new project folder
        
    Returns:
        Path to the created project directory
    """
    import shutil
    import subprocess
    
    # Setup paths
    target_dir = Path(target_dir)
    project_dir = target_dir / ductaflow_name
    current_dir = Path(__file__).parent.parent  # ductaflow root
    
    print(f"🚀 Creating ductaflow template: {ductaflow_name}")
    print(f"📁 Target: {project_dir}")
    
    # Copy entire ductaflow directory
    if project_dir.exists():
        print(f"⚠️ Directory exists, removing: {project_dir}")
        shutil.rmtree(project_dir)
    
    shutil.copytree(current_dir, project_dir, ignore=shutil.ignore_patterns('runs', '__pycache__', '.git'))
    print(f"✅ Copied ductaflow template")
    
    # Rename conductor.py to cnd_{ductaflow_name}.py
    old_conductor = project_dir / "conductor.py"
    new_conductor = project_dir / f"cnd_{ductaflow_name}.py"
    
    if old_conductor.exists():
        # Update title in conductor
        with open(old_conductor, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace title in first markdown cell
        updated_content = re.sub(
            r"# # .*? Conductor", 
            f"# # {ductaflow_name.replace('_', ' ').title()} Conductor", 
            content
        )
        
        with open(new_conductor, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        
        old_conductor.unlink()
        print(f"✅ Renamed conductor: conductor.py → cnd_{ductaflow_name}.py")
    
    # Capture git info
    try:
        git_info = subprocess.run(['git', 'rev-parse', 'HEAD'], 
                                capture_output=True, text=True, cwd=current_dir)
        if git_info.returncode == 0:
            with open(project_dir / "ductaflow_version.txt", 'w') as f:
                f.write(f"ductaflow git commit: {git_info.stdout.strip()}\n")
            print(f"✅ Captured git version: {git_info.stdout.strip()[:8]}")
    except:
        print("⚠️ Could not capture git info")
    
    print(f"\n🎉 Template ready: {ductaflow_name}")
    print(f"🚀 Open cnd_{ductaflow_name}.py as a notebook to start")
    
    return project_dir

# %%
def rename_conductor(project_dir: Union[str, Path], new_ductaflow_name: str) -> Path:
    """
    Rename conductor file and update its title to match new ductaflow name.
    
    Args:
        project_dir: Path to the ductaflow project directory
        new_ductaflow_name: New name for the ductaflow project
        
    Returns:
        Path to the renamed conductor file
    """
    project_dir = Path(project_dir)
    
    # Find existing conductor file
    conductor_files = list(project_dir.glob("cndctr_*.py"))
    if not conductor_files:
        raise FileNotFoundError("No conductor file found (cndctr_*.py)")
    
    old_conductor = conductor_files[0]
    new_conductor_filename = f"cndctr_{new_ductaflow_name}.py"
    new_conductor_path = project_dir / new_conductor_filename
    
    # Read and update conductor content
    with open(old_conductor, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Update the title in the markdown cell
    old_title_pattern = r"# # .*? Conductor"
    new_title = f"# # {new_ductaflow_name.replace('_', ' ').title()} Conductor"
    updated_content = re.sub(old_title_pattern, new_title, content)
    
    # Write to new file
    with open(new_conductor_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    # Remove old file if different name
    if old_conductor != new_conductor_path:
        old_conductor.unlink()
        print(f"🔄 Renamed conductor: {old_conductor.name} → {new_conductor_filename}")
    else:
        print(f"✅ Updated conductor title: {new_conductor_filename}")
    
    return new_conductor_path

# %% [markdown]
# ## Core Notebook Execution Function

# %%
def run_notebook(notebook_file: Union[str, Path], 
                notebooks_dir: Optional[Union[str, Path]] = None,
                config: Dict[str, Any] = {'param1': 'value1'},
                output_suffix: str = "_executed",
                kernel_name: str = "python3",
                timeout: Optional[int] = None,
                export_html: bool = True) -> Path:
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
        # Set up papermill execution parameters
        execute_params = {
            "input_path": str(source_notebook),
            "output_path": str(output_notebook),
            "parameters": {"config": config},
            "kernel_name": kernel_name,
            "request_save_on_cell_execute": True,
            "progress_bar": True
        }
        
        # Only add timeout if it's specified (not None)
        if timeout is not None:
            execute_params["execution_timeout"] = timeout
        
        pm.execute_notebook(**execute_params)
        
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
        # Clean up temporary files
        if temp_ipynb and temp_ipynb.exists():
            temp_ipynb.unlink()
