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

# %%
def make_notebook_scriptable(notebook_path: Union[str, Path], 
                           output_dir: Union[str, Path] = None) -> Path:
    """
    Generate a .bat file and modified .py file to make a notebook scriptable.
    
    Args:
        notebook_path: Path to the .py notebook file
        output_dir: Directory to save the scriptable version (defaults to same dir as notebook)
        
    Returns:
        Path to the generated .bat file
        
    Creates:
        - {notebook_name}_script.py (modified with CLI argument parsing)
        - run_{notebook_name}.bat (batch file for easy execution)
    """
    notebook_path = Path(notebook_path)
    if output_dir is None:
        output_dir = notebook_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read original notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
    
    # Generate scriptable Python file
    script_name = f"{notebook_path.stem}_script.py"
    script_path = output_dir / script_name
    
    # Auto-generated header for CLI argument parsing
    cli_header = '''#!/usr/bin/env python
"""
Auto-generated scriptable version of {notebook_name}
Run with: python {script_name} --config path/to/config.json
Or: run_{notebook_stem}.bat path/to/config.json
"""

import sys
import json
import argparse
from pathlib import Path

def load_config_from_args():
    """Parse command line arguments and load config"""
    parser = argparse.ArgumentParser(description='Run {notebook_name} with config')
    parser.add_argument('--config', required=True, 
                       help='Path to JSON configuration file')
    parser.add_argument('--output-dir', default='.',
                       help='Output directory for results')
    
    args = parser.parse_args()
    
    # Load config from JSON file
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {{config_path}}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config, args.output_dir

if __name__ == "__main__":
    # Auto-generated config loading
    config, output_dir = load_config_from_args()
    
    # Inject config variables into global namespace (ductaflow pattern)
    for key, value in config.items():
        if isinstance(value, dict):
            # Preserve parent dict AND flatten children 
            globals()[key] = value
            for sub_key, sub_value in value.items():
                globals()[sub_key] = sub_value
        else:
            globals()[key] = value
    
    print(f"📊 Loaded config from {{sys.argv[2] if len(sys.argv) > 2 else 'command line'}}")
    print(f"🎯 Running {{Path(__file__).name}} with {{len(config)}} parameters")
    
    # Change to output directory if specified
    import os
    if output_dir != '.':
        os.makedirs(output_dir, exist_ok=True)
        os.chdir(output_dir)
        print(f"📁 Changed to output directory: {{output_dir}}")

# Original notebook content follows:
'''.format(
        notebook_name=notebook_path.name,
        script_name=script_name,
        notebook_stem=notebook_path.stem
    )
    
    # Remove any existing parameters cell tags since we're handling config differently
    cleaned_content = re.sub(r'# %% tags=\["parameters"\]\n', '# %%\n', original_content)
    
    # Combine header with original content
    scriptable_content = cli_header + cleaned_content
    
    # Write scriptable Python file
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(scriptable_content)
    
    print(f"✅ Created scriptable Python: {script_name}")
    
    # Generate Windows batch file
    bat_name = f"run_{notebook_path.stem}.bat"
    bat_path = output_dir / bat_name
    
    bat_content = f'''@echo off
REM Auto-generated batch file for {notebook_path.name}
REM Usage: {bat_name} path/to/config.json [output_directory]

if "%1"=="" (
    echo Usage: {bat_name} config.json [output_directory]
    echo Example: {bat_name} my_config.json ./results/
    exit /b 1
)

set CONFIG_FILE=%1
set OUTPUT_DIR=%2
if "%OUTPUT_DIR%"=="" set OUTPUT_DIR=.

echo 🚀 Running {notebook_path.name} as script...
echo 📊 Config: %CONFIG_FILE%
echo 📁 Output: %OUTPUT_DIR%

python "{script_name}" --config "%CONFIG_FILE%" --output-dir "%OUTPUT_DIR%"

if %ERRORLEVEL% EQU 0 (
    echo ✅ Script completed successfully
) else (
    echo ❌ Script failed with error code %ERRORLEVEL%
)
pause
'''
    
    with open(bat_path, 'w', encoding='utf-8') as f:
        f.write(bat_content)
    
    print(f"✅ Created batch file: {bat_name}")
    
    # Generate example config if it doesn't exist
    example_config_path = output_dir / f"{notebook_path.stem}_example_config.json"
    if not example_config_path.exists():
        example_config = {
            "param1": "example_value",
            "param2": 42,
            "processing_options": {
                "export_format": "parquet",
                "include_diagnostics": True
            },
            "output_settings": {
                "save_plots": True,
                "plot_format": "png"
            }
        }
        
        with open(example_config_path, 'w', encoding='utf-8') as f:
            json.dump(example_config, f, indent=2)
        
        print(f"✅ Created example config: {example_config_path.name}")
    
    print(f"\\n🎯 Usage:")
    print(f"   {bat_name} {example_config_path.name}")
    print(f"   python {script_name} --config {example_config_path.name}")
    print(f"\\n💡 Your notebook is now 'scriptable' - notebooks all the way down!")
    
    return bat_path

# %%
def create_flow_run_script(flow_path: Union[str, Path], 
                          config_path: Union[str, Path] = None,
                          output_dir: Union[str, Path] = None) -> Path:
    """
    Create a simple Python script that runs a flow with run_notebook().
    Much simpler than make_notebook_scriptable - just wraps the ductaflow API call.
    
    Args:
        flow_path: Path to the .py flow file
        config_path: Path to JSON config file (optional, will create example)
        output_dir: Directory to save the run script (defaults to same dir as flow)
        
    Returns:
        Path to the generated run script
        
    Creates:
        - {flow_name}_run.py (simple Python script)
        - {flow_name}_config.json (example config if not provided)
    """
    flow_path = Path(flow_path)
    if output_dir is None:
        output_dir = flow_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate run script name
    run_script_name = f"{flow_path.stem}_run.py"
    run_script_path = output_dir / run_script_name
    
    # Determine config file
    if config_path is None:
        config_filename = f"{flow_path.stem}_config.json"
        config_path = output_dir / config_filename
        
        # Create example config if it doesn't exist
        if not config_path.exists():
            example_config = {
                "data_source": "data/input.csv",
                "output_format": "parquet",
                "processing_params": {
                    "threshold": 0.5,
                    "method": "advanced"
                },
                "export_results": True
            }
            
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(example_config, f, indent=2)
            
            print(f"✅ Created example config: {config_filename}")
    else:
        config_path = Path(config_path)
        config_filename = config_path.name
    
    # Generate the clean run script
    run_script_content = f'''#!/usr/bin/env python
"""
Run script for {flow_path.name}

This script executes the flow using ductaflow's run_notebook() function.
Modify the config below or load from external JSON file.

Usage:
    python {run_script_name}
"""

import sys
import json
from pathlib import Path

# Add ductaflow to path (adjust if needed)
sys.path.append('code')

from ductacore import run_notebook


def load_config():
    """Load configuration for the flow execution."""
    config_file = Path("{config_filename}")
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        print(f"📊 Loaded config from {{config_file}}")
    else:
        # Fallback config if file not found
        config = {{
            "data_source": "data/input.csv",
            "output_format": "parquet", 
            "processing_params": {{
                "threshold": 0.5,
                "method": "advanced"
            }},
            "export_results": True
        }}
        print(f"⚠️ Config file not found, using default config")
    
    return config


def main():
    """Main execution function."""
    print(f"🚀 Running {flow_path.name}")
    
    # Load configuration
    config = load_config()
    
    # Execute the flow using ductaflow
    try:
        executed_notebook = run_notebook(
            notebook_file="{flow_path.as_posix()}",
            config=config,
            output_suffix="_executed",
            export_html=True
        )
        
        print(f"✅ Flow completed successfully!")
        print(f"📁 Results: {{executed_notebook}}")
        
    except Exception as e:
        print(f"❌ Flow execution failed: {{e}}")
        sys.exit(1)


if __name__ == "__main__":
    main()
'''
    
    # Write the run script
    with open(run_script_path, 'w', encoding='utf-8') as f:
        f.write(run_script_content)
    
    print(f"✅ Created run script: {run_script_name}")
    print(f"🎯 Usage: python {run_script_name}")
    print(f"📋 Config: {config_filename}")
    print(f"\\n💡 Clean Python script - no notebook complexity!")
    
    return run_script_path

# %%
def create_standalone_python_script(flow_path: Union[str, Path], 
                                   output_dir: Union[str, Path] = None) -> Path:
    """
    Convert a ductaflow .py file to a standalone Python script with config injection.
    NO dependencies on papermill or jupytext - pure Python execution.
    
    Args:
        flow_path: Path to the .py flow file
        output_dir: Directory to save the standalone script (defaults to same dir as flow)
        
    Returns:
        Path to the generated standalone Python script
        
    Creates:
        - {flow_name}_standalone.py (pure Python script with config injection)
        - {flow_name}_config.json (example config)
    """
    flow_path = Path(flow_path)
    if output_dir is None:
        output_dir = flow_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read the original flow file
    with open(flow_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
    
    # Remove Jupytext cell markers and clean up
    cleaned_content = re.sub(r'# %%.*?\n', '', original_content)  # Remove cell markers
    cleaned_content = re.sub(r'# %% \[markdown\].*?\n', '', cleaned_content)  # Remove markdown cells
    cleaned_content = re.sub(r'^#.*?\n', '', cleaned_content, flags=re.MULTILINE)  # Remove comment-only lines
    cleaned_content = re.sub(r'\n\n\n+', '\n\n', cleaned_content)  # Clean up extra newlines
    
    # Generate standalone script
    script_name = f"{flow_path.stem}_standalone.py"
    script_path = output_dir / script_name
    config_name = f"{flow_path.stem}_config.json"
    config_path = output_dir / config_name
    
    # Create example config
    example_config = {
        "data_source": "data/input.csv",
        "output_file": "results/output.parquet",
        "processing_params": {
            "threshold": 0.5,
            "method": "advanced",
            "iterations": 100
        },
        "export_plots": True,
        "debug_mode": False
    }
    
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(example_config, f, indent=2)
    
    # Generate the standalone Python script
    standalone_content = f'''#!/usr/bin/env python
"""
Standalone Python script generated from {flow_path.name}

This script runs as a normal Python file with config injection.
JSON config keys become variable names automatically.

Usage:
    python {script_name}
    
Config file: {config_name}
"""

import json
import sys
from pathlib import Path


def load_and_inject_config():
    """Load config JSON and inject keys as global variables."""
    config_file = Path(__file__).parent / "{config_name}"
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        print(f"📊 Loaded config from {{config_file.name}}")
    else:
        # Fallback config if file not found
        config = {example_config}
        print(f"⚠️ Config file not found, using default config")
    
    # Inject config keys as global variables (ductaflow pattern)
    for key, value in config.items():
        if isinstance(value, dict):
            # Preserve parent dict AND flatten children
            globals()[key] = value
            for sub_key, sub_value in value.items():
                globals()[sub_key] = sub_value
        else:
            globals()[key] = value
    
    print(f"✅ Injected {{len(config)}} config parameters as variables")
    return config


def main():
    """Main execution function."""
    print(f"🚀 Running {flow_path.name} as standalone Python script")
    
    # Load config and inject variables
    config = load_and_inject_config()
    
    # Original flow code follows below this line
    # All config keys are now available as variables
    
{cleaned_content}


if __name__ == "__main__":
    main()
'''
    
    # Write the standalone script
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(standalone_content)
    
    print(f"✅ Created standalone Python script: {script_name}")
    print(f"📋 Config file: {config_name}")
    print(f"🎯 Usage: python {script_name}")
    print(f"\\n🚀 NO DEPENDENCIES - Pure Python execution!")
    print(f"💡 JSON config keys become variable names automatically")
    
    return script_path

# %%
def create_flow_bat_runner(flow_path: Union[str, Path], 
                          config_path: Union[str, Path] = None,
                          keep_script: bool = False,
                          output_dir: Union[str, Path] = None) -> Path:
    """
    Create a .bat file that generates a standalone Python script, runs it, then cleans up.
    Perfect for conductor workflows - temporary script execution without persistence.
    
    Args:
        flow_path: Path to the .py flow file
        config_path: Path to JSON config file (optional)
        keep_script: If True, keeps the generated script after execution
        output_dir: Directory to save the bat file (defaults to same dir as flow)
        
    Returns:
        Path to the generated .bat file
        
    Creates:
        - run_{flow_name}.bat (batch file that handles everything)
    """
    flow_path = Path(flow_path)
    if output_dir is None:
        output_dir = flow_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine config file
    if config_path is None:
        config_filename = f"{flow_path.stem}_config.json"
    else:
        config_filename = Path(config_path).name
    
    # Generate bat file
    bat_name = f"run_{flow_path.stem}.bat"
    bat_path = output_dir / bat_name
    script_name = f"{flow_path.stem}_temp_script.py"
    
    # Create the batch file content
    bat_content = f'''@echo off
REM Auto-generated batch runner for {flow_path.name}
REM This creates a temporary standalone script, runs it, then cleans up

echo 🚀 ductaflow: Running {flow_path.name} as standalone Python script

REM Step 1: Generate temporary standalone script
echo 📝 Creating temporary standalone script...
python -c "import sys; sys.path.append('code'); from ductacore import create_standalone_python_script; create_standalone_python_script('{flow_path.as_posix()}', '{output_dir.as_posix()}')" > nul 2>&1

REM Check if script was created successfully
if not exist "{script_name.replace('_temp_script', '_standalone')}" (
    echo ❌ Failed to create standalone script
    exit /b 1
)

REM Rename to temp version for cleanup
ren "{flow_path.stem}_standalone.py" "{script_name}"

echo ✅ Temporary script created

REM Step 2: Execute the standalone script
echo 🎯 Executing {flow_path.name}...
python "{script_name}"

set SCRIPT_EXIT_CODE=%ERRORLEVEL%

REM Step 3: Cleanup (unless keep_script is True)
{"echo 🧹 Keeping script for inspection: " + script_name if keep_script else f'''echo 🧹 Cleaning up temporary files...
if exist "{script_name}" del "{script_name}"
if exist "{flow_path.stem}_config.json" (
    echo 📋 Config file kept: {config_filename}
) else (
    echo 📋 Config file: {config_filename}
)'''}

REM Step 4: Report results
if %SCRIPT_EXIT_CODE% EQU 0 (
    echo ✅ {flow_path.name} completed successfully
) else (
    echo ❌ {flow_path.name} failed with error code %SCRIPT_EXIT_CODE%
)

echo 💡 Pure Python execution - no notebook dependencies needed!
pause
exit /b %SCRIPT_EXIT_CODE%
'''
    
    # Write the batch file
    with open(bat_path, 'w', encoding='utf-8') as f:
        f.write(bat_content)
    
    print(f"✅ Created flow runner: {bat_name}")
    print(f"🎯 Usage: {bat_name}")
    print(f"📋 Config: {config_filename}")
    print(f"🧹 Cleanup: {'Keeps script' if keep_script else 'Deletes temp script after run'}")
    print(f"\\n💡 Perfect for conductor workflows - run and clean!")
    
    return bat_path

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
