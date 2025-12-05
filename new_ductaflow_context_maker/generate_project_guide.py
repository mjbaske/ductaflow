#!/usr/bin/env python3
"""
Generate a detailed HOW TO guide for new LLM instances working on domain applications with ductaflow.

This script reads a project configuration and generates a comprehensive README
that explains ductaflow philosophy, patterns, and implementation details.
"""

import json
import sys
import io
from pathlib import Path
from datetime import datetime

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def load_project_config(config_path: str = "project_config.json") -> dict:
    """Load project configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Config file {config_path} not found!")
        print("Creating example config file...")
        
        example_config = {
            "project_name": "MyDomainProject",
            "domain": "Transportation Modeling",
            "ductaflow_location": "",
            "first_build_name": "baseline_model_run",
            "first_build_execution_dir": "runs/{first_build_name}/{scenario_name}",
            "first_flow_names": ["data_preparation", "model_execution", "results_analysis"],
            "scenario_dimensions": {
                "scenario_type": ["baseline", "optimized", "sensitivity"]
            },
            "description": "A transportation model that processes network data and generates travel forecasts"
        }
        
        with open(config_path, 'w') as f:
            json.dump(example_config, f, indent=2)
        
        print(f"✅ Created example {config_path}")
        print("Edit this file with your project details and run again.")
        sys.exit(1)

def generate_import_statement(ductaflow_location: str) -> str:
    """Generate the appropriate import statement based on ductaflow location."""
    if ductaflow_location.strip():
        return f"from {ductaflow_location} import is_notebook_execution, load_cli_config, unpack_config, run_notebook, display_config_summary"
    else:
        return "from ductaflow import is_notebook_execution, load_cli_config, unpack_config, run_notebook, display_config_summary"

def generate_project_guide(config: dict) -> str:
    """Generate the complete project guide based on configuration."""
    
    project_name = config["project_name"]
    domain = config["domain"]
    ductaflow_location = config["ductaflow_location"]
    first_build_name = config.get("first_build_name", "")
    first_build_execution_dir = config.get("first_build_execution_dir", "runs/{scenario_name}")
    first_flow_names = config["first_flow_names"]
    scenario_dimensions = config["scenario_dimensions"]
    description = config.get("description", "A domain-specific application")
    explain_build_level = config.get("explain_build_level", True)  # Default to True for backward compatibility
    
    import_statement = generate_import_statement(ductaflow_location)
    
    # Get the first scenario dimension for examples
    first_dimension_name = list(scenario_dimensions.keys())[0]
    first_dimension_values = scenario_dimensions[first_dimension_name]
    
    # Build scenario config list for conductor (avoid nested f-strings)
    scenario_config_list = ', '.join([f'{{"value": "{val}", "label": "{val.title()}"}}' for val in first_dimension_values])
    
    # Build directory structure for scenario examples (show up to 3, or all if fewer)
    scenario_dir_lines = []
    num_to_show = min(3, len(first_dimension_values))
    for i, val in enumerate(first_dimension_values[:num_to_show]):
        if i < num_to_show - 1 or len(first_dimension_values) <= 3:
            scenario_dir_lines.append(f"        ├── {val}/")
        else:
            scenario_dir_lines.append(f"        └── {val}/")
    if len(first_dimension_values) > 3:
        scenario_dir_lines.append(f"        └── ... ({len(first_dimension_values)} total scenarios)")
    scenario_dir_structure = "\n".join(scenario_dir_lines) if scenario_dir_lines else f"        └── {first_dimension_values[0]}/"
    
    guide = f"""# {project_name} - ductaflow Implementation Guide

**Domain**: {domain}  
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{description}

## So You Want to Make a New Project with ductaflow

Welcome! This guide will walk you through implementing {project_name} using ductaflow's philosophy and patterns. ductaflow is designed around three core principles:

1. **🗂️ Filesystem as Schema**: Your directory structure IS your data pipeline
2. **🔄 Everything is Re-executable**: Fix bugs by re-running just the failed step
3. **📊 Instance-based Testing**: Your real runs ARE your tests

## ductaflow Philosophy

**Stop thinking in terms of complex DAGs and orchestration tools.** Instead:

- **Flows** are atomic processing steps (`.py` files that work as both scripts and notebooks)
{f'- **Builds** orchestrate multiple flows in sequence' if explain_build_level else ''}
- **Conductor** runs flows{f' (via builds)' if explain_build_level else ''} across scenario dimensions
- **Filesystem paths** encode dependencies between instances

**The magic**: Every execution creates a complete debugging environment with exact configs, logs, and intermediate files.

{f'**Note**: This guide explains the full build/flow hierarchy. If you prefer a simpler single-level approach, you can skip builds and call flows directly from the conductor. See the CLI usage section below.' if explain_build_level else '**Note**: This guide uses a single-level flow approach. Flows are called directly without an intermediate build layer. You can also call flows individually via CLI using their default config files.'}

## Project Structure You Need to Create

```
{project_name.lower()}/
├── flows/                          # Atomic processing steps
│   ├── {first_flow_names[0]}.py
│   ├── {first_flow_names[1]}.py
│   └── {first_flow_names[2]}.py
{f'├── builds/                         # Orchestration of flows' if explain_build_level else ''}
{f'│   └── {first_build_name}.py' if explain_build_level else ''}
├── config/                         # JSON configurations
│   ├── flows/
│   │   ├── {first_flow_names[0]}.json
│   │   ├── {first_flow_names[1]}.json
│   │   └── {first_flow_names[2]}.json
{f'│   └── builds/' if explain_build_level else '│'}
{f'│       └── {first_build_name}.json' if explain_build_level else ''}
├── conductor.py                    # Multi-scenario orchestrator
└── runs/                          # All execution results
    └── {first_build_name if explain_build_level and first_build_name else 'scenario_name'}/
{scenario_dir_structure}
```

## Development Workflow

### 1. Start with a Single Flow

Create `flows/{first_flow_names[0]}.py`:

```python
# %% tags=["parameters"]
config = {{}}

# %% [markdown]
# # {first_flow_names[0].replace('_', ' ').title()}
#
# Atomic flow for {domain.lower()} processing.

# %%
# Ductaflow mandatory header
{import_statement}
if not is_notebook_execution(): # CLI mode only
    config = load_cli_config('config/flows/{first_flow_names[0]}.json', '{first_flow_names[0].replace("_", " ").title()}')
# Standardized config unpacking - ductaflow fundamental
vars().update(unpack_config(config, "{first_flow_names[0].replace('_', ' ').title()}", locals()))
# Display config summary for notebook instances
display_config_summary(config, "{first_flow_names[0].replace('_', ' ').title()}")

# %%
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import logging

# %%
# Get logger (auto-configured by run_notebook)
logger = logging.getLogger("flow:{first_flow_names[0]}")

logger.info("📊 Starting {first_flow_names[0].replace('_', ' ')}...")

# TODO: Add your domain-specific processing logic here
# Example:
# df = pd.read_csv(input_data)
# processed_df = your_domain_processing(df)
# processed_df.to_csv("processed_data.csv", index=False)

logger.info(f"✅ {first_flow_names[0].replace('_', ' ').title()} completed")

# %%
```

### 2. Create the Config File

Create `config/flows/{first_flow_names[0]}.json`:

```json
{{
  "description": "Configuration for {first_flow_names[0].replace('_', ' ')}",
  "input_data": "data/raw_data.csv",
  "output_format": "csv",
  "processing_options": {{
    "validate_inputs": true,
    "export_diagnostics": true
  }}
}}
```

### 3. Test the Flow

```python
# Test directly
from ductaflow import run_step_flow
import json

with open('config/flows/{first_flow_names[0]}.json') as f:
    config = json.load(f)

run_step_flow(
    'flows/{first_flow_names[0]}.py',
    'test_{first_flow_names[0]}',
    'debug_instance',
    config
)
```

{f'### 4. Build the Complete Build' if explain_build_level else '### 4. Create the Conductor (Single-Level Flow Approach)'}

{f'Create `builds/{first_build_name}.py`:' if explain_build_level else f'''Since we are using a single-level approach, we will create a conductor that calls flows directly. Create `conductor.py`:

**Important**: The conductor is stored as a `.py` file but is designed to be used as a **notebook** - it's your user interface for running scenarios. Unlike flows, the conductor:
- **Does NOT use the standard config unpacking pattern** (no `unpack_config()` or `load_cli_config()`)
- **Custom config is built directly in cells** - you edit scenario dimensions, base config, and execution logic interactively
- **Opened as a notebook** - run cells individually, modify configs on the fly, and iterate quickly
- **No CLI mode** - it's meant for interactive use, not script execution

This makes it easy to experiment with different scenario combinations, adjust parameters, and debug execution without editing JSON files.'''}

{f'''```python
# %% tags=["parameters"]
config = {{}}

# %% [markdown]
# # {first_build_name.replace('_', ' ').title()}
#
# Orchestrates multiple flows for {domain.lower()}.

# %%
# Ductaflow mandatory header
{import_statement}
if not is_notebook_execution(): # CLI mode only
    config = load_cli_config('config/builds/{first_build_name}.json', '{first_build_name.replace("_", " ").title()}')
# Standardized config unpacking - ductaflow fundamental
vars().update(unpack_config(config, "{first_build_name.replace('_', ' ').title()}", locals()))
# Display config summary for notebook instances
display_config_summary(config, "{first_build_name.replace('_', ' ').title()}")

# %%
from pathlib import Path
from datetime import datetime
import logging

# %%
# Get logger (auto-configured by run_notebook)
logger = logging.getLogger("flow:{first_build_name}")

# Build logic - defined in code, not config
project_root = Path(config.get('_project_root', '.'))
flows_dir = project_root / 'flows'

# Build defines its own execution directory
run_folder = Path("runs/{first_build_name}_{{scenario}}".format(first_build_name=first_build_name))

logger.info("=" * 60)
logger.info("BUILD: {{first_build_name.replace('_', ' ').title()}}".format(first_build_name=first_build_name))
logger.info("Project root: {{project_root}}".format(project_root=project_root))
logger.info("Flows directory: {{flows_dir}}".format(flows_dir=flows_dir))
logger.info("Run folder: {{run_folder}}".format(run_folder=run_folder))
logger.info("=" * 60)

# %% [markdown]
# ## {first_flow_names[0].replace('_', ' ').title()}
#
# {first_flow_names[0].replace('_', ' ').title()} prepares the initial data for {domain.lower()}.

# %%
# Initialize flow-specific config
flow_config = {{**config}}
flow_config['_flow_name'] = '{first_flow_names[0]}'

# Execute flow
start_time = datetime.now()
run_notebook(
    notebook_file=flows_dir / '{first_flow_names[0]}.py',
    config=flow_config,
    execution_dir=run_folder / 'execution' / '{first_flow_names[0]}',
    project_root=project_root,
    export_html=True
)
duration = (datetime.now() - start_time).total_seconds()
logger.info(f"✅ Flow completed: {first_flow_names[0]} | Duration: {{duration:.1f}}s")

# %% [markdown]
# ## {first_flow_names[1].replace('_', ' ').title()}
#
# {first_flow_names[1].replace('_', ' ').title()} processes the prepared data.

# %%
# Initialize flow-specific config
flow_config = {{**config}}
flow_config['_flow_name'] = '{first_flow_names[1]}'

# Execute flow
start_time = datetime.now()
run_notebook(
    notebook_file=flows_dir / '{first_flow_names[1]}.py',
    config=flow_config,
    execution_dir=run_folder / 'execution' / '{first_flow_names[1]}',
    project_root=project_root,
    export_html=True
)
duration = (datetime.now() - start_time).total_seconds()
logger.info(f"✅ Flow completed: {first_flow_names[1]} | Duration: {{duration:.1f}}s")

# %% [markdown]
# ## {first_flow_names[2].replace('_', ' ').title()}
#
# {first_flow_names[2].replace('_', ' ').title()} analyzes and summarizes the results.

# %%
# Initialize flow-specific config
flow_config = {{**config}}
flow_config['_flow_name'] = '{first_flow_names[2]}'

# Execute flow
start_time = datetime.now()
run_notebook(
    notebook_file=flows_dir / '{first_flow_names[2]}.py',
    config=flow_config,
    execution_dir=run_folder / 'execution' / '{first_flow_names[2]}',
    project_root=project_root,
    export_html=True
)
duration = (datetime.now() - start_time).total_seconds()
logger.info(f"✅ Flow completed: {first_flow_names[2]} | Duration: {{duration:.1f}}s")

# %%
    logger.info("{{first_build_name.replace('_', ' ').title()}} completed successfully!")

# %%
```''' if explain_build_level else f'''```python
# %% [markdown]
# # {project_name} Conductor
#
# Orchestrates flows directly across {first_dimension_name} scenarios (single-level approach).

# %%
from pathlib import Path
from datetime import datetime
import logging
import json
{import_statement}

# %%
# Set up logging for conductor
from ductaflow import setup_execution_logging
conductor_log = Path("conductor_execution_output.txt")
logger = setup_execution_logging(conductor_log, "conductor")

# %%
# Base configuration
base_config = {{
    "project_name": "{project_name}",
    "domain": "{domain}"
}}

# %%
# Scenario dimensions
scenario_dimensions = {{
    "{first_dimension_name}": [
        {scenario_config_list}
    ]
}}

scenarios = []
for scenario_config in scenario_dimensions["{first_dimension_name}"]:
    scenario = {{
        **base_config,
        "{first_dimension_name}": scenario_config["value"],
        "scenario_name": scenario_config["label"]
    }}
    scenarios.append(scenario)

logger.info(f"Generated {{len(scenarios)}} scenarios: {{[s['scenario_name'] for s in scenarios]}}")

# %%
# Execute scenarios - calling flows directly
project_root = Path.cwd()
flows_dir = project_root / 'flows'

for scenario in scenarios:
    scenario_name = scenario["scenario_name"]
    
    logger.info("=" * 60)
    logger.info(f"Running Scenario: {{scenario_name}}")
    logger.info("=" * 60)
    
    # Execution directory for this scenario
    execution_dir = Path(f"runs/{{scenario_name}}")
    
    try:
        start_time = datetime.now()
        
        # Execute each flow in sequence
        for flow_name in {first_flow_names}:
            logger.info("🔄 Running flow: " + flow_name)
            
            # Load flow's default config
            flow_config_path = project_root / 'config' / 'flows' / (flow_name + ".json")
            if flow_config_path.exists():
                with open(flow_config_path, 'r') as f:
                    flow_config = json.load(f)
            else:
                flow_config = {{}}
            
            # Add scenario context to config
            flow_config.update({{
                **scenario,
                '_project_root': str(project_root),
                '_flow_name': flow_name
            }})
            
            # Execute flow
            flow_start = datetime.now()
            run_notebook(
                notebook_file=flows_dir / (flow_name + ".py"),
                config=flow_config,
                execution_dir=execution_dir / 'execution' / flow_name,
                project_root=project_root,
                export_html=True
            )
            flow_duration = (datetime.now() - flow_start).total_seconds()
            logger.info(f"✅ Flow completed: {{flow_name}} | Duration: {{flow_duration:.1f}}s")
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Scenario completed: {{scenario_name}} | Duration: {{duration:.1f}}s")
        
    except Exception as e:
        logger.error(f"❌ Scenario failed: {{scenario_name}} | Error: {{str(e)}}")
        raise

logger.info("🎯 All scenarios completed!")

# %%
```'''}

{f'''### 5. Create the Conductor

Create `conductor.py`:

**Important**: The conductor is stored as a `.py` file but is designed to be used as a **notebook** - it's your user interface for running scenarios. Unlike flows, the conductor:
- **Does NOT use the standard config unpacking pattern** (no `unpack_config()` or `load_cli_config()`)
- **Custom config is built directly in cells** - you edit scenario dimensions, base config, and execution logic interactively
- **Opened as a notebook** - run cells individually, modify configs on the fly, and iterate quickly
- **No CLI mode** - it's meant for interactive use, not script execution

This makes it easy to experiment with different scenario combinations, adjust parameters, and debug execution without editing JSON files.

```python
# %% [markdown]
# # {project_name} Conductor
#
# Orchestrates {first_build_name} across {first_dimension_name} scenarios.

# %%
from pathlib import Path
from datetime import datetime
import logging
{import_statement}

# %%
# Set up logging for conductor
from ductaflow import setup_execution_logging
conductor_log = Path("conductor_execution_output.txt")
logger = setup_execution_logging(conductor_log, "conductor")

# %%
# Base configuration
base_config = {{
    "project_name": "{project_name}",
    "domain": "{domain}",
    "run_folder": "{first_build_execution_dir}"
}}

# %%
# Scenario dimensions
scenario_dimensions = {{
    "{first_dimension_name}": [
        {scenario_config_list}
    ]
}}

scenarios = []
for scenario_config in scenario_dimensions["{first_dimension_name}"]:
    scenario = {{
        **base_config,
        "{first_dimension_name}": scenario_config["value"],
        "scenario_name": scenario_config["label"]
    }}
    scenarios.append(scenario)

logger.info(f"Generated {{len(scenarios)}} scenarios: {{[s['scenario_name'] for s in scenarios]}}")

# %%
# Execute scenarios
for scenario in scenarios:
    scenario_name = scenario["scenario_name"]
    
    logger.info("=" * 60)
    logger.info(f"Running Scenario: {{scenario_name}}")
    logger.info("=" * 60)
    
    # Format the execution directory
    execution_dir = scenario["run_folder"].format(
        first_build_name="{first_build_name}",
        scenario_name=scenario_name
    )
    
    scenario_config = {{**scenario, "run_folder": execution_dir}}
    
    try:
        start_time = datetime.now()
        
        run_notebook(
            notebook_file="builds/{first_build_name}.py",
            config=scenario_config,
            execution_dir=execution_dir,
            project_root=Path.cwd(),
            export_html=True
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Build completed: {{scenario_name}} | Duration: {{duration:.1f}}s")
        
    except Exception as e:
        logger.error(f"❌ Build failed: {{scenario_name}} | Error: {{str(e)}}")

logger.info("🎯 All scenarios completed!")

# %%
```''' if explain_build_level else f'''### 5. CLI Usage: Calling Flows Directly

You can call flows directly from the command line using their default config files. This is useful for testing individual flows or running them outside the conductor.

**Call a single flow with its default config:**

```bash
python flows/{first_flow_names[0]}.py --config config/flows/{first_flow_names[0]}.json
```

**Or use ductaflow's `run_step_flow` function programmatically:**

```python
from ductaflow import run_step_flow
import json

# Load the flow's default config
with open('config/flows/{first_flow_names[0]}.json', 'r') as f:
    config = json.load(f)

# Run the flow
run_step_flow(
    notebook_path='flows/{first_flow_names[0]}.py',
    step_name='manual_run',
    instance_name='test_instance',
    config=config
)
```

**Note**: When calling flows directly via CLI, they will use the config file specified with `--config`. If no config is provided, the flow will run in notebook mode expecting config to be injected (e.g., via papermill or interactive execution).
'''}

## Key Patterns You Must Follow

### 1. **Flow Structure** (CRITICAL)

Every flow MUST have this mandatory header:

```python
# %% tags=["parameters"]
config = {{}}

# %%
# Ductaflow mandatory header
{import_statement}
if not is_notebook_execution(): # CLI mode only
    config = load_cli_config('config/flows/my_flow.json', 'My Flow')
# Standardized config unpacking - ductaflow fundamental
vars().update(unpack_config(config, "My Flow Name", locals()))
# Display config summary for notebook instances
display_config_summary(config, "My Flow Name")

# %%
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import logging

# %%
# Get logger (auto-configured by run_notebook)
logger = logging.getLogger("flow:my_flow_name")
logger.info("Starting processing...")
```

**This header**:
- Displays config summary with flow name
- Extracts all config keys as local variables
- Flattens nested dictionaries (e.g., `database.host` becomes `host`)
- Converts common path strings to `Path` objects automatically
- Sets up logging automatically (logger name matches flow filename)

### 2. **Build Logic in Code, Not Config**

Builds should explicitly sequence flows with markdown explainers:

```python
# ✅ Good - explicit flow sequence with documentation
# %% [markdown]
# ## Data Preparation
# Prepares raw data for processing.

# %%
flow_config = {{**config}}
flow_config['_flow_name'] = 'data_prep'
run_notebook(
    notebook_file=flows_dir / 'data_prep.py',
    config=flow_config,
    execution_dir=run_folder / 'execution' / 'data_prep',
    project_root=project_root,
    export_html=True
)

# %% [markdown]
# ## Analysis
# Performs core analysis on prepared data.

# %%
flow_config = {{**config}}
flow_config['_flow_name'] = 'analysis'
run_notebook(
    notebook_file=flows_dir / 'analysis.py',
    config=flow_config,
    execution_dir=run_folder / 'execution' / 'analysis',
    project_root=project_root,
    export_html=True
)

# ❌ Bad - for loop hides flow sequence and makes report less readable
for flow_name in flows_to_run:
    run_notebook(...)
```

### 3. **Filesystem as Schema**

Reference other instances' outputs using paths:

```python
# Access global flow outputs
if '_project_root' in config:
    project_root = Path(config['_project_root'])
    baseline_data = pd.read_csv(project_root / "runs/data_prep/baseline/output.csv")
```

### 4. **Testing Philosophy**

- **No separate test suite** - your instances ARE your tests
- Test flows: `run_step_flow('flows/my_flow.py', 'test', 'instance', config)`
- Debug failures: Open flow as notebook, load exact failing config
- CI: Store one "test instance" in repo

### 5. **Logging Approach**

ductaflow uses Python's standard `logging` module. Every execution automatically creates `{{flow_name}}_execution_output.txt` files.

**Usage in flows:**
```python
import logging

# Get logger (auto-configured by run_notebook with name "flow:{{flow_filename}}")
logger = logging.getLogger("flow:my_flow_name")

logger.info("📊 Starting processing...")
logger.info(f"Processing {{len(data)}} records")
logger.warning("⚠️ Found missing values")
logger.error("❌ Database connection failed")
logger.info("✅ Processing completed")
```

**How it works:**
- `run_notebook()` automatically configures a logger named `"flow:{{flow_filename}}"`
- Logs go to both console (simple format) and file (detailed with timestamps)
- Each execution gets its own isolated logger
- Works in both CLI and notebook execution modes
- Always produces txt files regardless of execution mode

**Search logs:** `Select-String "❌ Failed" runs/**/*_execution_output.txt`

## Development Gotchas

### 1. **Config Parameter Extraction**

Always use the standardized unpacking pattern:

```python
# ✅ Good - standardized unpacking
vars().update(unpack_config(config, "Flow Name", locals()))
# Now all config keys are available as local variables

# ❌ Bad - manual extraction
if 'config' in locals() and config:
    display_config_summary(config, flow_name)
    param1 = config.get("param1", default_value)
    param2 = config.get("param2", default_value)
```

### 2. **Working Directory**

ductaflow changes to the execution directory, so use relative paths:

```python
# ✅ Good - relative to execution directory
df.to_csv("results.csv")

# ❌ Bad - absolute paths break portability  
df.to_csv("/full/path/to/results.csv")
```

### 3. **Error Handling**

Let exceptions bubble up - ductaflow captures the error state:

```python
# ✅ Good - let it fail with context
df = pd.read_csv(input_file)

# ❌ Bad - swallowing errors
try:
    df = pd.read_csv(input_file)
except:
    df = pd.DataFrame()  # Silent failure!
```

## Debugging Workflow

When something fails:

1. **Check the logs**: Look in `runs/*/execution_output.txt` files
2. **Use debug_flow()**: `debug_flow("flows/failed_flow.py", "runs/failed/instance")`
3. **Interactive debugging**: Open flow as notebook, load exact config
4. **Fix and re-run**: Edit flow, then `run_step_flow()` just that step

## Next Steps

1. **Create the directory structure** shown above
2. **Start with one flow** - get the pattern right
3. **Add config file** - test CLI and papermill modes
4. **Build up to full build** - orchestrate multiple flows
5. **Add conductor** - run across scenarios
6. **Test everything** - use your instances as tests

## Remember

- **Filesystem IS the schema** - paths encode dependencies
- **Every execution creates debugging environment** - exact config + logs + artifacts
- **Use logging module** - `logger.info()`, `logger.warning()`, `logger.error()` automatically saved to files
- **Instances are tests** - no separate test suite required
- **Re-executable everything** - fix bugs by re-running just the failed step

Good luck building {project_name}! 🚀

---

*This guide was generated by `generate_project_guide.py` on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""

    return guide

def main():
    """Main function to generate project guide."""
    config_file = sys.argv[1] if len(sys.argv) > 1 else "project_config.json"
    
    print(f"🚀 Generating project guide from {config_file}...")
    
    config = load_project_config(config_file)
    guide = generate_project_guide(config)
    
    output_file = f"{config['project_name']}_HOWTO.md"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(guide)
    
    print(f"✅ Generated: {output_file}")
    print(f"📊 Project: {config['project_name']}")
    print(f"🏗️ Build: {config['first_build_name']}")
    print(f"🔄 Flows: {', '.join(config['first_flow_names'])}")
    print(f"📋 Scenarios: {list(config['scenario_dimensions'].keys())[0]} = {config['scenario_dimensions'][list(config['scenario_dimensions'].keys())[0]]}")

if __name__ == "__main__":
    main()
