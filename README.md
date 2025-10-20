# ductaflow
**🚀 The simple pipeline framework.**

Stop wrestling with complex orchestration tools and brittle notebook chains. ductaflow uses **readable .py flows** with **full artifact capture** and the ability to debug any flow as an interactive notebook.

## 🏗️ Architecture: 3-Level System

**ductaflow uses a simple 3-level hierarchy:**

### 📄 **Flows** (Atomic Processing Steps)
- **What**: Individual `.py` files that do one specific thing
- **Examples**: `data_cleaning.py`, `model_training.py`, `report_generation.py`
- **Runs**: Independently as scripts or notebooks
- **Purpose**: Single-responsibility processing units

### 🔗 **Builds** (Flow Orchestration)
- **What**: `.py` files that sequence and execute multiple flows
- **Examples**: `full_model_pipeline.py`, `daily_report_build.py`
- **Runs**: Calls flows in specific order with dependency management
- **Purpose**: Define how flows connect and share data

### 🎯 **Conductor** (Multi-Scenario Orchestration)
- **What**: Top-level script that runs builds across scenario dimensions
- **Examples**: `conductor.py` running 50 scenarios × 3 model variants = 150 builds
- **Runs**: Manages parameter combinations and parallel execution
- **Purpose**: Scale builds across parameter spaces

**Example hierarchy:**
```
Conductor → Build "Full Pipeline" → Flow "Data Prep" 
                                 → Flow "Model Training"
                                 → Flow "Results Export"
```

## Core Philosophy

**🗂️ Filesystem as Schema:** Your directory structure IS your data pipeline. Configs reference other instances' outputs using simple f-strings: `f"runs/data_prep/{scenario}/results.csv"`.

**🔄 Everything is Re-executable:** .py flows run as scripts OR notebooks. Fix a bug → re-run just that step. Change parameters → re-execute only what changed.

**📊 Dataframe Orchestration:** Use pandas DataFrames to systematically generate hundreds of instances from parameter combinations.

## Logging

**ductaflow logging philosophy**: Just use `print` statements. No custom logging libraries needed.

### How It Works

- **All `print` statements** automatically saved to `{flow_name}_execution_output.txt` files
- **No distinction** between logs and command output - everything is captured
- **Cross-platform**: Works on Windows, Linux, macOS without setup
- **Simple text analysis**: Pipeline status determined by scanning `{flow_name}_execution_output.txt` of every build and flow

### Status Detection

**🟢 Green Light** (Success): Python executed successfully + no warning indicators found

**🟠 Orange Light** (Warning): Python executed successfully + warning indicators found  

**🔴 Red Light** (Error): Python raised exception and halted execution

### Warning Detection

**Exactly what gets flagged as warnings** (first 15 characters of any line):

```python
# These WILL be detected as warnings:
print("⚠️ Warning: data issue")     # ⚠️ symbol
print("Warning: missing file")      # "warning:" (any case)
print("WARNING: deprecated")        # "WARNING:" 
print("warning: low memory")        # "warning:" (lowercase)

# These will NOT be detected (beyond 15 characters):
print("Processing data with warning messages inside")  # "warning" at position 20+
df.to_csv("warning_analysis.csv")   # not at line start
```

**Exact search patterns** (case-insensitive, first 15 chars only):
- `⚠️` (warning emoji)
- `warning:` (any capitalization)

**Why this works**: 
- ✅ **Success**: Python completed → no exceptions → no warning patterns found
- ⚠️ **Warning**: Python completed → no exceptions → warning patterns found  
- ❌ **Error**: Python raised exception → execution halted → automatic red status

### Live Status Reports

**After each build completes**, the conductor generates/updates a live HTML status report:
- **Location**: `runs/conductor_status_report.html` (in project root)
- **Updates**: Regenerated after each scenario completes
- **View**: Open in browser to see real-time red/orange/green status
- **Expandable**: Click builds to see individual flow status

**Pattern for any conductor**:
```python
# After each build execution
from ductaflow import generate_status_report

report_html = generate_status_report(results, Path("runs"))
with open("runs/conductor_status_report.html", 'w') as f:
    f.write(report_html)
```

**No logging setup needed** - just print what you need to see, and ductaflow handles the rest.



## Testing

**ductaflow testing philosophy**: Tests are just instances you have. No separate test suite needed.

**Testing a flow**: `run_step_flow('flows/my_flow.py', 'test_step', 'test_instance', config)` or use `--no-execute` to set up environment without running.

**Testing a build**: Run the build. When a flow fails, open that flow as an interactive notebook and debug directly. Fix → re-run just that flow.

**CI/Testing**: Store a single "test instance" in your repo if you need CI. Run it. If it passes, ship it.

**The real test**: Your actual production runs. The execution logs tell you everything - which flows succeeded, which failed, how long they took. No ceremony, just results.

## Debugging Workflow

**When a build fails**:
1. Check the build log - see which flow failed
2. Open `flows/failed_flow.py` as interactive notebook  
3. Load the exact config that failed: `config = json.load(open('runs/build_name/instance_name/failed_flow_config.json'))`
4. Run cells step by step until you find the issue
5. Fix the flow
6. Re-run just that flow: `run_step_flow('flows/failed_flow.py', 'step_name', 'instance_name', config)`

**The power**: Every execution creates a complete debugging environment. No "works on my machine" - you have the exact config, exact environment, exact failure state.

**IDE Debugging**: Use `debug_flow("flows/my_flow.py", "runs/debug/session")` to set up traditional step-through debugging. Your IDE debugger runs the actual flow instance with breakpoints and full debugging control.


### How It Works

**Built into `run_notebook()`**: The ductaflow `run_notebook()` function automatically captures all stdout/stderr to `_execution_output.txt` files while still showing output in the console.

**CLI mode**: When you run flows directly from command line, `load_cli_config()` captures output to `_cli_execution_output.txt`.

```bash
# Every flow execution automatically creates:
runs/flow_name/instance_name/flow_name.ipynb        # Notebook artifact  
runs/flow_name/instance_name/flow_name_execution_output.txt           # All print statements (via run_notebook)

# CLI execution creates:
./_cli_execution_output.txt                                  # All print statements (via CLI)
```

### No User Action Required

- **Builds calling flows**: Output automatically captured via `run_notebook()`
- **CLI execution**: Output automatically captured via `load_cli_config()`
- **Print statements**: Just use `print()` - everything is captured
- **Cross-platform**: Works on Windows, Linux, macOS

### Searching Logs

```bash
# Search across all runs (Windows PowerShell)
Select-String "❌ Failed" runs\**\*_execution_output.txt     # Find failures
Select-String "✅ Completed.*Time:" runs\**\*_execution_output.txt | Measure-Object | %{$_.Count}  # Count successes
Select-String "⚠️" runs\**\*_execution_output.txt           # Find warnings

# Cross-platform Python searching
python -c "import glob; [print(f) for f in glob.glob('runs/**/*_execution_output.txt', recursive=True) if '❌ Failed' in open(f).read()]"
```

**Key insight**: Just use `print()` statements in your flows. The logging happens automatically through ductaflow's execution functions.

## 🚀 Quick Setup

### Option 1: Install as Package (Recommended)
```bash
# Install as package
pip install -e .

# Use in any notebook/script
from ductaflow import run_notebook, display_config_summary, debug_flow
```

### Option 2: Single File Copy
If you prefer not to pip install, just copy `ductaflow/ductaflow.py` as `ductaflow.py` into your project root:

```python
# Then import directly (no path setup needed)
from ductaflow import run_notebook, display_config_summary, debug_flow
```

**File structure**:
```
your_project/
├── ductaflow.py           # Copied from ductaflow/ductaflow.py
├── flows/
├── builds/
└── config/
```

**Core Structure:**
```
your_project/
├── flows/                   # Atomic steps (.py notebooks)
├── builds/                  # some sequence of steps (.py notebooks)
├── runs/                    # Example Execution Directory  
├── conductor.py             # Main orchestration notebook of builds over steps
└── config/                  # JSON configurations
```

**Config Convention**: Each flow/build has a matching config file:
- `flows/data_prep.py` ↔ `config/flows/data_prep.json`
- `builds/pipeline.py` ↔ `config/builds/pipeline.json`

## 🔄 **Three Ways to Run: Your Choice**

### **📓 Interactive Mode (Debugging & Exploration)**
```python
# Open conductor.py as notebook in VS Code/Jupyter
# Run cells interactively with dataframe orchestration 
# Debug any flow by opening flow/analysis.py as notebook
# Execute single instances interactively for troubleshooting
```

### **💻 CLI Mode (Individual Flows)**
```bash
# Run single flow from command line with output directory
python flow/my_analysis.py --config config/my_config.json --output-dir runs/analysis/my_run
```

### **🐍 Pure Python Mode (Anti-Notebook)**
```python
# pure_conductor.py - No notebooks, just subprocess calls
import subprocess, pandas as pd, json
from pathlib import Path

# Load scenarios
scenarios = [
    {'instance': 'test_A', 'param1': 100},
    {'instance': 'test_B', 'param1': 200}
]

for scenario in scenarios:
    # Create run directory
    run_dir = Path(f"runs/analysis/{scenario['instance']}")
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # Save config
    config_file = run_dir / "config.json"
    with open(config_file, 'w') as f:
        json.dump(scenario, f)
    
    # Run flow as script (uses if __name__ == "__main__")
    subprocess.run([
        'python', 'flow/my_analysis.py', 
        '--config', str(config_file),
        '--output-dir', str(run_dir)
    ])
```

**🎯 The Point:** Same flows, same results - whether you love notebooks, prefer CLI, or want pure Python scripts.

## Core Dependencies
- **papermill** - Execute notebooks programmatically
- **jupytext** - .py ↔ .ipynb conversion
- **pandas** - Dataframe orchestration

Open source Python is a true blessing on the world.

## Core Concepts

1. **Interpretable .py flows**: Analysis steps written as readable .py files with inline code that domain experts can understand
2. **Configuration-driven**: Each execution uses JSON config for parameters
3. **Instance isolation**: Each run gets `runs/{flow_name}/{instance_name}/` directory with full artifact capture
4. **Debug as notebooks**: Any .py flow can be opened as interactive notebook for troubleshooting
5. **Flexible execution**: Run as scripts for production, notebooks for debugging, pure Python for anti-notebook users

## Key Benefits

- **Version Control**: .py files work seamlessly with git
- **Reproducibility**: Full execution state captured + automatic config saving
- **Parameterization**: Easy variations via config changes
- **Debugging**: Failed executions saved with error state
- **Modularity**: Individual steps developed/tested independently
- **🔄 Easy Re-execution**: Interactive notebook experience for iteration
- **Robust Execution**: Same flows work as notebooks, CLI scripts, or pure Python calls

## 🔄 Why Re-executability Matters

**The Problem:** Traditional pipeline tools make iteration painful - change one parameter, restart everything.

**The ductaflow Solution:** Everything is a notebook, everything is re-executable:

- **🐛 Bug fixes:** Fix flow → re-run cell → updated results
- **📊 Parameter changes:** Modify dataframe → re-run loop → only changed instances execute  
- **🆕 Add scenarios:** Add dataframe rows → run new cells → extend analysis
- **🔧 Client requests:** Modify config → re-execute → deliver results in minutes

No complex DAG management, no pipeline restart headaches.



## 🗂️ Filesystem as Schema

**Core Principle:** The filesystem IS the schema. Configs reference other instances' outputs using f-strings, creating natural dependency graphs.

```python
# %% Config references other instances' outputs
scenario = "scenario_A"
baseline = "baseline_run"

config = {
    "input_data": f"runs/data_prep/{scenario}/processed_data.csv",
    "baseline_results": f"runs/analysis/{baseline}/results.json", 
    "param1": 100
}

# The filesystem structure IS your data pipeline schema:
# runs/data_prep/scenario_A/processed_data.csv  ← Instance output
# runs/analysis/scenario_A/results.json         ← References the above
# runs/models/scenario_A/predictions.parquet    ← References both
```

**Why this works:** File paths encode dependencies. Instance isolation through directories. Easy debugging by tracing filesystem paths.

## 📁 Project Context Injection

**Problem**: Builds need to find flows when executing in different directories.

**Solution**: ductaflow automatically injects `_project_root` into configs:

```python
# In builds/my_build.py
project_root = Path(config['_project_root'])  # Auto-injected by ductaflow
flows_dir = project_root / 'flows'            # Always finds flows
inputs_dir = project_root / 'inputs'          # Always finds inputs

# Execute flows using project-relative paths
run_notebook(
    notebook_file=flows_dir / "01_data_prep.py",
    config=config,
    execution_dir=anywhere,  # Can execute anywhere!
    project_root=project_root
)
```

**Conductor usage**:
```python
# Conductor can execute builds anywhere
run_notebook(
    notebook_file="builds/get_traffic_demands.py",
    execution_dir="/totally/different/path/runs/scenario_A",
    project_root=Path.cwd(),  # Injects project root
    config=config
)
```

This enables flexible execution while maintaining access to project structure.

## 🔗 Flow Dependencies in Builds

**Super simple pattern**: Match output paths to input paths using flow instance names.

### Primary Mode: Relative Path Dependencies

**In builds**: Flows execute in directories like `runs/build_name/build_instance/runs/flow_instance_name/`

**In downstream flows**: Access upstream outputs using relative paths:

```python
# Flow Shell 2 accessing Flow Shell 1 outputs
upstream_data = pd.read_csv("../flow_shell_1_instance/output.csv")
results = json.load(open("../flow_shell_1_instance/results.json"))
```

**That's it!** No helper functions, no dependency dictionaries, no setup - just use relative paths.

### Global Access When Needed

**Rare case**: Access project-level flows or inputs using `_project_root` from config:

```python
# Access global flows or project inputs (uncommon)
if '_project_root' in config:
    project_root = Path(config['_project_root'])
    global_data = pd.read_csv(project_root / "runs/data_prep/baseline/output.csv")
    reference_file = project_root / "inputs/reference_data.xlsx"
```

### Example Pattern

```python
# In build: Define execution directories
flow_1_dir = run_folder / "runs" / "flow_shell_1_instance"  
flow_2_dir = run_folder / "runs" / "flow_shell_2_instance"

# Run Flow 1 first
run_notebook("flows/_flow_shell_1.py", config=config, execution_dir=flow_1_dir)

# Run Flow 2 - it can access Flow 1 outputs using ../flow_shell_1_instance/
run_notebook("flows/_flow_shell_2.py", config=config, execution_dir=flow_2_dir)
```

**Key principle**: Use f-strings and relative paths to connect upstream flow outputs. Keep it simple!

## Flow Structure

### 1. Parameters Cell (REQUIRED)
```python
# %% tags=["parameters"]
# Parameters cell - injected by papermill
config = {}
```

### 2. CLI Mode Block (for dual functionality)
```python
# %% CLI Mode - Same file works as notebook AND script
from ductaflow import is_notebook_execution, load_cli_config

if not is_notebook_execution():
    # CLI mode: load config from --config argument
    config = load_cli_config('config/base.json', 'Run my analysis')
```

### 4. Standardized Config Unpacking (FUNDAMENTAL)
```python
# %%
from ductaflow import unpack_config

# Extract all config as local variables - ductaflow fundamental
vars().update(unpack_config(config, "My Flow Name", locals()))
```

**This single line**:
- Displays config summary with flow name
- Extracts all config keys as local variables
- Flattens nested dictionaries (e.g., `database.host` becomes `host`)
- Converts common path strings to `Path` objects automatically
- Prints all extracted variables for transparency


## Execution Options

### Interactive Mode (Notebook)
```bash
# Open flow as notebook in Jupyter/VS Code
code flow/my_analysis.py  # Opens as notebook
```

### CLI Mode (Script)
```bash
# Uses default config (defined in flow)
python flows/my_flow.py

# Or specify custom config
python flows/my_flow.py --config config/experiment_1.json

# CLI with output directory (matches ductaflow behavior)
python flows/my_flow.py --output-dir runs/analysis/experiment_1
```

**Config Convention**: Each flow specifies its default config path:
- `flows/data_prep.py` defaults to `config/flows/data_prep.json`
- `builds/pipeline.py` defaults to `config/builds/pipeline.json`

### Programmatic Mode (ductaflow)
```python
from ductaflow import run_step_flow, run_notebook
from pathlib import Path

# Simple step flow
run_step_flow(
    notebook_file="flows/my_analysis.py",
    step_name="analysis",
    instance_name="experiment_1", 
    config=config
)

# Flexible execution with project root injection
run_notebook(
    notebook_file="builds/my_build.py",
    execution_dir="/any/path/runs/scenario_A",  # Execute anywhere!
    project_root=Path.cwd(),  # Injects _project_root into config
    config=config
)
```

## Installation

```bash
# Basic install
pip install -e .

# With HTML export support (optional)
pip install -e .[html]    # Adds nbconvert for HTML reports
```

**JSON configs only** - Simple, built into Python, no extra dependencies.