# ductaflow
**🚀 The pipeline framework that actually works in practice.**

Stop wrestling with complex orchestration tools and brittle notebook chains. ductaflow uses **inline code in .py flows** that make sequential steps easily interpretable by domain experts, with **full artifact capture** and the ability to debug any flow by executing a single instance as an interactive notebook. 

**🌊 Flow Pattern:** Use dataframe iteration to systematically generate hundreds of instances from parameter combinations, with results naturally organized in `runs/{flow_name}/{instance_name}/`.

**🔄 Easily Re-executable:** .py flows can be run as scripts OR opened as notebooks for debugging. Fix a bug → re-run just that step. Change parameters → modify dataframe and re-execute. Git-friendly .py files with bulletproof reproducibility.

## 🚀 Quick Setup

```bash
# Install as package
pip install -e .

# Use in any notebook/script
from ductaflow import run_notebook, display_config_summary
```

**Core Structure:**
```
your_project/
├── flow/                    # Analysis steps (.py notebooks)
├── runs/                    # Execution results  
├── conductor.py             # Main orchestration notebook
└── config/                  # JSON configurations
```

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
# Run single flow from command line
python flow/my_analysis.py --config config/my_config.json
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
        '--config', str(config_file)
    ], cwd=run_dir)
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
- **Reproducibility**: Full execution state captured
- **Parameterization**: Easy variations via config changes
- **Debugging**: Failed executions saved with error state
- **Modularity**: Individual steps developed/tested independently
- **🔄 Easy Re-execution**: Interactive notebook experience for iteration

## 🔄 Why Re-executability Matters

**The Problem:** Traditional pipeline tools make iteration painful - change one parameter, restart everything.

**The ductaflow Solution:** Everything is a notebook, everything is re-executable:

- **🐛 Bug fixes:** Fix flow → re-run cell → updated results
- **📊 Parameter changes:** Modify dataframe → re-run loop → only changed instances execute  
- **🆕 Add scenarios:** Add dataframe rows → run new cells → extend analysis
- **🔧 Client requests:** Modify config → re-execute → deliver results in minutes

No complex DAG management, no pipeline restart headaches.

## 🎯 Key Pattern: Dataframe-Driven Orchestration

**Use control dataframe where each row = one analysis instance:**

```python
# %% 
# Control dataframe with instance parameters
control_df = pd.DataFrame({
    'instance_name': ['scenario_A', 'scenario_B', 'scenario_C'],
    'network_base': ['NET_2024', 'NET_2024', 'NET_2025'], 
    'network_new': ['NET_2025_OPT1', 'NET_2025_OPT2', 'NET_2026'],
    'param1': [100, 200, 150],
    'param2': ['mode_A', 'mode_B', 'mode_A']
})

# %% 
# Iterate through dataframe to generate instances
for i in range(len(control_df)):
    row = control_df.iloc[i]
    
    # Build config from row values
    config = {
        'network_base': row['network_base'],
        'network_new': row['network_new'], 
        'processing_params': {
            'param1': row['param1'],
            'param2': row['param2']
        }
    }
    
    # Execute flow with config
    run_notebook(
        notebook_file="flow/network_analysis.py",
        config=config
    )
```

**Result:** Systematic generation of analysis instances, all trackable and reproducible.

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
if __name__ == "__main__":
    import argparse, json
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/base.json')
    args = parser.parse_args()
    
    # Load and inject config
    with open(args.config) as f:
        config = json.load(f)
    
    # Inject variables into globals
    for key, value in config.items():
        if isinstance(value, dict):
            globals()[key] = value
            for sub_key, sub_value in value.items():
                globals()[sub_key] = sub_value
        else:
            globals()[key] = value
```

### 3. Config Display (optional but helpful)
```python
# %%
from ductaflow import display_config_summary
display_config_summary(config, "My Analysis")
```

### 4. Your Analysis Code
```python
# %%
# Your actual analysis using config variables
print(f"Processing {network_base} → {network_new}")
# ... rest of your analysis
```

## Path Context

**CRITICAL:** Notebooks execute from `runs/{flow_name}/{instance_name}/`

All relative paths must account for this:
- Code imports: `sys.path.append('../../../code')`  
- Input data: `../../../inputs/data.csv`
- Previous outputs: `../../other_flow/instance/output.parquet`

## Installation Options

```bash
# Basic install
pip install -e .

# With HTML export support (optional)
pip install -e .[html]    # Adds nbconvert for HTML reports
```

**JSON configs only** - Simple, built into Python, no extra dependencies.