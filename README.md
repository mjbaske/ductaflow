# ductaflow
**🚀 The pipeline framework that actually works in practice.**

Stop wrestling with complex orchestration tools and brittle notebook chains. ductaflow lets you build reproducible pipelines using **notebooks all the way down** - your analysis steps are Jupytext .py files, and your conductor is a Jupytext .py file too. **Want results fast?** append your re-useable methods and objects in `code/` → capture common required sequential steps in a `flow/` → open `conductor.py` as a interactive notebook → run cells to loop or chain flows, grid search over dimensions etc, or do whatever you want interactively → every execution gets saved to `runs/{flow_name}/{instance_name}` with full state and capture, configs, and HTML reports. 

**🌊 Flow Pattern:** Let your conductor orchestrate streams of analysis - use dataframe iteration to systematically flow hundreds of instances from parameter combinations downstream, with results naturally deposited in client directories like `{target_dir}/{client_group}/{project}/{instance_name}`. 

**🔄 Easily Re-executable:** Since everything is a notebook (.py file), you can re-run any part at any time - fix a bug in your flow, re-run just that step; client wants different parameters, modify the control dataframe and re-execute; need to add new scenarios, just add rows and run the new cells. Pure notebook experience with git-friendly .py files and bulletproof reproducibility. Perfect for data science, ML experiments, and any analysis where you need results to flow reliably from source to destination.

## 🚀 Quick Setup: Copy Template

```python
from ductacore import make_new_ductaflow_instance

# Copy entire ductaflow template to new project
make_new_ductaflow_instance("my_analysis")

# Result: Complete ductaflow copy ready to customize
# my_analysis/
# ├── code/                    # ductacore utilities (copied)
# ├── flow/                    # Example flows (copied)  
# ├── runs/                    # Ready for results
# ├── cnd_my_analysis.py       # Renamed conductor
# ├── README.md               # This documentation
# └── ductaflow_version.txt   # Git commit info
```

**Simple template copy:**
- Copies entire working ductaflow
- Renames conductor with your project name  
- Captures git version for reproducibility
- Ready to customize and run

## 🔄 **Multiple Execution Options - Your Choice**

**🚫 Don't like notebooks?** No problem! ductaflow works EXACTLY like traditional Python scripts too.

### **Option 1: Interactive Notebooks (Jupyter/VS Code)**
```python
# Open flow/my_analysis.py as notebook
# Run cells interactively
# Variables come from config injection
```

### **Option 2: Traditional Python Scripts**
```python
# Create a clean Python run script 
from ductacore import create_flow_run_script
create_flow_run_script('flow/my_analysis.py')

# Now run like any Python script:
python my_analysis_run.py
```

### **Option 3: Standalone Python Script (when needed)**
```python
# Convert flow to pure Python script if you need it
from ductacore import create_standalone_python_script
create_standalone_python_script('flow/my_analysis.py')

# Result: my_analysis_standalone.py + my_analysis_config.json
# Run like any Python script:
python my_analysis_standalone.py
```

**When this is useful:**
- **Sharing with colleagues** who don't have jupyter/papermill installed
- **Running on servers** without notebook environments  
- **Batch processing** where you want simple command-line execution
- **CI/CD pipelines** that prefer standard Python scripts

### **Option 4: Temporary Script Runner**
```python
# Create a bat file for quick one-off runs
from ductacore import create_flow_bat_runner
create_flow_bat_runner('flow/my_analysis.py')

# Double-click run_my_analysis.bat or call from conductor
# Creates temp script → runs → cleans up automatically
```

**Handy for:** Quick execution without cluttering your workspace with script files.

**🎯 The Point:**
- **Same `.py file`** works as notebook OR script - your choice
- **Same config injection** regardless of how you run it
- **Start with notebooks** for exploration, convert to scripts when needed
- **Most of the time** you'll just use the interactive notebook experience


## Core dependencies: 
    ## papermill
    ## jupytext
    Open source python is a true blessing on the world.


## Core Concepts

The pipeline management pattern works as follows:

1. **Notebooks as .py files**: All analysis steps are written as Jupytext .py files (percent format)
2. **Configuration-driven**: Each execution instance uses a JSON config file for parameters
3. **Step-based isolation**: Each flow step gets its own directory with named instances for pipeline composition
4. **Execution artifacts**: Executed notebooks are saved with full output for debugging/review
5. **Pipeline orchestration**: Multiple flows are chained together via a conductor script with instance referencing

## Key Benefits

- **Version Control**: .py files work seamlessly with git
- **Reproducibility**: Full execution state captured in artifacts
- **Parameterization**: Easy to create variations via config changes
- **Debugging**: Failed executions saved with error state
- **Modularity**: Individual pipeline steps can be developed and tested independently
- **Scalability**: Can be extended to run on different compute environments
- **🔄 Easy Re-execution**: Interactive notebook experience means you can re-run any part anytime

## 🔄 Why Re-executability Matters

**The Problem:** Traditional pipeline tools make it painful to iterate - you change one parameter and have to restart everything, or you fix a bug and can't easily re-run just the affected parts.

**The ductaflow Solution:** Everything is a notebook, so everything is easily re-executable:

- **🐛 Bug fixes:** Fix a flow step → re-run just that cell in your conductor → updated results flow downstream
- **📊 Parameter changes:** Modify your control dataframe → re-run the iteration loop → only new/changed instances execute  
- **🆕 Add scenarios:** Add new rows to your dataframe → run just those new cells → seamlessly extend your analysis
- **🔧 Client requests:** "Can you change the threshold?" → modify config → re-execute → deliver updated results in minutes
- **🧪 Experimentation:** Try different approaches interactively, compare results, keep what works

**No complex DAG management, no pipeline restart headaches** - just the natural flow of interactive notebooks with reproducibility.

## 🎯 Key Pattern: Dataframe-Driven Instance Generation in the conductor

**The Constraint:** You want to generate dozens/hundreds of analysis instances systematically, not manually.

**The Solution:** Use a control dataframe in your conductor notebook where each row defines an instance:

```python
# %% 
# Control dataframe with instance parameters
control_df = pd.DataFrame({
    'instance_name': ['scenario_A', 'scenario_B', 'scenario_C'],
    'network_base': ['NET_2024', 'NET_2024', 'NET_2025'], 
    'network_new': ['NET_2025_OPT1', 'NET_2025_OPT2', 'NET_2026'],
    'client_group': ['transport', 'transport', 'planning'],
    'project': ['SEQ_analysis', 'SEQ_analysis', 'future_networks'],
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
    
    # Function to ducatacore.run_notebook to execute the current flow
    run_step_flow(
        notebook_path="flow/network_analysis.py",
        step_name="network_analysis", 
        instance_name=row['instance_name'],
        config=config
    )
    
    # Export to client directory structure
    source_dir = f"./runs/network_analysis/{row['instance_name']}"
    dest_dir = f"/client_delivery/{row['client_group']}/{row['project']}/{row['instance_name']}"
    
    if os.path.exists(source_dir):
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
        print(f"✅ Exported {row['instance_name']} to {dest_dir}")
```

**Result:** Systematic generation + organized delivery to clients, all trackable and reproducible.

## 🚀 Quick Setup: Copy Template

```python
from ductacore import make_new_ductaflow_instance

# Copy entire ductaflow template to new project
make_new_ductaflow_instance("my_analysis")

# Result: Complete ductaflow copy ready to customize
# my_analysis/
# ├── code/                    # ductacore utilities (copied)
# ├── flow/                    # Example flows (copied)  
# ├── runs/                    # Ready for results
# ├── cnd_my_analysis.py       # Renamed conductor
# ├── README.md               # This documentation
# └── ductaflow_version.txt   # Git commit info
```

**Simple template copy:**
- Copies entire working ductaflow
- Renames conductor with your project name  
- Captures git version for reproducibility
- Ready to customize and run

#### note 1. Parameters Cell in your flow.py's (REQUIRED)
```python
# %% tags=["parameters"]
# Parameters cell - will be injected by papermill
config = {}
# Pattern: Bulk processor coordinates multiple flow instances
for time_period in ["am", "pm"]:
    for category in categories:
        for zone_system in ["L2", "L4", "SCRAM"]:
            # Generate vector inputs instance
            vector_config = create_vector_config(time_period, category, zone_system)
            run_step_flow("generate_vector_inputs", f"{zone_system}_{category}_{time_period}", vector_config)
            
            # Generate visualization instance
            viz_config = create_viz_config(vector_instance=f"{zone_system}_{category}_{time_period}")
            run_step_flow("generate_stock_view", f"view_{zone_system}_{category}_{time_period}", viz_config)

# Meta-flow combines multiple instances
run_step_flow("generate_bulk_index", "combined_outputs", {
    "source_instances": get_all_instances("generate_stock_view"),
    "collated_output_path": "./runs/bulk_outputs/"
})
```
**Critical**: This cell MUST be tagged with `["parameters"]` for papermill execution. Without this cell, ductaflow execution will fail.

#### note 2. Add this to drop need for config['{name}'] and just have name of object in code
Configuration instance value handling cell
```python
# %%
# Configuration handling - ductaflow pattern
if 'config' in locals() and config:
    for key, value in config.items():
        if isinstance(value, dict):
            locals()[key] = value
            for sub_key, sub_value in value.items():
                locals()[sub_key] = sub_value
        else:
            locals()[key] = value
```
**Key Benefits:**
- **Mass Production**: Generate hundreds of analysis variants systematically
- **Smart Caching**: Skip existing instances to enable incremental processing  
- **Collated Outputs**: Centralized organization of bulk results with indexes
- **Meta-Composition**: Flows that consume outputs from multiple other flow instances

#### note 3. Code is for pys that arent a module yet but need to add it to path
```python
# %%
import sys
# Add code directory to path for imports (executed from runs/step_name/instance_name/)
sys.path.append('../../../code')
from your_modules import your_functions
**Directory Structure:**
```

### Path Context Awareness

**CRITICAL**: Ductaflow notebooks execute from the context: `runs/{step_name}/{instance_name}/`

The `run_step_flow` function in conductor.py:
1. Changes to the output directory: `os.chdir(output_dir)` 
2. Calls `run_notebook()` from `runs/{step_name}/{instance_name}/`
3. **Papermill executes your notebook from this directory - no further `os.chdir()` needed!**

**All relative paths must account for this execution context:**
- Code imports: `sys.path.append('../../../code')`  
- Input data: `../../../inputs/your_data.csv`
- Previous step outputs: `../../previous_step_name/instance_name/output.parquet`

**Do NOT use `os.chdir()` in notebooks** - you're already in the correct execution directory!

### Configuration Pattern

- Config dict gets passed and all keys that don't have a nested dict as their value become local variables
- Record of config.json always stored in run folder
- Config values displayed as markdown at top of executed notebook
- Nested dictionary handling preserves parent dictionaries while flattening child keys

## Original Notes
- config dict always gets passed and all keys that dont have a nested dict as their value become local variables in the executed instance
- record of config.json always stored in run folder
- config values displayed as markdown at top of executed notebook
- if you have a custom output location you will need to have ductacore accesible to python so need to update the sys.path.append('../../../code') line in the flow you are making





## Advanced Orchestration Patterns

### Flow-of-Flows (Bulk Processing)
For complex scenarios requiring many combinations of parameters/datasets, ductaflow supports **bulk orchestration**:

```python
# Pattern: Bulk processor coordinates multiple flow instances
for time_period in ["am", "pm"]:
    for category in categories:
        for zone_system in ["L2", "L4", "SCRAM"]:
            # Generate vector inputs instance
            vector_config = create_vector_config(time_period, category, zone_system)
            run_step_flow("generate_vector_inputs", f"{zone_system}_{category}_{time_period}", vector_config)
            
            # Generate visualization instance
            viz_config = create_viz_config(vector_instance=f"{zone_system}_{category}_{time_period}")
            run_step_flow("generate_stock_view", f"view_{zone_system}_{category}_{time_period}", viz_config)

# Meta-flow combines multiple instances
run_step_flow("generate_bulk_index", "combined_outputs", {
    "source_instances": get_all_instances("generate_stock_view"),
    "collated_output_path": "./runs/bulk_outputs/"
})
```

**Key Benefits:**
- **Mass Production**: Generate hundreds of analysis variants systematically
- **Smart Caching**: Skip existing instances to enable incremental processing  
- **Collated Outputs**: Centralized organization of bulk results with indexes
- **Meta-Composition**: Flows that consume outputs from multiple other flow instances

**Directory Structure:**
```
runs/
├── generate_vector_inputs/          # Atomic step instances
│   ├── L2_dwellings_am/
│   ├── L2_dwellings_pm/
│   ├── L4_employment_am/
│   └── SCRAM_trips_am/
├── generate_stock_view/             # Dependent instances
│   ├── view_L2_dwellings_am/
│   └── view_L4_employment_am/
└── generate_bulk_index/             # Meta-composition instances
    └── combined_outputs/
        ├── index.html               # Collated navigation
        └── bulk_summary.json        # Processing metadata
```


