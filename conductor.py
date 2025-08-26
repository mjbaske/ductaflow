# %% [markdown]
# # Core Execution Pattern - Minimal Example
#
# This shows the absolute essential pattern extracted from run_network_pipeline_notebook_simple.py
# for executing any notebook with configuration.

# %%
import sys
import os
from pathlib import Path
from datetime import datetime
import json

# Always add reference directory
sys.path.append('code')

try:
    from ductacore import run_notebook
    print("✅ ductaflow framework loaded")
except ImportError as e:
    print(f"❌ Error: {e}")

# %% [markdown]
# ## Describe One Instance of Calling a flow.py notebook as a Function

# %%
def run_flow(notebook_path, run_config, run_name=None):
    """
    THE CORE PATTERN - Execute any notebook with configuration
    
    This is the essential pattern from the original pipeline:
    1. Create config with timestamp
    2. Create output directory  
    3. Save config to JSON
    4. Execute notebook with run_notebook()
    5. Handle directory switching
    """
    
    # 1. Generate timestamp and run name
    timestamp = datetime.now().strftime("%Y%m%d_%H")
    if not run_name:
        run_name = f"run_{timestamp}"
    
    # 2. Create run directory
    run_dir = Path("runs")
    run_dir.mkdir(parents=True, exist_ok=True)
    
    output_dir = run_dir / f"{run_name}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. Save configuration Should be able to just inspect the config
    config_path = output_dir / "_config.json"
    with open(config_path, 'w') as f:
        json.dump(run_config, f, indent=2)
    
    print(f"🚀 Executing: {run_name}")
    print(f"📁 Output: {output_dir}")
    
    notebook_path = Path(notebook_path).resolve()

    try:
        # 4. Execute notebook (directory switching pattern to make it so re-running notebook from output location is same)
        original_dir = os.getcwd()
        os.chdir(output_dir)
        
        executed_notebook = run_notebook(
            notebook_file=notebook_path,
            config=run_config,
            notebooks_dir=original_dir,
            output_suffix="_executed",
            timeout=1800
        )
        
        print(f"✅ Completed: {executed_notebook}")
        return executed_notebook, output_dir
        
    finally:
        # 5. Always return to original directory
        os.chdir(original_dir)

# %% [markdown]
# # Iterate or Chain any Flows

# %%
for i in range(3):
    config = {'the_number':i}
    executed_nb, output_dir = run_flow(
        notebook_path="flow/_flow_shell.py",
        run_config=config,
        run_name=f"my_run_{i}",
    )


print("🎯 Core execution pattern ready!")
print("📋 Pattern: config → directory → run_notebook → cleanup")

# %%
