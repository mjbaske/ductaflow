# %% tags=["parameters"]
# Parameters cell - will be injected by papermill
config = {}


# %% [markdown]
# # Flow Shell
#
# This flow provides a shell for describing some sequential process on any given config instance. 
# Note: config json is always passed as a parameter and any key value at level zero or one gets created as a local var in the executed notebook

# %% CLI Mode - Same file works as notebook AND script
from ductaflow import is_notebook_execution, load_cli_config, display_config_summary

if not is_notebook_execution():
    # CLI mode: load config from --config argument
    config = load_cli_config('config/flow_shell.json', 'Run Flow Shell analysis')

# %%
# Import required libraries and display configuration
import sys
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import numpy as np

# Import ductaflow (works after pip install -e .)
pipeline_name = 'Flow Shell'

# %%
# Extract all config parameters as local variables (standardized unpacking)
if 'config' in locals() and config:
    display_config_summary(config, pipeline_name)
    
    # Extract config variables as locals (clean, scoped approach)
    for key, value in config.items():
        if isinstance(value, dict):
            locals()[key] = value
            for sub_key, sub_value in value.items():
                locals()[sub_key] = sub_value
                print(f"📋 {sub_key} = {sub_value}")
        else:
            locals()[key] = value
            print(f"📋 {key} = {value}")
else:
    print("⚠️ No config found - run with --config file.json or via papermill injection")

print(f"🔍 [flow Name] initialized")
print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# %% [markdown]
# ## [Section 1: Main Processing Logic]

# %%
print("Starting main processing...")

# TODO: Add your main processing logic here
# Example structure:

print(config)
output=0
for i in range(3):
    output+=i

pd.Series(output).to_csv("output.csv") # Notebooks are executed in the output folder so it can be placed at current directory.

# %%
# Export key variables for potential pipeline use (standardized approach)
flow_results = {
    "flow_name": pipeline_name,
    "config_loaded": 'config' in locals(),
    "execution_timestamp": datetime.now().isoformat(),
    "success": True  # Set to False if any issues occurred
}

print(f"🎯 {pipeline_name} completed successfully!" if flow_results["success"] else "⚠️  [flow Name] completed with issues")

# %%
