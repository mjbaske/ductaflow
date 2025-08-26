# %% tags=["parameters"]


# %% [markdown]
# # Flow Shell
#
# This flow provides a shell for describing some sequential process on any given config instance. 
# Note: config json is always passed as a parameter and any key value at level zero or one gets created as a local var in the executed notebook

# %%
# Import required libraries and display configuration
import sys
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import numpy as np

# Add code paths to assuming a ductaflow/runs/{instance_name}/flow.ipynb output execution path
# need to ensure the output flow has access to your ductaflow code folder otherwise

sys.path.append('../../code')

# Import pipeline framework for configuration display
try:
    from ductacore import display_config_summary
    # Display the injected configuration if available
    if 'config' in locals():
        display_config_summary(config, "[flow Name]")
    else:
        print("⚠️  No configuration found - running in standalone mode")
except ImportError:
    print("Pipeline framework not available - using simple parameter display")
    if 'config' in locals():
        print("Configuration:", json.dumps(config, indent=2, default=str))
    else:
        print("⚠️  No configuration available")

# %%
# Extract all config parameters as local variables (standardized unpacking)
if 'config' in locals() and config:
    for key, value in config.items():
        if isinstance(value, dict):
            # For nested dictionaries, flatten the keys
            for nested_key, nested_value in value.items():
                locals()[nested_key] = nested_value
                print(f"📋 {nested_key} = {nested_value}")
        else:
            # For simple key-value pairs
            locals()[key] = value
            print(f"📋 {key} = {value}")
else:
    print("⚠️  No config to unpack - please ensure config is properly injected")

print(f"🔍 [flow Name] initialized")
print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# %% [markdown]
# ## [Section 1: Main Processing Logic]

# %%
print("Starting main processing...")

# TODO: Add your main processing logic here
# Example structure:

print(config)
for i in range(3):
    print(i)


# %%
# Export key variables for potential pipeline use (standardized approach)
flow_results = {
    "flow_name": "[flow_name]",
    "config_loaded": 'config' in locals(),
    "execution_timestamp": datetime.now().isoformat(),
    "success": True  # Set to False if any issues occurred
}

print(f"🎯 [flow Name] completed successfully!" if flow_results["success"] else "⚠️  [flow Name] completed with issues")

# %%
