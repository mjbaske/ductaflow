# %% tags=["parameters"]
config = {}

# %% [markdown]
# # Flow Shell 2
#
# Demonstrates how flows can access dependencies set up by builds.
# This flow depends on outputs from Flow Shell 1.

# %% 
# Ductaflow mandatory header
from ductaflow import is_notebook_execution, load_cli_config, unpack_config, load_flow_dependency_pointer, display_config_summary
if not is_notebook_execution(): # CLI mode only
    config = load_cli_config('config/flows/_flow_shell_2.json', 'Flow Shell 2')
# Standardized config unpacking - ductaflow fundamental
vars().update(unpack_config(config, "Flow Shell 2", locals()))
# Display config summary for notebook instances
display_config_summary(config, "Flow Shell 2")





# %%
import pandas as pd
from pathlib import Path
import json
from datetime import datetime



# %%
print("📊 Starting Flow Shell 2 - dependency access demonstration...")

# %% [markdown]
# ## Access Upstream Flow Outputs
# 
# Simple pattern: reference upstream flow instance directories directly

# %%
# Example: Access Flow Shell 1 outputs using simple path
flow_shell_1_path = Path("../flow_shell_1_instance")
if flow_shell_1_path.exists():
    print(f"📂 Found upstream flow: {flow_shell_1_path}")
    
    # List available files
    available_files = list(flow_shell_1_path.glob("*"))
    print(f"📋 Available files: {[f.name for f in available_files]}")
    
    # Access CSV results from Flow Shell 1
    csv_file = flow_shell_1_path / "_flow_shell_1_results.csv"
    if csv_file.exists():
        print(f"📊 Loading results from Flow Shell 1: {csv_file.name}")
        df_upstream = pd.read_csv(csv_file)
        print(f"📈 Loaded {len(df_upstream)} rows from upstream flow")
    else:
        print("⚠️ Expected results file not found")
        df_upstream = pd.DataFrame()
else:
    print("⚠️ Upstream flow 'flow_shell_1_instance' not found")
    df_upstream = pd.DataFrame()

# %% [markdown]
# ## Processing with Dependencies

# %%
print("🔄 Processing Flow Shell 2 with upstream data...")

# Process the upstream data if available
if not df_upstream.empty:
    # Example processing: calculate summary statistics
    summary_stats = {
        "total_iterations": len(df_upstream),
        "mean_value": df_upstream['value'].mean() if 'value' in df_upstream.columns else 0,
        "max_value": df_upstream['value'].max() if 'value' in df_upstream.columns else 0,
        "processing_mode": df_upstream['processing_mode'].iloc[0] if 'processing_mode' in df_upstream.columns else "unknown"
    }
    
    print(f"📈 Processed {summary_stats['total_iterations']} upstream iterations")
    print(f"📊 Mean value: {summary_stats['mean_value']:.2f}")
    print(f"📊 Max value: {summary_stats['max_value']:.2f}")
else:
    summary_stats = {"note": "No upstream data available"}
    print("⚠️ No upstream data to process")

# Create Flow Shell 2 results
results = {
    "flow_name": "Flow Shell 2",
    "execution_timestamp": datetime.now().isoformat(),
    "upstream_data_processed": not df_upstream.empty,
    "upstream_summary": summary_stats,
    "processing_complete": True
}

# Save results
with open("_flow_shell_2_results.json", 'w') as f:
    json.dump(results, f, indent=2)

print(f"✅ Flow Shell 2 completed")
print(f"📊 Upstream data processed: {'Yes' if not df_upstream.empty else 'No'}")

# %%
