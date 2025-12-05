# %% tags=["parameters"]
config = {}

# %% [markdown]
# # Flow Shell 1
#
# Generic atomic flow for demonstration purposes.
# Flows are atomic stages that can be called directly or orchestrated by builds.

# %% 
# Ductaflow mandatory header
from ductaflow import is_notebook_execution, load_cli_config, unpack_config, display_config_summary
if not is_notebook_execution(): # CLI mode only
    config = load_cli_config('config/flows/_flow_shell_1.json', 'Flow Shell 1')
# Standardized config unpacking - ductaflow fundamental
vars().update(unpack_config(config, "Flow Shell 1", locals()))
# Display config summary for notebook instances
display_config_summary(config, "Flow Shell 1")

# %%
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
# %%
print("📊 Starting Flow Shell 1 processing...")

# Generic processing logic
results = []
for i in range(iterations):
    value = np.random.random() * 100
    results.append({
        'iteration': i + 1,
        'value': value,
        'processing_mode': processing_mode
    })
    print(f"🔄 Iteration {i + 1}: {value:.2f}")

# Save results
df = pd.DataFrame(results)
if output_format == "csv":
    df.to_csv("_flow_shell_1_results.csv", index=False)
elif output_format == "parquet":
    df.to_parquet("_flow_shell_1_results.parquet", index=False)

print(f"✅ Flow Shell 1 completed - {len(results)} iterations processed")

# %%
