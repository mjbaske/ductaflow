# %% [markdown]
# # Ductaflow Conductor
#
# This template demonstrates the step-based pattern discovered through real-world implementation.
# Shows general data pipeline patterns without domain-specific complexity.

# %%
import sys
import os
from pathlib import Path
from datetime import datetime
import json
import copy

# Always add reference directory
sys.path.append('code')

try:
    from ductacore import run_notebook
    print("✅ ductaflow framework loaded")
except ImportError as e:
    print(f"❌ Error: {e}")

# %% [markdown]
# ## Step-Based Pattern (RECOMMENDED)

# %%
def run_step_flow(notebook_path, step_name, instance_name, config):
    """
    STEP-BASED PATTERN - Execute notebook with step-based instance management
    
    Pattern: runs/{step_name}/{instance_name}/
    Benefits: Pipeline composition, dependency tracking, multiple variations
    """
    
    # 1. Create step-based directory structure
    step_dir = Path("runs") / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    
    output_dir = step_dir / instance_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Save configuration
    config_path = output_dir / "CONFIG.json"
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"🚀 Executing Step: {step_name}")
    print(f"📋 Instance: {instance_name}")
    print(f"📁 Output: {output_dir}")
    
    notebook_path = Path(notebook_path).resolve()

    # 3. Execute notebook with execution directory (ductacore handles directory switching)
    executed_notebook = run_notebook(
        notebook_file=notebook_path,
        config=config,
        output_suffix="_executed",
        execution_dir=output_dir
    )
    
    print(f"✅ Completed: {executed_notebook}")
    return executed_notebook, output_dir

# %%
def get_available_instances(step_name):
    """
    Get available instances for a given step
    Returns list of instance names for dependency selection
    """
    step_dir = Path("runs") / step_name
    if not step_dir.exists():
        return []
    
    instances = [d.name for d in step_dir.iterdir() if d.is_dir()]
    return sorted(instances)

def print_available_instances(step_name, description=""):
    """Display available instances with helpful formatting"""
    instances = get_available_instances(step_name)
    if instances:
        print(f"\n📋 Available {step_name} instances{description}:")
        for i, instance in enumerate(instances, 1):
            # Try to get additional info from config
            config_path = Path("runs") / step_name / instance / "CONFIG.json"
            info = ""
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        config = json.load(f)
                    # Add any relevant config info to display
                    if 'data_sources' in config:
                        data_source_count = len(config['data_sources'])
                        info = f" - {data_source_count} data sources"
                    elif 'processing_mode' in config:
                        info = f" - {config['processing_mode']} mode"
                except:
                    pass
            print(f"   {i}. {instance}{info}")
    else:
        print(f"\n⚠️  No {step_name} instances found")

def print_pipeline_status():
    """Display current pipeline state and dependencies"""
    runs_dir = Path("runs")
    if not runs_dir.exists():
        print("📊 No pipeline runs found")
        return
    
    print("\n📊 Pipeline Status:")
    steps = [d.name for d in runs_dir.iterdir() if d.is_dir()]
    for step in sorted(steps):
        instances = get_available_instances(step)
        print(f"   {step}: {len(instances)} instances")
        for instance in instances:
            print(f"      - {instance}")

# %% [markdown]
# ## General Data Pipeline Example
# 
# This demonstrates a typical 3-step data processing pipeline:
# 1. Data preparation/ingestion
# 2. Data processing/transformation  
# 3. Analysis/reporting

# %%
# Load control dataframe for systematic instance generation
import pandas as pd
import os
import shutil

control_df = pd.DataFrame({
    'instance_name': ['NET_2025_OPT1', 'NET_2025_OPT2_TWB', 'NET_2026_TAU'],
    'Network_Base': ['NET_2024_BASE', 'NET_2024_BASE', 'NET_2025_BASE'], 
    'Network_New': ['NET_2025_OPT1', 'NET_2025_OPT2_TWB', 'NET_2026_TAU'],
    'client_group': ['transport', 'transport', 'planning'],
    'project': ['SEQ_Modelling', 'SEQ_Modelling', 'future_networks'],
    'scenario_type': ['optimization', 'twb_variant', 'tau_baseline']
})

print("📊 Control Dataframe:")
print(control_df)

# %%
# Iterate through dataframe to systematically generate instances
for i in range(len(control_df)):
    print(f"\n🚀 Processing instance {i+1}/{len(control_df)}")
    print(control_df.iloc[i])
    
    # Extract row parameters
    base_scenario = control_df.iloc[i]['Network_Base']
    target_scenario = control_df.iloc[i]['Network_New']
    instance_name = control_df.iloc[i]['instance_name']
    
    # Conditional logic based on scenario name (your example pattern)
    if "TWB" in target_scenario:
        cfn = 'Common_TWB'
    else:
        cfn = 'Common_TAU'
    
    # Build configuration from row
    scenario_config = {
        "base_scenario": base_scenario,
        "target_scenario": target_scenario,
        "common_function": cfn,
        "scenario_type": control_df.iloc[i]['scenario_type'],
        "processing_options": {
            "export_format": "parquet",
            "include_diagnostics": True
        }
    }
    
    # Execute flow step
    run_step_flow(
        notebook_path="flow/scenario_analysis.py",
        step_name="scenario_analysis", 
        instance_name=instance_name,
        config=scenario_config
    )
    
    # Export to client delivery structure (your pattern)
    source_dir = f"./runs/scenario_analysis/{instance_name}"
    dest_base_dir = "/mnt/n/SEQ_Modelling/#NETWORKS_4_6/Scenarios/SEQ/"
    dest_dir = os.path.join(dest_base_dir, str(target_scenario))
    
    # Remove existing folder if it exists
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
        print(f"🗑️ Removed existing directory: {dest_dir}")
    
    # Copy the entire directory tree
    if os.path.exists(source_dir):
        shutil.copytree(source_dir, dest_dir)
        print(f"✅ Copied {source_dir} to {dest_dir}")
    else:
        print(f"⚠️ Warning: Source directory {source_dir} does not exist")

# %%
# Show complete pipeline status after dataframe-driven generation
print_pipeline_status()
print(f"\n✅ Generated {len(control_df)} instances systematically from dataframe")


# %% [markdown]
# ## Configuration Reuse Pattern (inline, no functions)

# %%
# Create multiple analysis variants using copy.deepcopy
analysis_variants = ["summary", "detailed", "executive"]

for variant in analysis_variants:
    # Create variant configuration inline
    variant_config = copy.deepcopy(base_analysis_config)
    variant_config.update({
        "report_level": variant,
        "chart_settings": {
            "width": 1200 if variant == "detailed" else 800,
            "height": 800 if variant == "detailed" else 600,
            "theme": "professional" if variant == "executive" else "default"
        }
    })
    
    # Execute with descriptive instance name
    run_step_flow(
        notebook_path="flow/03_analyze.py",
        step_name="analysis",
        instance_name=f"{variant}_report",
        config=variant_config
    )

print("✅ Multiple analysis reports created using configuration reuse")

# %% [markdown]
# ## Alternative Processing Pipeline

# %%
# Create alternative processing pipeline for comparison
alt_processing_config = copy.deepcopy(processing_config)
alt_processing_config.update({
    "aggregation_level": "weekly",  # Different aggregation
    "metrics": ["sum", "count"],    # Different metrics
    "experimental_features": True
})

run_step_flow(
    notebook_path="flow/02_process_data.py",
    step_name="process_data",
    instance_name="weekly_aggregates",
    config=alt_processing_config
)

# Create analysis using the alternative processing
alt_analysis_config = copy.deepcopy(base_analysis_config)
alt_analysis_config.update({
    "input_instance": "weekly_aggregates",  # Reference alternative processing
    "report_title": "Weekly Analysis Report"
})

run_step_flow(
    notebook_path="flow/03_analyze.py",
    step_name="analysis", 
    instance_name="weekly_report",
    config=alt_analysis_config
)



# %%
