# %% [markdown]
# # Ductaflow Conductor Template
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

    try:
        # 3. Execute notebook with directory switching
        original_dir = os.getcwd()
        os.chdir(output_dir)
        
        executed_notebook = run_notebook(
            notebook_file=notebook_path,
            config=config,
            notebooks_dir=original_dir,
            output_suffix="_executed",
            timeout=1800
        )
        
        print(f"✅ Completed: {executed_notebook}")
        return executed_notebook, output_dir
        
    finally:
        # 4. Always return to original directory
        os.chdir(original_dir)

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
# Step 1: Data Preparation
data_prep_config = {
    "input_file": "raw_data/source_data.csv",
    "output_format": "parquet",
    "validation_rules": {
        "check_nulls": True,
        "check_duplicates": True,
        "date_format": "%Y-%m-%d"
    },
    "processing_options": {
        "normalize_text": True,
        "handle_missing": "interpolate"
    }
}

print("🎯 STEP 1: Data Preparation")
run_step_flow(
    notebook_path="flow/01_data_prep.py",
    step_name="data_prep", 
    instance_name="cleaned_dataset",
    config=data_prep_config
)

# Show available instances for the next step
print_available_instances("data_prep", " (prepared datasets)")

# %%
# Step 2: Data Processing (referencing Step 1 instance)
processing_config = {
    "input_instance": "cleaned_dataset",  # Reference to runs/data_prep/cleaned_dataset/
    "aggregation_level": "daily",
    "metrics": ["mean", "median", "std"],
    "groupby_columns": ["category", "region"],
    "filters": {
        "date_range": ["2024-01-01", "2024-12-31"],
        "min_threshold": 10
    }
}

print("\n🎯 STEP 2: Data Processing")
run_step_flow(
    notebook_path="flow/02_process_data.py",
    step_name="process_data",
    instance_name="daily_aggregates", 
    config=processing_config
)

print_available_instances("process_data", " (processed datasets)")

# %%
# Step 3: Analysis & Reporting (referencing Step 2 instance)

# Create base analysis configuration inline
base_analysis_config = {
    "input_instance": "daily_aggregates",  # Reference to Step 2
    "output_format": "html",
    "chart_settings": {
        "width": 800,
        "height": 600,
        "theme": "default"
    },
    "report_sections": ["summary", "trends", "comparisons"]
}

# Run main analysis
print("\n🎯 STEP 3: Analysis & Reporting")
run_step_flow(
    notebook_path="flow/03_analyze.py",
    step_name="analysis",
    instance_name="main_report",
    config=base_analysis_config
)

# Show complete pipeline status
print_pipeline_status()

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
print("🎯 Ductaflow step-based pattern ready!")
print("\n📋 Key Patterns Demonstrated:")
print("  ✅ Step-based instance management")
print("  ✅ Instance discovery and dependency tracking") 
print("  ✅ Inline configuration (no functions needed)")
print("  ✅ Configuration reuse with copy.deepcopy()")
print("  ✅ Pipeline composition and alternatives")
print("\n🚀 Ready to build robust, composable data pipelines!")

# %%
