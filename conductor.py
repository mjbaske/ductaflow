# %% [markdown]
# # DUCTAFLOW Conductor - Multi-Scenario Orchestrator
# 
# The conductor orchestrates builds across scenario dimensions.
# It takes a base configuration and varies it across specified dimensions,
# running builds for each scenario combination.
# 
# **Architecture**:
# - Conductor calls builds over outer scenario dimensions
# - Builds orchestrate flows in sequence
# - Flows are atomic processing stages

# %%
from pathlib import Path
import json
from datetime import datetime
from itertools import product
import copy

# Import ductaflow utilities
from ductaflow import run_notebook, analyze_execution_logs, generate_status_report

# %% [markdown]
# ## Base Configuration

# %%
# Model root and base configuration
model_root = Path("./runs/conductor_runs")

# Base configuration (shared across all scenarios)
base_config = {
    "model_root": str(model_root),
    "processing_mode": "standard",
    "iterations": 10,
    "output_format": "csv",
    "analysis_type": "summary",
    "threshold": 50.0
}

# %% [markdown]
# ## Scenario Dimensions
# 
# Define the dimensions to vary across scenarios.
# Each dimension is a list of values to explore.

# %%
# Define scenario dimensions to explore
scenario_dimensions = {
    "processing_mode": [
        {"processing_mode": "standard", "label": "Standard"},
        {"processing_mode": "enhanced", "label": "Enhanced"}
    ],
    "analysis_type": [
        {"analysis_type": "summary", "label": "Summary"},
        {"analysis_type": "filter", "label": "Filter"}
    ],
    "threshold": [
        {"threshold": 30.0, "label": "Low"},
        {"threshold": 70.0, "label": "High"}
    ]
}

# Select which dimensions to actually run (for testing/partial runs)
active_dimensions = ["processing_mode"]  # Can be subset of all dimensions

print(f"Available scenario dimensions: {list(scenario_dimensions.keys())}")
print(f"Active dimensions for this run: {active_dimensions}")

# %% [markdown]
# ## Scenario Generation

# %%
def generate_scenarios(dimensions_dict, active_dims):
    """Generate all combinations of active scenario dimensions"""
    active_values = [dimensions_dict[dim] for dim in active_dims]
    
    scenarios = []
    for combination in product(*active_values):
        scenario = {}
        labels = []
        
        for i, dim_name in enumerate(active_dims):
            dim_config = combination[i]
            # Merge dimension config (excluding label)
            for key, value in dim_config.items():
                if key != "label":
                    scenario[key] = value
            labels.append(dim_config["label"])
        
        scenario["scenario_name"] = f"Scenario_{'_'.join(labels)}"
        scenarios.append(scenario)
    
    return scenarios

# Generate scenarios
scenarios = generate_scenarios(scenario_dimensions, active_dimensions)

print(f"\nGenerated {len(scenarios)} scenarios:")
for i, scenario in enumerate(scenarios, 1):
    print(f"  {i}. {scenario['scenario_name']}")

# %% [markdown]
# ## Scenario Execution

# %%
results = []

for scenario in scenarios:
    scenario_name = scenario["scenario_name"]
    
    print(f"\n{'='*80}")
    print(f"Running Scenario: {scenario_name}")
    print(f"{'='*80}")
    
    # Create scenario-specific config by merging base + scenario
    scenario_config = {
        **base_config,
        **scenario,
        "run_folder": f"runs/{scenario_name}"
    }
    
    # Output directory for this scenario
    output_dir = model_root / "Model_Runs" / scenario_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save scenario config for reproducibility
    config_path = output_dir / "config.json"
    with open(config_path, 'w') as f:
        json.dump(scenario_config, f, indent=2, default=str)
    
    print(f"Output directory: {output_dir}")
    print(f"Config saved: {config_path}")
    
    # Display scenario-specific parameters
    scenario_params = {k: v for k, v in scenario.items() if k != "scenario_name"}
    if scenario_params:
        print(f"Scenario parameters: {scenario_params}")
    
    # Run the build
    try:
        start_time = datetime.now()
        
        run_notebook(
            notebook_file="builds/build_shell.py",  # Build orchestrates flows
            config=scenario_config,
            execution_dir=output_dir,
            project_root=Path.cwd(),  # Inject project root for flexible execution
            export_html=True
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()  # seconds
        
        # Simple logging - just print statements (automatically captured to files)
        print(f"✅ Build completed: {scenario_name} | Duration: {duration:.1f}s | Path: {output_dir}")
        
        print(f"\n✓ Scenario {scenario_name} completed successfully!")
        print(f"Duration: {duration:.1f} seconds")
        
        results.append({
            "scenario": scenario_name,
            "status": "success",
            "duration_minutes": duration / 60,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            **scenario_params
        })
        
    except Exception as e:
        # Simple logging - just print statements (automatically captured to files)
        print(f"❌ Build failed: {scenario_name} | Error: {str(e)} | Path: {output_dir}")
        
        print(f"\n❌ Scenario {scenario_name} failed!")
        print(f"Error: {e}")
        
        results.append({
            "scenario": scenario_name,
            "status": "failed",
            "error": str(e),
            **scenario_params
        })
    
    # Generate live status report after each build completes
    print(f"\n📊 Updating live status report...")
    report_html = generate_status_report(results, model_root)
    live_report_file = Path("runs/conductor_status_report.html")
    live_report_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(live_report_file, 'w', encoding='utf-8') as f:
        f.write(report_html)
    
    print(f"📈 Live report updated: {live_report_file}")
    print(f"   Open in browser to see current red/orange/green status")

# %% [markdown]
# ## Results Summary

# %%
print(f"\n{'='*80}")
print("CONDUCTOR EXECUTION SUMMARY")
print(f"{'='*80}")

successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "failed"]

print(f"Total scenarios: {len(results)}")
print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

if successful:
    total_time = sum(r["duration_minutes"] for r in successful)
    avg_time = total_time / len(successful)
    print(f"Total execution time: {total_time:.1f} minutes")
    print(f"Average time per scenario: {avg_time:.1f} minutes")

print(f"\nDetailed results:")
for result in results:
    status_icon = "✓" if result["status"] == "success" else "❌"
    duration_str = f" ({result.get('duration_minutes', 0):.1f}min)" if result["status"] == "success" else ""
    print(f"  {status_icon} {result['scenario']}{duration_str}")

# Save results summary
results_file = model_root / "conductor_results.json"
with open(results_file, 'w') as f:
    json.dump({
        "execution_timestamp": datetime.now().isoformat(),
        "base_config": base_config,
        "scenario_dimensions": {dim: scenario_dimensions[dim] for dim in active_dimensions},
        "results": results
    }, f, indent=2, default=str)

print(f"\nResults saved to: {results_file}")

# %% [markdown]
# ## Generate Detailed Status Report
# 
# Simple pattern: Add this after your conductor execution to get nested status reports

# %%
# Pattern for any conductor: Add these lines after build execution
# The functions automatically analyze execution logs for status indicators

# Generate final comprehensive report  
report_html = generate_status_report(results, model_root)
report_file = Path("runs/conductor_status_report.html")
with open(report_file, 'w', encoding='utf-8') as f:
    f.write(report_html)

print(f"📊 Final status report generated: {report_file}")
print(f"   Open in browser to see expandable build/flow status")

# %%