#!/usr/bin/env python3
"""
Pure Python Conductor Example - No Notebooks Required

This shows how anti-notebook users can orchestrate ductaflow pipelines
using pure Python scripts and the CLI mode of flows.
"""

import subprocess
import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime


def run_flow_as_script(flow_path, step_name, instance_name, config):
    """
    Run a ductaflow .py file as a pure Python script
    
    Uses the if __name__ == "__main__" CLI mode built into flows
    """
    # Create step-based directory structure
    step_dir = Path("runs") / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    
    output_dir = step_dir / instance_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save config file for the flow to read
    config_path = output_dir / "config.json"
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"🚀 Executing Step: {step_name}")
    print(f"📋 Instance: {instance_name}")
    print(f"📁 Output: {output_dir}")
    
    # Change to output directory and run flow as script
    original_cwd = os.getcwd()
    try:
        os.chdir(output_dir)
        
        # Run the flow as a Python script with config
        result = subprocess.run([
            'python', 
            str(Path(original_cwd) / flow_path),
            '--config', 
            str(config_path)
        ], check=True, capture_output=True, text=True)
        
        print(f"✅ Completed: {step_name}/{instance_name}")
        print(f"📄 Output: {result.stdout}")
        
        return output_dir
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed: {step_name}/{instance_name}")
        print(f"Error: {e.stderr}")
        raise
    finally:
        os.chdir(original_cwd)


def get_available_instances(step_name):
    """Get available instances for a given step"""
    step_dir = Path("runs") / step_name
    if not step_dir.exists():
        return []
    return [d.name for d in step_dir.iterdir() if d.is_dir()]


def main():
    """
    Example orchestration using pure Python + CLI flows
    
    This replicates conductor.py functionality without any notebooks
    """
    print("🐍 Pure Python Conductor - No Notebooks!")
    print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Example: Load control dataframe (like conductor.py does)
    control_data = [
        {'instance_name': 'scenario_A', 'param1': 100, 'param2': 'mode_A'},
        {'instance_name': 'scenario_B', 'param1': 200, 'param2': 'mode_B'},
        {'instance_name': 'scenario_C', 'param1': 150, 'param2': 'mode_A'},
    ]
    control_df = pd.DataFrame(control_data)
    
    print(f"📊 Processing {len(control_df)} scenarios")
    
    # Step 1: Run data preparation for each scenario
    for _, row in control_df.iterrows():
        config = {
            'data_source': f"data/{row['instance_name']}.csv",
            'param1': row['param1'],
            'param2': row['param2'],
            'processing_params': {
                'method': 'standard',
                'iterations': 100
            }
        }
        
        run_flow_as_script(
            flow_path="flow/_flow_shell.py",  # Your actual flow
            step_name="data_prep",
            instance_name=row['instance_name'],
            config=config
        )
    
    # Step 2: Chain dependent analysis step
    prep_instances = get_available_instances("data_prep")
    print(f"🔗 Found {len(prep_instances)} prep instances for analysis")
    
    for instance in prep_instances:
        analysis_config = {
            'prep_instance': instance,
            'analysis_type': 'full',
            'output_format': 'html'
        }
        
        run_flow_as_script(
            flow_path="flow/_flow_shell.py",  # Your analysis flow
            step_name="analysis",
            instance_name=instance,
            config=analysis_config
        )
    
    print("✅ Pure Python orchestration complete!")
    print(f"📁 Results in: runs/")


if __name__ == "__main__":
    main()
