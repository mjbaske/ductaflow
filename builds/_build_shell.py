# %% tags=["parameters"]
config = {}

# %% [markdown]
# # Build Shell
#
# Demonstrates orchestrating flows with dependencies.
# Shows Flow Shell 1 → Flow Shell 2 dependency pattern.

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
# Build logic - defined in code, not config
project_root = Path(config.get('_project_root', '.'))
flows_dir = project_root / 'flows'

# Build defines its own execution directory and dependencies
run_folder = Path(f"runs/_build_shell_{scenario}_{year}")
flows_to_run = ["_flow_shell_2"]  # Build decides which flows to run

print(f"{'=' * 60}")
print(f"BUILD: Build Shell")
print(f"Project root: {project_root}")
print(f"Flows directory: {flows_dir}")
print(f"Run folder: {run_folder}")
print(f"Scenario: {scenario}")
print(f"{'=' * 60}")

# %% [markdown]
# ## Execute Flows

# %%
print(f"\n{'=' * 40}")
print("EXECUTING FLOWS")
print(f"{'=' * 40}")

# First run Flow Shell 1 to create data
print(f"\n🔄 Running Flow Shell 1 (creates data)")
try:
    start_time = datetime.now()
    
    flow_1_execution_dir = run_folder / "runs" / "flow_shell_1_instance"
    run_notebook(
        notebook_file=flows_dir / "_flow_shell_1.py",
        config=config,
        execution_dir=flow_1_execution_dir,
        project_root=project_root,
        export_html=True
    )
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"✅ Flow Shell 1 completed | Duration: {duration:.1f}s")
    
    # Flow Shell 2 can access Flow Shell 1 outputs using simple paths
    # No setup needed - just reference the upstream flow instance directory
    
except Exception as e:
    print(f"❌ Flow Shell 1 failed | Error: {str(e)}")
    raise

# Now run Flow Shell 2 with dependencies
for flow_name in flows_to_run:
    print(f"\n🔄 Running flow: {flow_name}")
    
    try:
        start_time = datetime.now()
        
        run_notebook(
            notebook_file=flows_dir / f"{flow_name}.py",
            config=config,
            execution_dir=run_folder / "runs" / f"{flow_name}_instance",
            project_root=project_root,
            export_html=True
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        print(f"✅ Flow completed: {flow_name} | Duration: {duration:.1f}s")
        
    except Exception as e:
        print(f"❌ Flow failed: {flow_name} | Error: {str(e)}")
        raise

print(f"\n🎯 Build Shell completed successfully!")

# %%
