# ductaflow
A simple, useful, model instance execution pattern.
This framework provides a robust system for creating and managing pipelines of code using Jupytext .py files that are executed as notebooks with full state capture and record keeping.

## Core Concept

The pipeline management pattern works as follows:

1. **Notebooks as .py files**: All analysis steps are written as Jupytext .py files (percent format)
2. **Configuration-driven**: Each execution instance uses a JSON config file for parameters
3. **Step-based isolation**: Each flow step gets its own directory with named instances for pipeline composition
4. **Execution artifacts**: Executed notebooks are saved with full output for debugging/review
5. **Pipeline orchestration**: Multiple flows are chained together via a conductor script with instance referencing

## Key Benefits

- **Version Control**: .py files work seamlessly with git
- **Reproducibility**: Full execution state captured in artifacts
- **Parameterization**: Easy to create variations via config changes
- **Debugging**: Failed executions saved with error state
- **Modularity**: Individual pipeline steps can be developed and tested independently
- **Scalability**: Can be extended to run on different compute environments

## Directory Structure

### Original Pattern
```
ductaflow/
├── code/                                # Reference code and utilities
│   ├── ductacore.py                     # Core functions of ductaflow
├── flow/                                # Source .py notebook files
│   ├── 01_data_prep.py
│   ├── 02_analysis.py  
│   └── 03_outputs.py
└── runs/                                # Execution instances
    ├── run_20250826_09/
    │   ├── CONFIG.json                  # Run configuration
    │   ├── 01_data_prep_executed.ipynb  # Execution artifacts
    │   └── outputs/                     # Generated outputs
└── conductor.py                         # Pipeline orchestration
```

### Improved Step-Based Pattern (Recommended)
```
ductaflow/
├── code/
│   ├── ductacore.py
├── flow/                                # Source .py notebook files
│   ├── 01_create_point_grid.py
│   ├── 02_generate_vector_inputs.py
│   ├── 03_generate_stock_view.py
│   └── 04_generate_stock_difference_view.py
├── runs/                                # Step-based execution instances
│   ├── create_point_grid/               # Step 1 instances
│   │   ├── main_grid/                   # Named instance
│   │   │   ├── CONFIG.json
│   │   │   ├── 01_create_point_grid_executed.ipynb
│   │   │   └── point_grid.parquet       # Step outputs
│   │   └── high_resolution_grid/        # Alternative instance
│   ├── generate_vector_inputs/          # Step 2 instances
│   │   ├── sc_seql2_multi_vectors/      # Named instance
│   │   │   ├── CONFIG.json
│   │   │   ├── stock_data.parquet
│   │   │   ├── zone_totals.parquet
│   │   │   └── 02_generate_vector_inputs_executed.ipynb
│   │   └── scram_analysis/              # Alternative instance
│   └── generate_stock_view/             # Step 3 instances
│       ├── l2_view/
│       └── l4_view/
├── example_configs/                     # Configuration templates
│   ├── 01_create_point_grid_example.json
│   └── 02_generate_vector_inputs_example.json
└── conductor.py                         # Pipeline orchestration with instance discovery

## Implementation Lessons & Recommendations

### Key Patterns Discovered

#### 1. Step-Based Instance Management
**Pattern**: `runs/{step_name}/{instance_name}/`
- **Benefits**: Enables pipeline composition, clear dependency tracking, multiple variations
- **Usage**: Reference previous outputs via `f'../../{step_name}/{instance_name}/output.parquet'`
- **Recommendation**: Always use descriptive instance names (`main_grid`, `sc_seql2_multi_vectors`)

#### 2. Conductor Script Best Practices
```python
# Instance discovery and status reporting
def get_available_instances(step_name):
    """Show available instances for dependency selection"""
    
def print_pipeline_status():
    """Display current pipeline state and dependencies"""

# Configuration reuse patterns
base_config = {...}
specific_config = copy.deepcopy(base_config)
specific_config.update({...})
```

#### 3. Configuration Management Improvements
- **Nested Dict Handling**: Preserve parent dictionaries while flattening child keys
- **Base Templates**: Create reusable base configurations to reduce duplication
- **Type Safety**: Handle data type consistency for parquet/file outputs
- **Optional Parameters**: Make advanced features optional with sensible defaults

#### 4. Data Flow Patterns
- **Standardized Outputs**: Use consistent file formats (e.g., geoparquet for spatial data)
- **Metadata Preservation**: Include data lineage and processing metadata
- **Performance Optimization**: Round numerical data, optimize calculations for large datasets
- **Error Handling**: Graceful handling of edge cases (missing zones, type mismatches)

### Configuration Best Practices

#### Nested Dictionary Pattern
```python
# In flow file - handle both flat and nested configs
if 'config' in locals() and config:
    for key, value in config.items():
        if isinstance(value, dict):
            # Preserve parent dict AND flatten children
            locals()[key] = value  # Keep nested dict accessible
            for sub_key, sub_value in value.items():
                locals()[sub_key] = sub_value
        else:
            locals()[key] = value
```

#### Instance Referencing Pattern
```python
# In conductor.py
vector_inputs_config = {
    "point_grid_instance": "main_grid",  # Reference to runs/create_point_grid/main_grid/
    "processing_mode": "stock",
    "data_sources": {
        "am_dwellings": {
            "path": "data/L2_O_by_DESTCAT.parquet",
            "category": "Total_Occupied_dwellings"
        }
    }
}
```

### Error Handling Improvements

#### Type Safety for File Outputs
```python
# Handle mixed types for parquet compatibility
df['zone_id'] = df['zone_id'].fillna(-1).astype('Int64')  # Not strings!
```

#### Graceful Degradation
- Zone totals only for valid hexagons
- Optional features don't break core functionality
- Clear error messages with context

### Performance Considerations
- **Large Dataset Handling**: Use efficient pandas operations (drop_duplicates, groupby)
- **Memory Management**: Process in chunks where needed
- **File Formats**: Use parquet for large datasets, JSON for configs
- **Rounding**: Round display values (1 decimal) for cleaner UX

### Conductor Script Patterns
```python
# Show available options for dependent steps
print_available_instances("create_point_grid", " (spatial grids)")

# Execute with instance naming
run_step_flow("02_generate_vector_inputs", "sc_seql2_multi_vectors", vector_inputs_config)

# Pipeline status tracking
print_pipeline_status()
```

## Ductaflow Flow Development Requirements

### Mandatory Cell Structure

All ductaflow notebooks must include the following cell structure:

#### 1. Parameters Cell (REQUIRED)
```python
# %% tags=["parameters"]
# Parameters cell - will be injected by papermill
config = {}
```
**Critical**: This cell MUST be tagged with `["parameters"]` for papermill execution. Without this cell, ductaflow execution will fail.

#### 2. Configuration Handling Cell
```python
# %%
# Configuration handling - ductaflow pattern
if 'config' in locals() and config:
    for key, value in config.items():
        if isinstance(value, dict):
            locals()[key] = value
            for sub_key, sub_value in value.items():
                locals()[sub_key] = sub_value
        else:
            locals()[key] = value
```

#### 3. Imports with Correct Path
```python
# %%
import sys
# Add code directory to path for imports (executed from runs/step_name/instance_name/)
sys.path.append('../../../code')
from your_modules import your_functions
```

### Path Context Awareness

**CRITICAL**: Ductaflow notebooks execute from the context: `runs/{step_name}/{instance_name}/`

The `run_step_flow` function in conductor.py:
1. Changes to the output directory: `os.chdir(output_dir)` 
2. Calls `run_notebook()` from `runs/{step_name}/{instance_name}/`
3. **Papermill executes your notebook from this directory - no further `os.chdir()` needed!**

**All relative paths must account for this execution context:**
- Code imports: `sys.path.append('../../../code')`  
- Input data: `../../../inputs/your_data.csv`
- Previous step outputs: `../../previous_step_name/instance_name/output.parquet`

**Do NOT use `os.chdir()` in notebooks** - you're already in the correct execution directory!

### Configuration Pattern

- Config dict gets passed and all keys that don't have a nested dict as their value become local variables
- Record of config.json always stored in run folder
- Config values displayed as markdown at top of executed notebook
- Nested dictionary handling preserves parent dictionaries while flattening child keys

### Common Path Patterns
```python
# From runs/step_name/instance_name/ execution context:
sys.path.append('../../../code')                     # Access code modules  
profile_dir = Path("../../previous_step/instance")   # Reference previous step output

# For datasets within specific model runs, use config parameter:
model_runs_dir = Path(model_runs_dir)                # Convert config to Path
data = pd.read_parquet(model_runs_dir / 'subfolder/file.parquet')  # Clean path joining
```

## Original Notes
- config dict always gets passed and all keys that dont have a nested dict as their value become local variables in the executed instance
- record of config.json always stored in run folder
- config values displayed as markdown at top of executed notebook
- if you have a custom output location you will need to have ductacore accesible to python so need to update the sys.path.append('../../../code') line in the flow you are making

## Recommended Next Steps
1. **Standardize step-based pattern** across all ductaflow projects
2. **Create helper functions** for common patterns (instance discovery, config reuse)
3. **Develop templates** for common flow types (data processing, visualization, analysis)
4. **Build validation tools** for configuration schemas and data types
5. **Add pipeline visualization** tools for complex dependency graphs
