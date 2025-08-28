# ductaflow
A simple, useful, model instance execution pattern.
This framework provides a robust system for creating and managing pipelines of code using Jupytext .py files that are executed as notebooks with full state capture and record keeping.

## Core Concept

The pipeline management pattern works as follows:

1. **Notebooks as .py files**: All analysis steps are written as Jupytext .py files (percent format)
2. **Configuration-driven**: Each execution instance uses a JSON config file for parameters
3. **Directory-based isolation**: Each run gets its own directory with configs and artifacts
4. **Execution artifacts**: Executed notebooks are saved with full output for debugging/review
5. **Pipeline chaining**: Multiple flows can be chained together in a conductor

## Key Benefits

- **Version Control**: .py files work seamlessly with git
- **Reproducibility**: Full execution state captured in artifacts
- **Parameterization**: Easy to create variations via config changes
- **Debugging**: Failed executions saved with error state
- **Modularity**: Individual pipeline steps can be developed and tested independently
- **Scalability**: Can be extended to run on different compute environments

## Directory Structure

```
ductaflow/
├── code/                                # Also a place to include other reference code (if desired)
│   ├── ductacore.py                     # core functions of ductaflow
├── flow/                                # Source .py notebook files
│   ├── 01_data_prep.py
│   ├── 02_analysis.py  
│   └── 03_outputs.py
└── runs/                                # Execution instances
    ├── run_20250826_09/
    │   ├── CONFIG.json                  # Run configuration
    │   ├── 01_data_prep_executed.ipynb  # Execution artifacts
    │   ├── 02_analysis_executed.ipynb
    │   └── outputs/                     # Generated outputs
    └── run_20250827_10/
        └── ...
└── conductor.py              # notebook for chaining or running many instances of a flow

## Notes
- config dict always gets passed and all keys that dont have a nested dict as their value become local variables in the executed instance
- record of config.json always stored in run folder
- config values displayed as markdown at top of executed notebook
- if you have a custom output location you will need to have ductacore accesible to python so need to update the sys.path.append('../../code') line in the flow you are making
