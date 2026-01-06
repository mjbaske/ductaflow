# ductaflow Project Guide Generator

Generates comprehensive HOW TO guides for new LLM instances working on domain applications with ductaflow.

## Usage

```bash
# Use default config
python generate_project_guide.py

# Use custom config
python generate_project_guide.py my_project_config.json
```

## Configuration

Edit `project_config.json` to describe your domain application:

```json
{
  "project_name": "MyDomainProject",
  "domain": "Your Domain (e.g., 'Financial Modeling')",
  "ductaflow_location": "",
  "first_build_name": "main_analysis_run",
  "first_build_execution_dir": "runs/{first_build_name}/{scenario_name}",
  "first_flow_names": ["data_prep", "analysis", "reporting"],
  "scenario_dimensions": {
    "scenario_type": ["baseline", "optimized", "sensitivity"]
  },
  "description": "Brief description of what your project does"
}
```

### Configuration Fields

- **`ductaflow_location`**: If blank, assumes `pip install -e .`. If specified (e.g., `"myproject.vendor"`), generates imports like `from myproject.vendor.ductacore import ...`
- **`first_build_execution_dir`**: f-string template for execution directories
- **`scenario_dimensions`**: Dictionary with scenario dimension names and their possible values

## Output

Generates `{project_name}_HOWTO.md` containing:

- ductaflow philosophy explanation
- Complete project structure
- Step-by-step development workflow
- Code templates for flows, builds, and conductor
- Key patterns and gotchas
- Testing and debugging approaches
- Logging best practices

Perfect for onboarding new LLM instances to your domain-specific ductaflow project!
