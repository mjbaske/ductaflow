"""
ductaflow - The pipeline framework that actually works in practice.

A simple, powerful framework for reproducible data pipelines using notebooks.
"""

from .ductacore import (
    # Core execution functions
    run_notebook,
    run_step_flow,
    
    # Configuration display
    display_config_summary,
    generate_config_markdown,
    
    # HTML export
    convert_notebook_to_html,
    
    # Utilities
    setup_logging,
    load_cli_config,
    is_notebook_execution
)

__version__ = "0.3.0"
__author__ = "Mitchell Baskerville"
__description__ = "The pipeline framework that actually works in practice"

# Make key functions easily accessible
__all__ = [
    'run_notebook',
    'run_step_flow',
    'display_config_summary',
    'generate_config_markdown',
    'convert_notebook_to_html',
    'setup_logging',
    'load_cli_config',
    'is_notebook_execution'
]
