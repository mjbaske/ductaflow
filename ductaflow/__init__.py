"""
ductaflow - The simple pipeline framework

A simple, powerful framework for reproducible data pipelines using notebooks.
"""

from .ductaflow import (
    # Core execution functions
    run_notebook,
    run_step_flow,
    debug_flow,


    # Configuration handling
    display_config_summary,
    generate_config_markdown,
    unpack_config,

    # Status analysis
    analyze_execution_logs,
    generate_status_report,

    # HTML export
    convert_notebook_to_html,

    # Utilities
    load_cli_config,
    is_notebook_execution,
    setup_execution_logging,
    setup_conductor_logging,
)

__version__ = "0.3.0"
__author__ = "Mitchell Baskerville"
__description__ = "The pipeline framework that actually works in practice"

# Make key functions easily accessible
__all__ = [
    'run_notebook',
    'run_step_flow',
    'debug_flow',
    'display_config_summary',
    'generate_config_markdown',
    'unpack_config',
    'analyze_execution_logs',
    'generate_status_report',
    'convert_notebook_to_html',
    'load_cli_config',
    'is_notebook_execution',
    'setup_execution_logging',
    'setup_conductor_logging',
]
