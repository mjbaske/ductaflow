"""
Setup file for ductaflow - The simple pipeline framework
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
if readme_path.exists():
    with open(readme_path, "r", encoding="utf-8") as f:
        long_description = f.read()
else:
    long_description = "The simple pipeline framework"

setup(
    name="ductaflow",
    version="0.3.0",
    author="Mitchell Baskerville",
    description="The pipeline framework that actually works in practice",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: None"
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "papermill>=2.3.0",
        "jupytext>=1.13.0",
        "pandas>=1.3.0",

    ],
    extras_require={
        "html": ["nbconvert>=6.0.0"],
    },
    include_package_data=True,
    project_urls={
        "Bug Reports": "https://github.com/mjbaske/ductaflow/issues",
        "Source": "https://github.com/mjbaske/ductaflow",
        "Documentation": "https://github.com/mjbaske/ductaflow/blob/main/README.md",
    },
)
