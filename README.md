# searvey

[![pre-commit.ci](https://results.pre-commit.ci/badge/github/oceanmodeling/searvey/master.svg)](https://results.pre-commit.ci/latest/github/oceanmodeling/searvey/master)
[![tests](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml/badge.svg)](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml)
[![readthedocs](https://readthedocs.org/projects/pip/badge/)](https://readthedocs.org/projects/searvey)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/oceanmodeling/searvey/master?urlpath=%2Flab)

Searvey aims to provide the following functionality:

- Unified catalogue of observational data including near real time.

- Real time data analysis/clean up to facilitate comparison with numerical
  models.

- On demand data retrieval from multiple sources that currently include:

    - U.S. Center for Operational Oceanographic Products and Services (CO-OPS)
    - Flanders Marine Institute (VLIZ); Intergovernmental Oceanographic Commission (IOC)

## Installation

The package can be installed with `conda`:

`conda install -c conda-forge searvey`

## Development

```
python3 -mvenv .venv
source .venv/bin/activate
poetry install
pre-commit install
```

If you wish to use jupyterlab to test searvey, then, assuming you have an
existing jupyterlab
installation, you should be able to add a kernel to it with:

```bash
python -m ipykernel install --user --name searvey
```
