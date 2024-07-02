# searvey

[![pre-commit.ci](https://results.pre-commit.ci/badge/github/oceanmodeling/searvey/master.svg)](https://results.pre-commit.ci/latest/github/oceanmodeling/searvey/master)
[![tests](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml/badge.svg)](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml)
[![readthedocs](https://readthedocs.org/projects/pip/badge/)](https://readthedocs.org/projects/searvey)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/oceanmodeling/searvey/master?urlpath=%2Flab)

Searvey aims to provide the following functionality:

- Unified catalogue of observational data including near real time (WIP).

- Real time data analysis/clean up to facilitate comparison with numerical models (WIP).

- On demand data retrieval from multiple sources that currently include:

    - U.S. Center for Operational Oceanographic Products and Services (CO-OPS)
    - Flanders Marine Institute (VLIZ); Intergovernmental Oceanographic Commission (IOC)
    - U.S. Geological Survey (USGS)
    - National Data Buoy Center (NDBC)

## Installation

The package can be installed with `pip`:

```
pip install searvey
```

and conda`:

```
conda install -c conda-forge searvey
```


## Development

In order to develop `searvey` you will need:

- Python 3.8+
- GNU Make
- [poetry](https://python-poetry.org/) >= 1.2 (you can install it with [pipx](https://github.com/pypa/pipx): `pipx install poetry`).
- [poetry-dynamic-versioning](https://github.com/mtkennerly/poetry-dynamic-versioning) which is a poetry plugin.
  Take note that this needs to be installed in the same (virtual) environment as poetry, not in the `searvey` one!
  If you used `pipx` for installing `poetry`, then you can inject it in the proper env with `pipx inject poetry poetry-dynamic-versioning`.
- [pre-commit](https://pre-commit.com/). You can also install this one with `pipx`: `pipx install pre-commit`

In order to setup the dev environment you can use:

```
python3 -mvenv .venv
source .venv/bin/activate
make init
```

which will:

1. create and activate a virtual environment,
2. install the full set of dependencies
3. Setup the pre-commit hooks

After that you should run the tests with:

```
make test
```

If you execute `make` without arguments, you should see more subcommands. E.g.

```
make mypy
make lint
make docs
make deps
```

Check them out!

### Jupyter

If you wish to use jupyterlab to test searvey, then, assuming you have an
existing jupyterlab installation, you should be able to add a kernel to it with:

```bash
python -m ipykernel install --user --name searvey
```
