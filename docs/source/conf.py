# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
import datetime
import os
import subprocess
import sys
from os import PathLike
from pathlib import Path

from dunamai import Version


def repository_root(path: PathLike = None) -> Path:
    if path is None:
        path = __file__
    if not isinstance(path, Path):
        path = Path(path)
    if path.is_file():
        path = path.parent
    if ".git" in (child.name for child in path.iterdir()) or path == path.parent:
        return path
    else:
        return repository_root(path.parent)


sys.path.insert(0, str(repository_root()))

subprocess.run(
    f"{sys.executable} -m pip install -U pip",
    shell=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

# -- Project information -----------------------------------------------------
project = "searvey"
copyright = f"{datetime.date.today().year}, https://github.com/oceanmodeling"

# The full version, including alpha/beta/rc tags
try:
    release = Version.from_any_vcs().serialize(dirty=True)
except RuntimeError:
    release = os.environ.get("VERSION", "0.0.0")

# -- General configuration ---------------------------------------------------

autoclass_content = "both"  # include both class docstring and __init__
autodoc_default_options = {
    # Make sure that any autodoc declarations show the right members
    "members": True,
    "inherited-members": True,
    "private-members": True,
    "member-order": "bysource",
    "exclude-members": "__weakref__",
}
autosummary_generate = True  # Make _autosummary files and include them
napoleon_numpy_docstring = False  # Force consistency, leave only Google
napoleon_use_rtype = False  # More legible

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    # Need the autodoc and autosummary packages to generate our docs.
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    # The Napoleon extension allows for nicer argument formatting.
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinxext.opengraph",
    "m2r2",
]

# sphinx_autodoc_typehints settings
always_use_bars_union = True
typehints_use_rtype = False
typehints_use_signature = False
typehints_use_signature_return = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# -- Extension configuration -------------------------------------------------
source_suffix = [".rst", ".md"]
bibtex_bibfiles = ["references.bib"]
