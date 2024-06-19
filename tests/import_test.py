from __future__ import annotations

import importlib.metadata

import searvey


def test_version():
    assert searvey.__version__ == importlib.metadata.version("searvey")
