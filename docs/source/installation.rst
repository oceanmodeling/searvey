Installation
============

To install ``searvey``, you need `Python greater than or equal to version 3.6 <https://www.python.org/downloads>`_.

Once you have installed Python, and presumably have ``pip`` that comes with the default installation,
you can run the following command to install ``searvey``:

.. code-block:: bash

    pip install git+https://github.com/oceanmodeling/searvey.git

installing into a virtual environment
-------------------------------------

The above steps detail how to install ``searvey`` to the system Python installation.
However, you might find the need to install to a virtual environment, such as an `Anaconda <https://conda.io/projects/conda/en/latest/user-guide/install/index.html#regular-installation>`_, ``virtualenv``.

To set up a ``virtualenv`` environment, do the following:

.. code-block:: bash

    pip install virtualenv
    mkdir ~/environments
    virtualenv ~/environments/searvey
    source ~/environments/searvey/bin/activate
    pip install git+https://github.com/oceanmodeling/searvey.git

Then, you can execute a Python script from this environment:

.. code-block:: bash

    source ~/environments/searvey/bin/activate
    python /path/to/python/script.py
    deactivate
