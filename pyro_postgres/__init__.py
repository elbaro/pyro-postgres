# When [submodule]/__init__.py is executed, this __init__.py is already executed and `pyro_postgres` indicates the root, not .so.
from .pyro_postgres import *

__doc__ = pyro_postgres.__doc__
__all__ = pyro_postgres.__all__
