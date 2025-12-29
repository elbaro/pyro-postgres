# Logging

pyro_postgres uses Python's standard `logging` module. The logs from Rust code are automatically bridged to Python logging when the module is imported.

The logger names are `pyro_postgres` and `zero_postgres`.

## Basic Setup

```py
import logging
from pyro_postgres.sync import Conn

logging.basicConfig(level=logging.DEBUG)
conn = Conn("pg://localhost/test")  # logs will appear
```

```py
import logging

logger = logging.getLogger("pyro_postgres")  # Python layer logger
logger = logging.getLogger("zero_postgres")  # Rust layer logger
```
