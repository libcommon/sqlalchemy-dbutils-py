# sqlalchemy-dbutils-py

## Overview

[SQLAlchemy](https://www.sqlalchemy.org/) has two high-level components: Core and ORM. Core provides (not surprisingly)
the core functionality of SQLAlchemy's SQL abstraction layer. The ORM ("Object-Relational Mapper") component offers
the ability to map between Python and database types. `sqlalchemy-dbutils-py` offers a number of utilities built upon
the ORM component, including:
* Views and materialized views as regular database tables (`view` module)
* Default types for common database engines (`schema` module)
* Database connection/session management (`manager` module)

## Installation

### Install from PyPi (preferred method)

```bash
pip install lc-sqlalchemy-dbutils
```

### Install from GitHub with Pip

```bash
pip install git+https://github.com/libcommon/sqlalchemy-dbutils-py.git@vx.x.x#egg=lc_sqlalchemy_dbutils
```

where `x.x.x` is the version you want to download.

## Install by Manual Download

To download the source distribution and/or wheel files, navigate to
`https://github.com/libcommon/sqlalchemy-dbutils-py/tree/releases/vx.x.x/dist`, where `x.x.x` is the version you want to install,
and download either via the UI or with a tool like wget. Then to install run:

```bash
pip install <downloaded file>
```

Do _not_ change the name of the file after downloading, as Pip requires a specific naming convention for installation files.

## Dependencies

`sqlalchemy-dbutils-py` depends on, and is designed to work with, [SQLAlchemy](https://www.sqlalchemy.org/). Only Python
versions >= 3.6 are officially supported.

## Getting Started

### Views

The `view` module exposes a function, `create_view`, for creating (materialized) views that act like ORM tables.

```python
from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select

from lc_sqlalchemy_dbutils.view import create_view


BaseTable = declarative_base()


class User(BaseTable):
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    email_address = Column(Text, nullable=False)


# Creates view named "vuser_names" as "SELECT id, name FROM user"
UserNames = create_view("vuser_names", select([User.id, User.name]), BaseTable.metadata)
```

The `UserNames` type, which points to the `vuser_names` view in the database, can be used like any other ORM table class.
For Postgres databases, the `materialized` parameter to `create_view` can be set to `True` to make a `MATERIALIZED VIEW`. For
more information about the difference from a standard SQL view, see https://www.postgresql.org/docs/current/rules-materializedviews.html.

### Database Types

The `schema` module defines a type to generate database expressions for default datetime/timestamp values.
A common database design pattern is to use datetime/timestamp columns to track when records are created and/or modified.
The `TimestampDefaultExpression` type can be used with the `server_default` parameter to the
[Column constructor](https://docs.sqlalchemy.org/en/13/core/metadata.html#sqlalchemy.schema.Column.params.server_default).

```python
from sqlalchemy import Column, Integer, Text, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select

from lc_sqlalchemy_dbutils.schema import TimestampDefaultExpression


BaseTable = declarative_base()


class User(BaseTable):
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    email_address = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(True), nullable=False, server_default=TimestampDefaultExpression())
```

Note the use of `TIMESTAMP(True)`, as the `TimestampDefaultExpression` type will attempt to generate an expression to
retrieve a UTC timestamp in all cases.

### Database Connection Management

The `manager` module exposes a class, `DBManager`, for managing database connections and sessions with higher-level methods.
Simply create an instance of `DBManager` with an RFC-1738 compliant connection URL, and with that instance you can
connect to the datbase server, generate ORM [Sessions](https://docs.sqlalchemy.org/en/13/orm/session_api.html#session-and-sessionmaker),
build queries using ORM objects, add and remove records from the active session, and commit or rollback transactions.

```python
import sys

from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select

from lc_sqlalchemy_dbutils.manager import DBManager


BaseTable = declarative_base()


class User(BaseTable):
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    email_address = Column(Text, nullable=False)


def main() -> int:
    # Get commandline arguments
    config_path_str = sys.argv[1]
    name_filter = sys.argv[2]

    # Create DB manager from connection URL in config file
    # and attach MetaData object from BaseTable
    manager = (DBManager
               .from_file(config_path_str)
               .with_metadata(BaseTable.metadata))

    # Connect to database (but don't generate a session yet)
    manager.connect()
    # NOTE: connect() is effectively equivalent to
    # manager.create_engine().create_session_factory(), but it can also
    # call the bootstrap_db() method to create all tables in the database.
    # The caveat with using connect() is that you cannot pass specific kwargs
    # to create_engine() or create_session_factory().

    # Create an active database Session
    manager.gen_session()
    # Query the "user" table for the name specified on the commandline
    matching_user = manager.query(User, name=name_filter).first()
    if matching_user:
        print("Found matching user with name {} (ID: {})", name_filter, matching_user.id)
    else:
        print("Did not find matching user with name {}", name_filter)

    # Close active session and dispose of database engine (which closes all connections)
    # NOTE: close_engine() automatically calls close_session()
    manager.close_engine()
    return 0


if __name__ == "__main__":
    main()
```

The script above will read the database connection URL from the provided config filepath, connect to the database
and generate a `Session`, run a query to find the first `User` record where `name` matches the provided name filter,
and print the results. This is just an (heavily commented) example to show easy session management can be with the `DBManager`
class.

## Contributing/Suggestions

Contributions and suggestions are welcome! To make a feature request, report a bug, or otherwise comment on existing
functionality, please file an issue. For contributions please submit a PR, but make sure to lint, type-check, and test
your code before doing so. Thanks in advance!
