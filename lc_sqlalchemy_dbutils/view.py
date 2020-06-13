##  -*- coding: UTF8 -*-
## view.py
## Copyright (c) 2020 libcommon
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.
# pylint: disable=W0613

import os

from sqlalchemy import Column
from sqlalchemy.engine.interfaces import Compiled
from sqlalchemy.event import listen
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement, MetaData, Table
from sqlalchemy.sql.expression import FromClause


__author__ = "libcommon"


class CreateViewExpression(DDLElement):
    """Custom DDL element to create SQL view.
    NOTE: Implementation taken from
    http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/
    """
    def __init__(self, name: str, selectable: FromClause) -> None:
        self.name = name
        self.selectable = selectable

@compiles(CreateViewExpression)
def generate_view_create_expression(element: CreateViewExpression, compiler: Compiled, **kwargs) -> str:
    return "CREATE VIEW {} AS {}".format(element.name,
                                         compiler.sql_compiler.process(element.selectable,
                                                                       literal_binds=True))


class CreateMaterializedViewExpression(CreateViewExpression):
    """Custom DDL Element to create Postgres materialized view (see: CreateViewExpression)."""

@compiles(CreateMaterializedViewExpression, "postgresql")
def generate_mview_create_expression(element, compiler: Compiled, **kwargs) -> str:
    return "CREATE MATERIALIZED VIEW {} AS {}".format(element.name,
                                                      compiler.sql_compiler.process(element.selectable,
                                                                                    literal_binds=True))


class DropViewExpression(DDLElement):
    """Custom DDL element to drop SQL view."""
    def __init__(self, name: str) -> None:
        self.name = name

@compiles(DropViewExpression)
def generate_view_drop_expression(element, compiler: Compiled, **kwargs) -> str:
    return "DROP VIEW IF EXISTS {}".format(element.name)


class DropMaterializedViewExpression(DropViewExpression):
    """Cusotm DDL element to drop Postgres materialized view."""

@compiles(DropMaterializedViewExpression, "postgresql")
def generate_mview_drop_expression(element, compiler: Compiled, **kwargs) -> str:
    return "DROP MATERIZLIZED VIEW IF EXISTS {}".format(element.name)


def create_view(name: str, selectable: FromClause, metadata: MetaData, materialized: bool = False) -> Table:
    """
    Args:
        name            => name of materialized view to create
        selectable      => query to create view as
        metadata        => metadata to listen for events on
        materialized    => whether to create standard or materialized view
    Returns:
        Table object bound to temporary MetaData object with columns
        returned from selectable (essentially creates table as view).
        NOTE:
            For non-postgresql backends, creating a materialized view
            will result in a standard view, which cannot be indexed.
    Preconditions:
        N/A
    Raises:
        N/A
    """
    _tmp_mt = MetaData()
    tbl = Table(name, _tmp_mt)
    for column in selectable.c:
        tbl.append_column(Column(column.name, column.type, primary_key=column.primary_key))
    listen(metadata,
           "after_create",
           (CreateMaterializedViewExpression(name, selectable)
            if materialized else CreateViewExpression(name, selectable)))
    listen(metadata,
           "before_drop",
           DropMaterializedViewExpression(name) if materialized else DropViewExpression(name))
    return tbl


if os.environ.get("ENVIRONMENT") == "TEST":
    from datetime import datetime
    import unittest

    from sqlalchemy.engine import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import select

    from tests.common import BaseTable, User, Post


    PostAuditTimeline = create_view("post_audit_timeline",
                                    select([User.id, User.first_name, Post.created_at], order_by=Post.created_at),
                                    BaseTable.metadata)


    class TestViewUtilities(unittest.TestCase):
        """Tests for view creation/drop utilities."""

        def setUp(self):
            # Create SQLAlchemy engine for in-memory SQLite database
            # See: https://docs.sqlalchemy.org/en/13/core/engines.html#sqlite
            self.engine = create_engine("sqlite://")
            # Create all tables in database
            BaseTable.metadata.create_all(self.engine)
            # Bind sessionmaker instance to engine
            self.session_factory = sessionmaker(bind=self.engine)
            # Create session
            self.session = self.session_factory()

        def test_create_view_expression_single_table(self):
            """Test that view creation query compiles correctly
            where view selects from a single table.
            """
            # Create view query
            view_query = select([User.id, User.first_name, User.last_name])
            # Set expected output
            ddl_statement = ("CREATE VIEW user_names AS "
                             "SELECT \"user\".id, \"user\".first_name, \"user\".last_name FROM \"user\"")
            # NOTE: have to remove newline because SQLAlchemy's select inserts them by default
            self.assertEqual(ddl_statement,
                             str(CreateViewExpression("user_names", view_query)).replace("\n", ""))

        def test_create_view_expression_join(self):
            """Test that view creation query compiles correctly
            where view selects from two tables (with join).
            """
            # Create view query
            view_query = select([
                User.id,
                (User.first_name + User.last_name).label("full_name"),
                Post.content
            ])
            ddl_statement = ("CREATE VIEW user_posts AS "
                             "SELECT \"user\".id, \"user\".first_name || \"user\".last_name AS full_name, post.content "
                             "FROM \"user\", post")
            self.assertEqual(ddl_statement,
                             str(CreateViewExpression("user_posts", view_query)).replace("\n", ""))

        def test_drop_view_expression_single_table(self):
            """Test that drop view query compiles correctly."""
            self.assertEqual("DROP VIEW IF EXISTS user_names", str(DropViewExpression("user_names")))

        def test_select_from_created_view(self):
            """Test that PostAuditTimeline was created in database and
            has the right columns by:
                1) Adding a User record
                2) Adding a Post record tied to User
                3) Selecting from view
            """
            # Add User record to database
            user = User(first_name="Susan", last_name="Sarandon", email="susan.sarandon@gmail.com")
            # NOTE: Pylint doesn't see these methods on Session type
            self.session.add(user)  # pylint: disable=E1101
            self.session.commit()   # pylint: disable=E1101
            # Add Post record to database
            created_at_datetime = datetime.utcnow()
            post = Post(user_id=user.id, content="<h1>This is a post</h1>", created_at=created_at_datetime)
            self.session.add(post)  # pylint: disable=E1101
            self.session.commit()   # pylint: disable=E1101
            # Select records from post_audit_timeline and ensure match up with records in database
            self.assertEqual([(1, "Susan", created_at_datetime)], self.session.query(PostAuditTimeline).all())  # pylint: disable=E1101

        def tearDown(self):
            self.session.close()    # pylint: disable=E1101
            self.engine.dispose()
