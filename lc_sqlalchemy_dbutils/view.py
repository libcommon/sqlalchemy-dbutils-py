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
