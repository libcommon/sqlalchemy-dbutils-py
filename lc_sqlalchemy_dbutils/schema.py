##  -*- coding: UTF8 -*-
## schema.py
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

from sqlalchemy.engine.interfaces import Compiled
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement


__author__ = "libcommon"


class TimestampDefaultExpression(ClauseElement):
    """Class to generate server default timestamp
    expressions based on SQL dialect. To be used with
    the `server_default` parameter in the sqlalchemy.schema.Column
    constructor.
    """

@compiles(TimestampDefaultExpression, "mssql")
def generate_timestamp_expression_mssql(element: TimestampDefaultExpression, compiler: Compiled, **kwargs):
    return "GETUTCDATE()"

@compiles(TimestampDefaultExpression, "mysql")
def generate_timestamp_expression_mysql(element: TimestampDefaultExpression, compiler: Compiled, **kwargs):
    return "UTC_TIMESTAMP()"

@compiles(TimestampDefaultExpression, "oracle")
def generate_timestamp_expression_oracle(element: TimestampDefaultExpression, compiler: Compiled, **kwargs):
    return "SYS_EXTRACT_UTC(SYSTIMESTAMP)"

@compiles(TimestampDefaultExpression, "postgresql")
def generate_timestamp_expression_pgsql(element: TimestampDefaultExpression, compiler: Compiled, **kwargs):
    return "(NOW() AT TIME ZONE 'UTC')"

@compiles(TimestampDefaultExpression, "sqlite")
def generate_timestamp_expression_sqlite(element: TimestampDefaultExpression, compiler: Compiled, **kwargs):
    return "CURRENT_TIMESTAMP"
