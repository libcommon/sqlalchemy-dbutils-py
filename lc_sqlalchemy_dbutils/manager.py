##  -*- coding: UTF8 -*-
## manager.py
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

from typing import Any, Optional, Union

from sqlalchemy import create_engine as sqla_create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import scoped_session as ScopedSession, Session, sessionmaker as SessionMaker
from sqlalchemy.orm.query import Query


__author__ = "libcommon"


DBManagerSessionFactory = Union[ScopedSession, SessionMaker]
DBManagerSession = Union[ScopedSession, Session]


class DBManager:
    """SQLAlchemy ORM database connection manager with 
    utility methods for connecting to, querying, performing/rolling back
    transactions on, and deleting records from the database.  Agnostic to
    database backend and designed for use within a single process (not shared
    by multiple processes.)
    """
    __slots__ = ("_engine", "_scoped_sessions", "_session", "_session_factory", "connection_uri", "metadata",)

    @staticmethod
    def bootstrap_db(engine: Engine, metadata: MetaData) -> None:
        """
        Args:
            engine      => SQLAlchemy database engine
            metadata    => database schema definition
        Procedure:
            Create all tables defined in metadata
            (see: https://docs.sqlalchemy.org/en/13/core/metadata.html)
        Preconditions:
            N/A
        Raises:
            N/A
        """
        metadata.create_all(engine)

    def __init__(self,
                 connection_uri: str,
                 metadata: MetaData = None,
                 scoped_sessions: bool = False):
        self.connection_uri = connection_uri
        self.metadata: MetaData = metadata
        self._scoped_sessions = scoped_sessions
        self._engine: Optional[Engine] = None
        self._session: Optional[Session] = None
        self._session_factory: Optional[DBManagerSessionFactory] = None

    def create_engine(self, **kwargs) -> "DBManager":
        """
        Args:
            kwargs  => passed to SQLAlchemy Engine constructor
        Procedure:
            Generate SQLAlchemy engine (see: sqlalchemy.engine.Engine)
            using connection URI.
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self.engine is already set and persist is True
        """
        # Ensure self._engine isn't already defined
        if self._engine:
            raise RuntimeError("Cannot attach new Engine without removing existing one")

        # Create SQLAlchemy Engine with connection URI
        engine = sqla_create_engine(self.connection_uri, **kwargs)
        self._engine = engine
        return self

    def close_engine(self) -> "DBManager":
        """
        Args:
            N/A
        Procedure:
            Close and dispose of existing connection pool on self._engine.
        Preconditions:
            N/A
        Raises:
            N/A
        """
        # If self._engine defined
        if self._engine:
            # Dispose of existing connection pool
            self._engine.dispose()
            self._engine = None
        return self

    def create_session_factory(self, **kwargs) -> "DBManager":
        """
        Args:
            kwargs  => passed to SQLAlchemy sessionmaker constructor
        Procedure:
            Generate SQLAlchemy scoped_session if self._scoped_sessions
            otherwise sessionmaker.
        Preconditions:
            N/A
        Raises:
            N/A
        """
        # Ensure self._session_factory isn't already defined
        if self._engine:
            raise RuntimeError("Cannot attach new session factory without removing existing Engine")

        # Generate sessionmaker session factory
        self._session_factory = SessionMaker(bind=self._engine, **kwargs)
        # If scoped sessions, wrap in scoped_sessions factory
        if self._scoped_sessions:
            self._session_factory = ScopedSession(self._session_factory)
        return self

    def gen_session(self, persist: bool = True) -> DBManagerSession:
        """
        Args:
            persist => whether to persist created session on self
        Returns:
            SQLAlchemy scoped session manager if self._scoped_sessions,
            otherwise SQLAlchemy session factory.
            NOTE:
                See https://docs.sqlalchemy.org/en/13/orm/contextual.html for more information
                about the SQLAlchemy session factory (sessionmaker) and scoped session manager.
        Preconditions:
            N/A
        Raises:
            RuntimeError: if session factory hasn't been created yet, or 
                          if self.session is already set and persist is True (for non-scoped sessions)
        """
        # Ensure session factory has been created
        if not self._session_factory:
            raise ValueError("Session factory not initialized")

        # If scoped sessions, return scoped session manager
        if self._scoped_sessions:
            return self._session_factory
        # Otherwise, generate new session from session factory
        session = self._session_factory()
        # If persist session to self, ensure self.session isn't already defined
        if persist:
            if self._session:
                raise RuntimeError("Cannot attach new Session without removing existing Session")
            self._session = session
        return session

    def close_session(self) -> "DBManager":
        """
        Args:
            N/A
        Procedure:
            Closes the current session
        Preconditions:
            N/A
        Raises:
            N/A
        """
        # If scoped sessions and session factory has been initialized,
        # remove current session
        if self._scoped_sessions and self._session_factory:
            self.session_factory.remove()
        # If session on self, close it
        elif self._session:
            self._session.close()
            self._session = None
        return self

    def connect(self, bootstrap: bool = True) -> None:
        """
        Args:
            N/A
        Procedure:
            TODO
        Preconditions:
            N/A
        Raises:
            ValueError: if bootstrap and self.metadata isn't defined
        """
        # Generate database engine if needed
        if not self._engine:
            self.create_engine()
        # Bootstrap database if asked
        if bootstrap:
            if not self.metadata:
                raise ValueError("No MetaData to bootstrap database with")
            self.bootstrap_db(self._engine, self.metadata)
        # Generate session factory if needed
        if not self._session_factory:
            self.create_session_factory()

    def _assert_session(self) -> None:
        """
        Args:
            N/A
        Procedure:
            Raise ValueError if self._session not defined
        Preconditions:
            N/A
        Raises:
            ValueError: if self._session not defined
        """
        if not self._session:
            raise ValueError("No existing session")

    def query(self, model: Any, **kwargs) -> Query:
        """
        Args:
            model   => model of table to query
            kwargs  => passed to query.filter method
        Returns:
            Generated query
            Wrapper for Session.query
        Preconditions:
            model is a child class of a class created with SQLAlchemy's declarative_base
        Raises:
            N/A
        """
        # Ensure self._session is defined
        self._assert_session()

        query = self._session.query(model)
        for arg in kwargs:
            query = query.filter(getattr(model, arg) == kwargs[arg])
        return query

    def add(self, record: Any, commit: bool = False) -> "DBManager":
        """
        Args:
            record  => record to add to session
            commit  => whether to commit the transaction after adding record to session
        Procedure:
            Wrapper for Session.add, with option to commit the transaction
        Preconditions:
            model is a child class of a class created with SQLAlchemy's declarative_base
        Raises:
            ValueError: if self._session isn't defined
        """
        # Ensure self._session is defined
        self._assert_session()

        # Add record to session
        self._session.add(record)
        # Commit if asked
        if commit:
            self.commit()
        return self

    def delete(self, record: Any, commit: bool = False) -> "DBManager":
        """
        Args:
            record  => record to delete from session
            commit  => whether to commit the transaction after deleting record from session
        Procedure:
            Wrapper for Session.delete, with option to commit the transaction
        Preconditions:
            model is a child class of a class created with SQLAlchemy's declarative_base
        Raises:
            ValueError: if self._session isn't defined
        """
        # Ensure self._session is defined
        self._assert_session()

        # Delete record from session
        self._session.delete(record)
        # Commit if asked
        if commit:
            self.commit(session)
        return self

    def commit(self) -> "DBManager":
        """
        Args:
            N/A
        Procedure:
            Wrapper for Session.commit
        Preconditions:
            N/A
        Raises:
            ValueError: if self._session isn't defined
        """
        self._assert_session()
        self._session.commit()
        return self

    def rollback(self) -> "DBManager":
        """
        Args:
            N/A
        Procedure:
            Wrapper for Session.commit
        Preconditions:
            N/A
        Raises:
            ValueError: if self._session isn't defined
        """
        self._assert_session()
        self._session.rollback()
        return self
