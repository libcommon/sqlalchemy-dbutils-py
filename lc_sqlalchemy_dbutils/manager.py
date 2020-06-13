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

from getpass import getpass
import os
from pathlib import Path
from typing import Any, Optional, Union

from sqlalchemy import create_engine as sqla_create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.orm import scoped_session as ScopedSession, Session, sessionmaker as SessionMaker
from sqlalchemy.orm.query import Query


__author__ = "libcommon"


DBManagerSessionFactory = Union[ScopedSession, SessionMaker]
DBManagerSession = Union[ScopedSession, Session]
ConnectionURL = Union[str, URL]


class DBManager:
    """SQLAlchemy ORM database connection manager with
    utility methods for connecting to, querying, performing/rolling back
    transactions on, and deleting records from the database.  Agnostic to
    database backend and designed for use within a single process (not shared
    by multiple processes.)
    """
    __slots__ = ("_engine", "_scoped_sessions", "_session", "_session_factory", "connection_url", "metadata",)

    @classmethod
    def from_file(cls, config_path_str: str) -> "DBManager":
        """
        Args:
            config_path     => path to file containing connection URL
        Description:
            Reads connection URL from config file and creates instance of class.
            Will validate connection URL and if it doesn't have password, will prompt user.
        Preconditions:
            Connection URL must be a valid RFC1738 URL and must be the only content in the file.
        Raises:
            FileNotFoundError: if provided config_path isn't an existing file
            ValueError: if validation (parsing) of connection URL fails
        """
        # Ensure config_path is existing file
        config_path = Path(config_path_str)
        if not config_path.is_file():
            raise FileNotFoundError(str(config_path))

        # Read first line from file and use as connection URL
        with open(str(config_path)) as config_file:
            connection_url_str = config_file.read().strip()

        # Parse connection URL into various components
        try:
            connection_url = make_url(connection_url_str)
        except Exception as exc:
            raise ValueError("Failed to parse URL from file ({})".format(exc))
        # If is not SQLite file and password not provided, get password from user
        if not ("sqlite" in connection_url.drivername or connection_url.password):
            passwd = getpass("Enter database password: ")
            connection_url.password = passwd
        return cls(connection_url)

    def __init__(self,
                 connection_url: ConnectionURL,
                 metadata: Optional[MetaData] = None,
                 scoped_sessions: bool = False):
        if isinstance(connection_url, str):
            connection_url = make_url(connection_url)

        self.connection_url = connection_url
        self.metadata = metadata
        self._scoped_sessions = scoped_sessions
        self._engine: Optional[Engine] = None
        self._session: Optional[Session] = None
        self._session_factory: Optional[DBManagerSessionFactory] = None

    def create_engine(self, **kwargs) -> "DBManager":
        """
        Args:
            kwargs  => passed to SQLAlchemy Engine constructor
        Description:
            Create SQLAlchemy Engine using self.connection_url.
            See: https://docs.sqlalchemy.org/en/13/core/engines.html
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self.engine is already set and persist is True
        """
        # Ensure self._engine isn't already defined
        # NOTE: Consider whether this implementation makes sense, or if it makes more sense
        #       to simply dispose of existing engine (with DEBUG log) before creating new one.
        if self._engine:
            raise RuntimeError("Cannot attach new Engine without removing existing one")

        # Create SQLAlchemy Engine with connection URL
        engine = sqla_create_engine(self.connection_url, **kwargs)
        self._engine = engine
        return self

    def close_engine(self) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Close and dispose of existing Engine and connection pool on
            self._engine if defined.
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

    def with_metadata(self, metadata: MetaData) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Setter for self.metadata using builder pattern.
        Preconditions:
            N/A
        Raises:
            N/A
        """
        self.metadata = metadata
        return self

    def bootstrap_db(self) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Create all tables defined in self.metadata.
            See: https://docs.sqlalchemy.org/en/13/core/metadata.html
        Preconditions:
            N/A
        Raises:
            N/A
        """
        if not self._engine:
            raise RuntimeError("Cannot bootstrap database without an Engine")
        if not self.metadata:
            raise RuntimeError("Cannot bootstrap database with MetaData")

        self.metadata.create_all(self._engine)
        return self

    def create_session_factory(self, **kwargs) -> "DBManager":
        """
        Args:
            kwargs  => passed to SQLAlchemy sessionmaker constructor
        Description:
            Create SQLAlchemy scoped_session if self._scoped_sessions is True,
            otherwise sessionmaker. All kwargs are passed to sessionmaker constructor.
            This method should only be called _once_ by the DBManager. SQLAlchemy doesn't
            recommend manually closing all sessions, and the mechanics for doing so have changed
            across versions.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#session-and-sessionmaker
            and  https://docs.sqlalchemy.org/en/13/orm/contextual.html#sqlalchemy.orm.scoping.scoped_session
            and  https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.sessionmaker.close_all
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self._session_factory is already defined, or
                          if self._engine isn't defined
        """
        # Ensure self._session_factory isn't already defined
        if self._session_factory:
            raise RuntimeError("Session factory already created")
        # Ensure self._engine is defined
        if not self._engine:
            raise RuntimeError("Cannot create session factory without an Engine")

        # Generate sessionmaker session factory
        self._session_factory = SessionMaker(bind=self._engine, **kwargs)
        # If scoped sessions, wrap in scoped_sessions factory
        if self._scoped_sessions:
            self._session_factory = ScopedSession(self._session_factory)
        return self

    def connect(self, bootstrap: bool = False) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Create database engine and session factory (but _not_ active session).
            gen_session must be called subsequently to create an active session.
            If bootstrap specified, use self.metdata and self._engine to create all tables,
            indexes, views, etc.
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
            self.bootstrap_db()
        # Generate session factory if needed
        if not self._session_factory:
            self.create_session_factory()
        return self

    def gen_session(self, persist: bool = True) -> DBManagerSession:
        """
        Args:
            persist => whether to persist created session on self
        Description:
            Generate new database session. If persist is True, assign new session
            to self._session. In this way, the DBManager can act simply as a factory for new sessions,
            or as a more complete DB manager. Use the `session` method to access the active session.
            See: https://docs.sqlalchemy.org/en/13/orm/session_basics.html#basics-of-using-a-session
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self._session_factory hasn't been created yet, or
                          if self._session is already set and persist is True (for non-scoped sessions)
        """
        # Ensure session factory has been created
        if not self._session_factory:
            raise RuntimeError("Session factory must be created before a session can be generated")

        # If scoped sessions, return scoped session manager
        if self._scoped_sessions:
            return self._session_factory    # type: ignore
        # Otherwise, generate new session from session factory
        session = self._session_factory()
        # If persist session to self, ensure self.session isn't already defined
        if persist:
            if self._session:
                raise RuntimeError("Cannot attach new Session without removing existing Session")
            self._session = session
        return session

    def session(self) -> Optional[DBManagerSession]:
        """
        Args:
            N/A
        Description:
            Current session (if exists).
        Preconditions:
            N/A
        Raises:
            N/A
        """
        # If scoped sessions, return scoped session manager
        if self._scoped_sessions:
            return self._session_factory    # type: ignore
        # Otherwise, return self._session
        return self._session

    def close_session(self) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Close the current session.
        Preconditions:
            N/A
        Raises:
            N/A
        """
        # If scoped sessions and session factory has been initialized,
        # remove current session
        if self._scoped_sessions and self._session_factory:
            self._session_factory.remove()  # type: ignore
        # If session on self, close it
        elif self._session:
            self._session.close()
            self._session = None
        return self

    def _assert_session(self) -> DBManagerSession:
        """
        Args:
            N/A
        Description:
            Raise ValueError if no existing session. If scoped_sessions
            is True, then requires self._session_factory to be defined.
            Otherwise, requires self._session to be defined (non-None).
        Preconditions:
            N/A
        Raises:
            ValueError: if self._session not defined
        """
        session = self.session()
        if not session:
            raise RuntimeError("Must have active session")
        return session

    def query(self, model: Any, **kwargs) -> Query:
        """
        Args:
            model   => model of table to query
            kwargs  => passed to query.filter method
        Description:
            Wrapper for Session.query, with option to build WHERE clause.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.query
        Preconditions:
            record is instance of class whose parent class was created using SQLAlchemy's declarative_base.
        Raises:
            RuntimeError: if self._session isn't defined
        """
        # Ensure active session
        session = self._assert_session()

        query = session.query(model)
        for arg in kwargs:
            query = query.filter(getattr(model, arg) == kwargs[arg])
        return query

    def add(self, record: Any, commit: bool = False) -> "DBManager":
        """
        Args:
            record  => record to add to session
            commit  => whether to commit the transaction after adding record to session
        Description:
            Wrapper for Session.add, with option to commit the transaction.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.add
        Preconditions:
            record is instance of class whose parent class was created using SQLAlchemy's declarative_base.
        Raises:
            RuntimeError: if self._session isn't defined
        """
        # Ensure active session
        session = self._assert_session()

        # Add record to session
        session.add(record)
        # Commit if asked
        if commit:
            session.commit()
        return self

    def delete(self, record: Any, commit: bool = False) -> "DBManager":
        """
        Args:
            record  => record to delete from session
            commit  => whether to commit the transaction after deleting record from session
        Description:
            Wrapper for Session.delete, with option to commit the transaction.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.delete
        Preconditions:
            record is instance of class whose parent class was created using SQLAlchemy's declarative_base.
        Raises:
            RuntimeError: if self._session isn't defined
        """
        # Ensure active session
        session = self._assert_session()

        # Delete record from session
        session.delete(record)
        # Commit if asked
        if commit:
            session.commit()
        return self

    def commit(self) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Wrapper for Session.commit.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.commit
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self._session isn't defined
        """
        # Ensure active session
        session = self._assert_session()

        session.commit()
        return self

    def rollback(self) -> "DBManager":
        """
        Args:
            N/A
        Description:
            Wrapper for Session.rollback.
            See: https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.rollback
        Preconditions:
            N/A
        Raises:
            RuntimeError: if self._session isn't defined
        """
        # Ensure active session
        session = self._assert_session()

        session.rollback()
        return self


if os.environ.get("ENVIRONMENT") == "TEST":
    import unittest
    from unittest.mock import patch, mock_open

    from tests.common import BaseTable, User


    class TestDBManager(unittest.TestCase):
        """Tests for DBManager API."""

        def setUp(self):
            self.connection_url_default = "postgresql://dbuser@pghost10/appdb"
            self.connection_url_with_password = "postgresql://dbuser:kx%25jj5%2Fg@pghost10/appdb"
            self.connection_url_sqlite = "sqlite://"

        def test_from_file_invalid_filepath(self):
            """Test that invalid filepath to DBManager.from_file
            raises FileNotFoundError.
            """
            nonexistent_filepath = Path().cwd().joinpath("url_config.txt")
            self.assertRaises(FileNotFoundError, DBManager.from_file, nonexistent_filepath)

        def test_from_file_invalid_url(self):
            """Test that invalid URL in file passed to DBManager.from_file
            raises ValueError.
            """
            #                                                                  |--| port is not number
            connection_url = "postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10:port/appdb"
            with patch("{}.open".format(__name__), mock_open(read_data=connection_url)):
                self.assertRaises(ValueError, DBManager.from_file, __file__)

        def test_from_file_no_passwd_sqlite(self):
            """Test that if connection URL isn't for SQLite and no
            password provided, prompts for password and updates
            database connection URL.
            """
            passwd = "passphrase"
            with patch("{}.getpass".format(__name__), return_value=passwd), \
                 patch("{}.open".format(__name__), mock_open(read_data=self.connection_url_default)):
                manager = DBManager.from_file(__file__)
            self.assertEqual(passwd, manager.connection_url.password)

        def test_create_engine_with_existing(self):
            """Test that engine creation raises RuntimeError when engine
            is already set.
            """
            manager = DBManager(self.connection_url_sqlite).create_engine()
            self.assertRaises(RuntimeError, manager.create_engine)

        def test_close_engine_with_existing(self):
            """Test that engine is set to None if already set."""
            manager = DBManager(self.connection_url_sqlite).create_engine()
            manager.close_engine()
            self.assertIsNone(manager._engine)

        def test_bootstrap_db(self):
            """Test that bootstrap_db raises RuntimeError without Engine and MetaData."""
            manager = DBManager(self.connection_url_sqlite)
            # Bootstrap database without Engine
            self.assertRaises(RuntimeError, manager.bootstrap_db)
            manager.create_engine()
            # Bootstrap database without MetaData
            self.assertRaises(RuntimeError, manager.bootstrap_db)

        def test_create_session_factory_without_engine(self):
            """Test that session factory creation raises RuntimeError without Engine."""
            manager = DBManager(self.connection_url_sqlite)
            self.assertRaises(RuntimeError, manager.create_session_factory)

        def test_create_session_factory_with_existing(self):
            """Test that session factory creation raises RuntimeError with
            existing session factory.
            """
            manager = DBManager(self.connection_url_sqlite).connect()
            self.assertRaises(RuntimeError, manager.create_session_factory)

        def test_gen_session_without_factory(self):
            """Test that session generation raises RuntimeError without session factory."""
            manager = DBManager(self.connection_url_sqlite)
            self.assertRaises(RuntimeError, manager.gen_session)
            manager.create_engine()
            self.assertRaises(RuntimeError, manager.gen_session)

        def test_gen_session_non_scoped_persist(self):
            """Test that non-scoped session persists to self if persist is True."""
            manager = DBManager(self.connection_url_sqlite).connect()
            session = manager.gen_session(persist=True)
            self.assertIsNotNone(session)
            self.assertEqual(session, manager._session)

        def test_close_session_with_existing(self):
            """Test that persisted session is set to None if already set."""
            manager = DBManager(self.connection_url_sqlite).connect()
            manager.gen_session(persist=True)
            self.assertIsNotNone(manager._session)
            manager.close_session()
            self.assertIsNone(manager._session)

        def test_session_methods_no_session(self):
            """Test that query, add, delete, commit, and rollback methods fail
            without existing Session.
            """
            manager = DBManager(self.connection_url_sqlite, metadata=BaseTable.metadata).connect()
            user_record = User(first_name="Samuel", last_name="Jackson", email="samuel.l.jackson@protonmail.com")

            self.assertRaises(RuntimeError, manager.query, User)
            self.assertRaises(RuntimeError, manager.commit)
            self.assertRaises(RuntimeError, manager.rollback)
            for method_name in ("add", "delete"):
                with self.subTest(test_name=method_name):
                    self.assertRaises(RuntimeError, getattr(manager, method_name), user_record)

        def test_query_where_clause_kwargs(self):
            """Test that kwargs supplied to query get properly passed to session.query.filter
            to build WHERE clause.
            """
            manager = DBManager(self.connection_url_sqlite, metadata=BaseTable.metadata).connect()
            manager.gen_session(persist=True)
            expected_query = ("SELECT \"user\".id, \"user\".first_name, \"user\".last_name, \"user\".email "
                              "FROM \"user\" "
                              "WHERE \"user\".first_name = 'Samuel' AND \"user\".email = 'samuel.l.jackson@gmail.com'")
            query_str = (str(manager
                         .query(User, first_name="Samuel", email="samuel.l.jackson@gmail.com")
                         .statement
                         .compile(compile_kwargs={"literal_binds": True}))
                         .replace("\n", ""))
            self.assertEqual(expected_query, query_str)
