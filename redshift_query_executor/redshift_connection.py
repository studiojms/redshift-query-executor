import os
import psycopg2
import structlog

from psycopg2.errors import lookup
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor

# pylint: disable=invalid-name
FeatureNotSupportedException = lookup("0A000")  # http://initd.org/psycopg/docs/errors.html

LOGGER = structlog.get_logger(__name__)


class RedshiftConnection:
    """
    Connects to Redshift and executes queries
    """

    def __init__(self, as_admin=False, high_priority=False, keep_conn=False):
        self._jarvis_secrets = None
        self._secret_manager_client = None
        self._as_admin = as_admin
        self._high_priority = high_priority
        self.keep_conn = keep_conn
        self.conn = None

    @property
    def redshift_host(self) -> str:
        return os.environ.get("DB_HOST", "localhost")

    @property
    def redshift_port(self) -> int:
        return int(os.environ.get("DB_PORT", 5439))

    @property
    def redshift_database(self) -> str:
        return os.environ.get("DB_DATABASE", "dev")

    @property
    def redshift_user(self) -> str:
        return os.environ.get("DB_USER", "dev")

    @property
    def redshift_password(self) -> str:
        return os.environ.get("DB_PASSWORD", "dev")

    @property
    def connection(self):
        if self.keep_conn and self.conn:
            return self.conn
        # this prevent "EOF detected error" in laptop for a long running query
        keepalive_kwargs = {"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 5, "keepalives_count": 5}

        return psycopg2.connect(
            host=self.redshift_host,
            port=self.redshift_port,
            database=self.redshift_database,
            user=self.redshift_user,
            password=self.redshift_password,
            **keepalive_kwargs,
        )

    def query(self, *query: str, parameters=None, auto_commit: bool = False):
        if not isinstance(auto_commit, bool):
            raise TypeError("The 'auto_commit' parameter must be a Boolean")

        conn = self.connection
        LOGGER.debug(
            "Connection opened", host=self.redshift_host, port=self.redshift_port, database=self.redshift_database
        )

        if auto_commit:
            LOGGER.debug("Setting connection isolation level to auto commit because auto_commit is set")
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        results = []

        try:
            query_group = "admin"
            with conn.cursor(cursor_factory=DictCursor) as curs:
                curs.execute(f"SET query_group = '{query_group}'")
            conn.commit()

            for single_query in query:
                with conn.cursor(cursor_factory=DictCursor) as curs:
                    curs.execute(single_query, vars=parameters)
                    if curs.description:
                        LOGGER.debug("Fetching all data from cursor", description=curs.description)
                        results.append(curs.fetchall())
                    else:
                        results.append([])
            if not auto_commit:
                LOGGER.debug("Committing transaction")
                conn.commit()
        except Exception as ex:
            if not auto_commit:
                LOGGER.debug("Rolling back transaction")
                conn.rollback()
            raise ex
        finally:
            if not self.keep_conn:
                LOGGER.debug("Closing connection")
                conn.close()

        return results

    def close(self):
        if self.keep_conn:
            LOGGER.debug("Closing connection")
            self.conn.close()

    def execute_redshift_vacuum(self, table_name):
        LOGGER.info("Starting_VACUUM", table_name=table_name)

        conn = psycopg2.connect(
            host=self.redshift_host,
            port=self.redshift_port,
            database=self.redshift_database,
            user=self.redshift_user,
            password=self.redshift_password,
        )
        LOGGER.debug(
            "Connection opened", host=self.redshift_host, port=self.redshift_port, database=self.redshift_database
        )
        # vacuum commands must be executed outside a transaction
        LOGGER.debug("Setting connection isolation level to auto commit for all VACUUM queries")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        try:
            with conn.cursor() as curs:
                LOGGER.info("Executing_VACUUM", table_name=table_name)
                curs.execute(f"VACUUM {table_name} TO 100 PERCENT")
        except FeatureNotSupportedException:
            LOGGER.error("Vacuum is currently running. Unable to run Vacuum", table_name=table_name)
        finally:
            LOGGER.debug("Closing connection")
            conn.close()
