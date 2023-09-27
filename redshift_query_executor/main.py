import structlog

from .redshift_connection import RedshiftConnection
from .constants import CREATE_TABLE_STRUCTURE, GET_ALL_IDS, CHECK_TABLE_EXISTS

LOGGER = structlog.get_logger(__name__)


class Main:
    def __init__(self):
        self.db_connection = RedshiftConnection()

    def get_org_ids(self) -> list:
        LOGGER.info("Getting org ids")
        return self.db_connection.query(GET_ALL_IDS)

    def _get_table_name(self, org_id: str) -> str:
        return f"api_app_segment_asin_estimates_for_org_{org_id}"

    def check_table_exists(self, org_id: str) -> bool:
        table_name = self._get_table_name(org_id)
        LOGGER.info(f"Checking if table {table_name} exists")
        sql = CHECK_TABLE_EXISTS.format(table_name=table_name)
        return self.db_connection.query(sql)[0][0][0] or False

    def create_table(self, org_id):
        LOGGER.info(f"Creating table for org {org_id}")
        table_name = self._get_table_name(org_id)
        sql = CREATE_TABLE_STRUCTURE.format(table_name=table_name)
        self.db_connection.query(sql)

    def _flatten(self, l):
        return [item for sublist in l for item in sublist]

    def run(self):
        org_ids = self._flatten(self._flatten(self.get_org_ids()))
        for org_id in org_ids:
            # if not self.check_table_exists(org_id):
            self.create_table(org_id)
            # else:
            #     LOGGER.info(f"Table for org {org_id} already exists")
