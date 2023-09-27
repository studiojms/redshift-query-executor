GET_ALL_IDS = """
SELECT id from organization WHERE enabled = 'true';
"""

CHECK_TABLE_EXISTS = """
SELECT EXISTS(
  SELECT 1
  FROM pg_tables
  WHERE tablename = '{table_name}');
"""

CREATE_TABLE_STRUCTURE = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id               varchar(256)     not null ENCODE zstd PRIMARY KEY,
    product_id       varchar(256)     not null ENCODE zstd,
    report_date      timestamptz      not null ENCODE raw,
    country          varchar(4)                ENCODE zstd,
    created_at       timestamptz default ('now'::text)::timestamptz not null ENCODE delta32k
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (report_date, segment_id, product_id);"""
