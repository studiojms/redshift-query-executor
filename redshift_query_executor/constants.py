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
    segment_id       varchar(256)     not null ENCODE zstd,
    product_id       varchar(256)     not null ENCODE zstd,
    report_date      timestamptz      not null ENCODE raw,
    asin             varchar(256)     not null ENCODE zstd,
    brand            varchar(1024)             ENCODE zstd,
    category         varchar(256)              ENCODE zstd,
    subcategories    varchar(1024)             ENCODE zstd,
    price            double precision          ENCODE zstd,
    review_count     double precision          ENCODE zstd,
    ratings          double precision          ENCODE zstd,
    revenue          double precision          ENCODE zstd,
    revenue_1p       double precision          ENCODE zstd,
    revenue_3p       double precision          ENCODE zstd,
    sales            double precision          ENCODE zstd,
    sales_1p         double precision          ENCODE zstd,
    sales_3p         double precision          ENCODE zstd,
    seller_id        varchar(256)              ENCODE zstd,
    seller_type      varchar(256)              ENCODE zstd,
    is_available     double precision          ENCODE zstd,
    category_rank    BIGINT                    ENCODE zstd,
    sub_category     varchar(256)              ENCODE zstd,
    subcategory_rank BIGINT                   ENCODE zstd,
    is_shared_bsr    boolean          not null ENCODE zstd,
    is_parent        boolean          not null ENCODE zstd,
    is_sales_imputed boolean          not null ENCODE zstd,
    has_revenue      boolean          not null ENCODE zstd,
    new_offer_count  INTEGER                   ENCODE zstd,
    used_offer_count INTEGER                   ENCODE zstd,
    parent_asin      varchar(256)              ENCODE zstd,
    parent_id        varchar(256)              ENCODE zstd,
    seller_id_1p     varchar(256)              ENCODE zstd,
    seller_id_3p     varchar(256)              ENCODE zstd,
    seller_type_1p   varchar(256)              ENCODE zstd,
    seller_type_3p   varchar(256)              ENCODE zstd,
    variant_count    INTEGER                   ENCODE zstd,
    currency_code    varchar(4)                ENCODE zstd,
    country          varchar(4)                ENCODE zstd,
    created_at       timestamptz default ('now'::text)::timestamptz not null ENCODE delta32k
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (report_date, segment_id, product_id);"""
