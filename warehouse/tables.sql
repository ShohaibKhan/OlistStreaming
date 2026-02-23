create schema staging;
-- Create tables if not exist


CREATE TABLE IF NOT EXISTS staging.orders (
    order_id              VARCHAR(256),
    customer_id           VARCHAR(256),
    order_status          VARCHAR(50),
    purchase_ts           TIMESTAMP,
    approved_ts           TIMESTAMP,
    carrier_ts            TIMESTAMP,
    delivered_ts          TIMESTAMP,
    estimated_ts          TIMESTAMP,
    ingested_at           TIMESTAMP,
    delivery_days         INT,
    delivery_delay_days   INT
)DISTSTYLE EVEN;


CREATE TABLE IF NOT EXISTS staging.payments (
    order_id              VARCHAR(256),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(256),
    payment_installments  INTEGER,
    payment_value         DOUBLE PRECISION,
    ingested_at           TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY AUTO;


CREATE TABLE IF NOT EXISTS staging.items (
    order_id            VARCHAR(256),
    product_id          VARCHAR(256),
    seller_id           VARCHAR(256),
    order_item_id       INTEGER,
    shipping_limit_ts   TIMESTAMP,
    price               DOUBLE PRECISION,
    freight_value       DOUBLE PRECISION,
    ingested_at         TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY AUTO;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id               VARCHAR(256),
    customer_unique_id        VARCHAR(256),
    customer_zip_code_prefix  BIGINT,
    customer_city             VARCHAR(256),
    customer_state            VARCHAR(256)
)
DISTSTYLE AUTO
SORTKEY AUTO;

CREATE TABLE IF NOT EXISTS staging.products (
    product_id                 VARCHAR(256),
    product_category_name      VARCHAR(256),
    product_name_lenght        BIGINT,
    product_description_lenght BIGINT,
    product_photos_qty         BIGINT,
    product_weight_g           BIGINT,
    product_length_cm          BIGINT,
    product_height_cm          BIGINT,
    product_width_cm           BIGINT
)
DISTSTYLE AUTO
SORTKEY AUTO;


-- Auto Copy jobs to the staging tables


COPY staging.orders
FROM 's3://ecommercestream/processed/orders/purchase_date='
IAM_ROLE 'arn:aws:iam::650251702489:role/RedshiftToS3'
FORMAT AS PARQUET
JOB CREATE auto_copy_orders
AUTO ON;

COPY staging.items
FROM 's3://ecommercestream/processed/items/tems'
IAM_ROLE 'arn:aws:iam::650251702489:role/RedshiftToS3'
FORMAT AS PARQUET
JOB CREATE auto_copy_items
AUTO ON;

COPY staging.payments
FROM 's3://ecommercestream/processed/payments'
IAM_ROLE 'arn:aws:iam::650251702489:role/RedshiftToS3'
FORMAT AS PARQUET
JOB CREATE auto_copy_payments
AUTO ON;

COPY staging.customers
FROM 's3://ecommercestream/processed/customers'
IAM_ROLE 'arn:aws:iam::650251702489:role/RedshiftToS3'
FORMAT AS CSV
IGNOREHEADER 1
DELIMITER ','
JOB CREATE auto_copy_customers
AUTO ON;

COPY staging.products
FROM 's3://ecommercestream/processed/products'
IAM_ROLE 'arn:aws:iam::650251702489:role/RedshiftToS3'
FORMAT AS CSV
IGNOREHEADER 1
DELIMITER ','
JOB CREATE auto_copy_products
AUTO ON;


