CREATE TABLE dim_customer (
    customer_key BIGINT IDENTITY(1,1),
    customer_id VARCHAR(256) NOT NULL,
    customer_unique_id VARCHAR(256),
    customer_zip_code_prefix BIGINT,
    customer_city VARCHAR(256),
    customer_state VARCHAR(256),
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT GETDATE()
) DISTSTYLE ALL
SORTKEY(customer_id);



CREATE TABLE dim_product (
    product_key BIGINT IDENTITY(1,1),
    product_id VARCHAR(256) NOT NULL,
    product_category_name VARCHAR(256),
    product_name_length BIGINT,
    product_description_length BIGINT,
    product_photos_qty BIGINT,
    product_weight_g BIGINT,
    product_length_cm BIGINT,
    product_height_cm BIGINT,
    product_width_cm BIGINT,
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT GETDATE()
) DISTSTYLE ALL
SORTKEY(product_id);


CREATE TABLE fact_order_transactions (
    transaction_key BIGINT IDENTITY(1,1),
    order_id VARCHAR(256) NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    purchase_date_key INTEGER,
    approved_date_key INTEGER,
    delivered_date_key INTEGER,
    order_status VARCHAR(50),
    order_item_id INTEGER,
    seller_id VARCHAR(256),
    payment_type VARCHAR(256),
    payment_sequential INTEGER,
    payment_installments INTEGER,
    item_price DECIMAL(18,2),
    freight_value DECIMAL(18,2),
    payment_value DECIMAL(18,2),
    delivery_days INTEGER,
    delivery_delay_days INTEGER,
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(customer_key)
COMPOUND SORTKEY(purchase_date_key, order_id);


CREATE OR REPLACE PROCEDURE load_dim_customer()
AS $$
BEGIN
    UPDATE dim_customer dc
    SET expiration_date = CURRENT_DATE - 1, is_current = FALSE
    FROM staging.customers sc
    WHERE dc.customer_id = sc.customer_id
      AND dc.is_current = TRUE
      AND (COALESCE(dc.customer_city, '') != COALESCE(sc.customer_city, '')
           OR COALESCE(dc.customer_state, '') != COALESCE(sc.customer_state, ''));

    INSERT INTO dim_customer (
        customer_id, customer_unique_id, customer_zip_code_prefix,
        customer_city, customer_state, effective_date
    )
    SELECT sc.customer_id, sc.customer_unique_id, sc.customer_zip_code_prefix,
           sc.customer_city, sc.customer_state, CURRENT_DATE
    FROM staging.customers sc
    LEFT JOIN dim_customer dc ON sc.customer_id = dc.customer_id AND dc.is_current = TRUE
    WHERE dc.customer_key IS NULL
       OR (COALESCE(dc.customer_city, '') != COALESCE(sc.customer_city, '')
           OR COALESCE(dc.customer_state, '') != COALESCE(sc.customer_state, ''));
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE load_dim_product()
AS $$
BEGIN
    UPDATE dim_product dp
    SET expiration_date = CURRENT_DATE - 1, is_current = FALSE
    FROM staging.products sp
    WHERE dp.product_id = sp.product_id
      AND dp.is_current = TRUE
      AND COALESCE(dp.product_category_name, '') != COALESCE(sp.product_category_name, '');

    INSERT INTO dim_product (
        product_id, product_category_name, product_name_length,
        product_description_length, product_photos_qty, product_weight_g,
        product_length_cm, product_height_cm, product_width_cm, effective_date
    )
    SELECT sp.product_id, sp.product_category_name, sp.product_name_lenght,
           sp.product_description_lenght, sp.product_photos_qty, sp.product_weight_g,
           sp.product_length_cm, sp.product_height_cm, sp.product_width_cm, CURRENT_DATE
    FROM staging.products sp
    LEFT JOIN dim_product dp ON sp.product_id = dp.product_id AND dp.is_current = TRUE
    WHERE dp.product_key IS NULL
       OR COALESCE(dp.product_category_name, '') != COALESCE(sp.product_category_name, '');
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE load_fact_order_transactions()
AS $$
BEGIN
    INSERT INTO fact_order_transactions (
        order_id, customer_key, product_key,
        purchase_date_key, approved_date_key, delivered_date_key,
        order_status, order_item_id, seller_id,
        payment_type, payment_sequential, payment_installments,
        item_price, freight_value, payment_value,
        delivery_days, delivery_delay_days
    )
    SELECT
        so.order_id,
        dc.customer_key,
        dp.product_key,
        TO_CHAR(so.purchase_ts, 'YYYYMMDD')::INTEGER,
        TO_CHAR(so.approved_ts, 'YYYYMMDD')::INTEGER,
        TO_CHAR(so.delivered_ts, 'YYYYMMDD')::INTEGER,
        so.order_status,
        si.order_item_id,
        si.seller_id,
        sp.payment_type,
        sp.payment_sequential,
        sp.payment_installments,
        si.price,
        si.freight_value,
        sp.payment_value,
        so.delivery_days,
        so.delivery_delay_days
    FROM staging.orders so
    INNER JOIN staging.items si ON so.order_id = si.order_id
    INNER JOIN dim_customer dc ON so.customer_id = dc.customer_id AND dc.is_current = TRUE
    INNER JOIN dim_product dp ON si.product_id = dp.product_id AND dp.is_current = TRUE
    LEFT JOIN staging.payments sp ON so.order_id = sp.order_id AND si.order_item_id = sp.payment_sequential
    LEFT JOIN fact_order_transactions ft
        ON so.order_id = ft.order_id
        AND si.order_item_id = ft.order_item_id
    WHERE ft.transaction_key IS NULL;
END;
$$ LANGUAGE plpgsql;
