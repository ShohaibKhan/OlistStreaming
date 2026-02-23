CREATE OR REPLACE PROCEDURE new_data()
AS $$
BEGIN
    -- Load dimensions
    CALL load_dim_customer();
    CALL load_dim_product();

    -- Load fact table
    CALL load_fact_order_transactions();
END;
$$ LANGUAGE plpgsql;


-- Below call will get triggered from the EventBridge
call new_data();
