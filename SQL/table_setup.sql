-- Source Table 1: PRODUCTS (For product reference data)
CREATE TABLE PRODUCTS (
    product_id   NUMBER(6) PRIMARY KEY,
    list_price   NUMBER(10, 2) NOT NULL,
    category     VARCHAR2(50) NOT NULL
);
-- Source Table 2: SALES_TRANSACTIONS (The high-volume source data)
CREATE TABLE SALES_TRANSACTIONS (
    transaction_id   NUMBER(12) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    product_id       NUMBER(6) REFERENCES PRODUCTS(product_id),
    quantity         NUMBER(6) NOT NULL,
    discount_percent NUMBER(3, 2) DEFAULT 0.00
);
-- Target Table: DAILY_SALES_SUMMARY (The destination for the ETL process)
CREATE TABLE DAILY_SALES_SUMMARY (
    sales_date           DATE,
    product_id           NUMBER(6) NOT NULL,
    product_category     VARCHAR2(50) NOT NULL,
    total_quantity_sold  NUMBER(10) NOT NULL,
    total_revenue        NUMBER(12, 2) NOT NULL,
    CONSTRAINT pk_sales_summary PRIMARY KEY (sales_date, product_id)
);
-- Logging Table
CREATE TABLE ETL_LOG (
    log_id          NUMBER GENERATED ALWAYS AS IDENTITY,
    process_name    VARCHAR2(100),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    rows_processed  NUMBER,
    status          VARCHAR2(20),
    error_message   CLOB
);

--edit
ALTER TABLE ETL_LOG RENAME COLUMN rows_processed TO target_rows_merged;
ALTER TABLE ETL_LOG ADD (
    source_rows_read NUMBER,
    duration_seconds NUMBER(10, 3)
);
COMMIT;

-- Insert Sample Products
INSERT INTO PRODUCTS (product_id, list_price, category) VALUES (101, 19.99, 'Books');
INSERT INTO PRODUCTS (product_id, list_price, category) VALUES (102, 499.50, 'Electronics');
INSERT INTO PRODUCTS (product_id, list_price, category) VALUES (103, 75.00, 'Apparel');
INSERT INTO PRODUCTS (product_id, list_price, category) VALUES (104, 10.50, 'Books');
COMMIT;


SET SERVEROUTPUT ON
DECLARE
    v_start_date DATE := DATE '2025-10-01';
    v_end_date   DATE := DATE '2025-10-31';
    v_rows_to_insert NUMBER := 1000000;
    v_counter NUMBER := 1;
    v_product_id NUMBER;
    v_discount NUMBER;
    v_rand_day NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting data generation...');
    SELECT NVL(MAX(transaction_id), 0) + 1 INTO v_counter FROM SALES_TRANSACTIONS;

    FOR i IN 1 .. v_rows_to_insert LOOP
        v_product_id := 100 + TRUNC(DBMS_RANDOM.VALUE(1, 5)); 
        v_discount := TRUNC(DBMS_RANDOM.VALUE(0, 3)) * 0.10; 
        v_rand_day := TRUNC(DBMS_RANDOM.VALUE(0, v_end_date - v_start_date + 1));

        INSERT INTO SALES_TRANSACTIONS (
            transaction_id, transaction_date, product_id, quantity, discount_percent
        )
        VALUES (
            v_counter,
            v_start_date + v_rand_day,
            v_product_id,
            TRUNC(DBMS_RANDOM.VALUE(1, 10)), 
            v_discount
        );
        
        v_counter := v_counter + 1;

        IF MOD(i, 50000) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE(i || ' rows inserted and committed.');
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data generation complete. Total rows: ' || (v_counter - 1));

END;
/