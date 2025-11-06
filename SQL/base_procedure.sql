CREATE OR REPLACE PROCEDURE PRC_ETL_SALES_BASELINE (
    p_sales_date IN DATE
)
AS
    v_log_id ETL_LOG.log_id%TYPE;
    v_rows_processed NUMBER := 0;
    v_error_message CLOB;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_interval INTERVAL DAY TO SECOND; -- Define variable to hold the TIMESTAMP difference
    
    -- Extract phase
    CURSOR c_sales_data IS
        SELECT
            t.transaction_date,
            t.product_id,
            p.category,
            t.quantity,
            p.list_price,
            t.discount_percent
        FROM
            SALES_TRANSACTIONS t
        JOIN
            PRODUCTS p ON t.product_id = p.product_id
        WHERE
            TRUNC(t.transaction_date) = TRUNC(p_sales_date);
            
    r_sales_record c_sales_data%ROWTYPE;
BEGIN
    -- 1. Start Logging (Audit)
    v_start_time := SYSTIMESTAMP;
    INSERT INTO ETL_LOG (process_name, start_time, status)
    VALUES ('PRC_ETL_SALES_BASELINE', v_start_time, 'RUNNING')
    RETURNING log_id INTO v_log_id;
    COMMIT;
    
    -- 2. Row-by-Row Processing (INEFFICIENT Loop)
    FOR r_sales_record IN c_sales_data LOOP
        
        -- Calculate Revenue (Transform phase)
        DECLARE
            v_revenue NUMBER(12, 2);
        BEGIN
            v_revenue := (r_sales_record.quantity * r_sales_record.list_price) * (1 - r_sales_record.discount_percent);
        
            -- Load Phase: Insert/Update the aggregated summary table one row at a time.
            MERGE INTO DAILY_SALES_SUMMARY d
            USING (
                SELECT 
                    TRUNC(r_sales_record.transaction_date) AS sales_date,
                    r_sales_record.product_id AS product_id,
                    r_sales_record.category AS product_category,
                    r_sales_record.quantity AS total_quantity_sold,
                    v_revenue AS total_revenue
                FROM DUAL
            ) s
            ON (d.sales_date = s.sales_date AND d.product_id = s.product_id)
            WHEN MATCHED THEN
                UPDATE SET
                    d.total_quantity_sold = d.total_quantity_sold + s.total_quantity_sold,
                    d.total_revenue = d.total_revenue + s.total_revenue
            WHEN NOT MATCHED THEN
                INSERT (sales_date, product_id, product_category, total_quantity_sold, total_revenue)
                VALUES (s.sales_date, s.product_id, s.product_category, s.total_quantity_sold, s.total_revenue);

            v_rows_processed := v_rows_processed + 1;
        END;
    END LOOP;

    -- 3. Final Logging
    v_end_time := SYSTIMESTAMP;
    v_interval := v_end_time - v_start_time; -- Calculate the INTERVAL
    
    UPDATE ETL_LOG
    SET 
        end_time = v_end_time,
        target_rows_merged = (SELECT COUNT(DISTINCT product_id) FROM SALES_TRANSACTIONS WHERE TRUNC(transaction_date) = TRUNC(p_sales_date)),
        source_rows_read = v_rows_processed,
        duration_seconds = ROUND(
            (EXTRACT(DAY FROM v_interval) * 24 * 60 * 60) +
            (EXTRACT(HOUR FROM v_interval) * 60 * 60) +
            (EXTRACT(MINUTE FROM v_interval) * 60) +
            EXTRACT(SECOND FROM v_interval), 
            3
        ), 
        status = 'SUCCESS'
    WHERE log_id = v_log_id;
    
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_end_time := SYSTIMESTAMP;
        v_error_message := SQLERRM;
        v_interval := v_end_time - v_start_time;
        
        UPDATE ETL_LOG
        SET 
            end_time = v_end_time,
            status = 'FAILED',
            error_message = v_error_message,
            source_rows_read = v_rows_processed,
            duration_seconds = ROUND(
                (EXTRACT(DAY FROM v_interval) * 24 * 60 * 60) +
                (EXTRACT(HOUR FROM v_interval) * 60 * 60) +
                (EXTRACT(MINUTE FROM v_interval) * 60) +
                EXTRACT(SECOND FROM v_interval), 
                3
            )
        WHERE log_id = v_log_id;
        COMMIT;
        RAISE;
END PRC_ETL_SALES_BASELINE;
/