CREATE OR REPLACE PROCEDURE PRC_ETL_SALES_TUNED (
    p_sales_date IN DATE
)
AS
    v_log_id ETL_LOG.log_id%TYPE;
    v_target_rows_merged NUMBER; 
    v_source_rows_read NUMBER;
    v_error_message CLOB; 
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_interval INTERVAL DAY TO SECOND; -- Define variable to hold the TIMESTAMP difference
BEGIN
    -- 1. Start Logging
    v_start_time := SYSTIMESTAMP;
    INSERT INTO ETL_LOG (process_name, start_time, status)
    VALUES ('PRC_ETL_SALES_TUNED', v_start_time, 'RUNNING')
    RETURNING log_id INTO v_log_id;
    COMMIT;

    -- *** FIRST ACTION: Count all source rows read for the date (32,525) ***
    SELECT COUNT(*) 
    INTO v_source_rows_read
    FROM SALES_TRANSACTIONS T
    WHERE TRUNC(T.transaction_date) = TRUNC(p_sales_date);
    
    -- 2. Single MERGE Statement (The Optimization)
    MERGE INTO DAILY_SALES_SUMMARY D
    USING (
        SELECT
            TRUNC(T.transaction_date) AS sales_date,
            T.product_id,
            P.category AS product_category,
            SUM(T.quantity) AS total_quantity_sold,
            SUM((T.quantity * P.list_price) * (1 - T.discount_percent)) AS total_revenue
        FROM
            SALES_TRANSACTIONS T
        JOIN
            PRODUCTS P ON T.product_id = P.product_id
        WHERE
            TRUNC(T.transaction_date) = TRUNC(p_sales_date)
        GROUP BY
            TRUNC(T.transaction_date), T.product_id, P.category
    ) S
    ON (D.sales_date = S.sales_date AND D.product_id = S.product_id)
    
    WHEN MATCHED THEN
        UPDATE SET
            D.total_quantity_sold = D.total_quantity_sold + S.total_quantity_sold,
            D.total_revenue = D.total_revenue + S.total_revenue
            
    WHEN NOT MATCHED THEN
        INSERT (sales_date, product_id, product_category, total_quantity_sold, total_revenue)
        VALUES (S.sales_date, S.product_id, S.product_category, S.total_quantity_sold, S.total_revenue);

    -- Capture the number of rows inserted/updated (4 rows)
    v_target_rows_merged := SQL%ROWCOUNT;
    
    -- 3. Final Logging: Calculate and store the duration
    v_end_time := SYSTIMESTAMP;
    v_interval := v_end_time - v_start_time; -- Calculate the INTERVAL

    -- Correct duration calculation using EXTRACT
    UPDATE ETL_LOG
    SET 
        end_time = v_end_time,
        target_rows_merged = v_target_rows_merged,
        source_rows_read = v_source_rows_read,
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
            source_rows_read = v_source_rows_read,
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
END PRC_ETL_SALES_TUNED;
/