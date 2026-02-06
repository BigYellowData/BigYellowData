-- ============================================================================
--  populate_dim_date.sql
--  Automatically populates dim_date based on actual dates in fact_trip
-- ============================================================================

SET search_path TO dw;

-- Clear existing dim_date 
TRUNCATE TABLE dim_date CASCADE;

-- Generate only dates present in fact_trip
INSERT INTO dim_date (date_id, full_date, year, month, day, day_of_week, day_name, month_name, is_weekend)
SELECT DISTINCT
    date_id,
    TO_DATE(date_id::TEXT, 'YYYYMMDD') AS full_date,
    EXTRACT(YEAR FROM TO_DATE(date_id::TEXT, 'YYYYMMDD'))::INT AS year,
    EXTRACT(MONTH FROM TO_DATE(date_id::TEXT, 'YYYYMMDD'))::INT AS month,
    EXTRACT(DAY FROM TO_DATE(date_id::TEXT, 'YYYYMMDD'))::INT AS day,
    EXTRACT(ISODOW FROM TO_DATE(date_id::TEXT, 'YYYYMMDD'))::INT AS day_of_week,
    TO_CHAR(TO_DATE(date_id::TEXT, 'YYYYMMDD'), 'Day') AS day_name,
    TO_CHAR(TO_DATE(date_id::TEXT, 'YYYYMMDD'), 'Month') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM TO_DATE(date_id::TEXT, 'YYYYMMDD')) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM fact_trip
ORDER BY date_id
ON CONFLICT (date_id) DO NOTHING;

-- Apply Foreign Key constraint now that dim_date is populated
-- (Gracefully handle if constraint already exists)
DO $$
BEGIN
    ALTER TABLE fact_trip ADD CONSTRAINT fk_fact_trip_date
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id);
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'FK constraint already exists';
END $$;

-- Validation Summary
DO $$
DECLARE
    v_date_count INTEGER;
    v_min_date DATE;
    v_max_date DATE;
BEGIN
    SELECT COUNT(*), MIN(full_date), MAX(full_date)
    INTO v_date_count, v_min_date, v_max_date
    FROM dim_date;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'dim_date populated from fact_trip data';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total dates:  % rows', v_date_count;
    RAISE NOTICE 'Date range:   % to %', v_min_date, v_max_date;
    RAISE NOTICE '========================================';
END $$;
