-- ============================================================================
--  populate_dim_date.sql
--  Génère dim_date automatiquement à partir des dates réelles de fact_trip
-- ============================================================================

SET search_path TO dw;

-- Vider dim_date existante
TRUNCATE TABLE dim_date CASCADE;

-- Générer uniquement les dates présentes dans fact_trip
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

-- Ajouter la contrainte FK maintenant que dim_date est remplie
-- (ignorer l'erreur si elle existe déjà)
DO $$
BEGIN
    ALTER TABLE fact_trip ADD CONSTRAINT fk_fact_trip_date
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id);
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'FK constraint already exists';
END $$;

-- Afficher le résumé
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
