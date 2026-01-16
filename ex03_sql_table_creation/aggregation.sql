-- ============================================================================
--  aggregation.sql
--  Population des tables de faits agrégées depuis fact_trip
--  Ces tables optimisent les requêtes analytiques pré-calculées
-- ============================================================================

SET search_path TO dw;

-- ============================================================================
--  1) fact_vendor_daily : Agrégation par vendeur et par jour
-- ============================================================================

-- Vider la table avant réinsertion (idempotence)
TRUNCATE TABLE fact_vendor_daily;

INSERT INTO fact_vendor_daily (
    date_id,
    vendor_id,
    trips_count,
    total_fare_amount,
    total_tip_amount,
    total_total_amount,
    total_distance,
    avg_fare_amount,
    avg_tip_amount,
    avg_tip_ratio
)
SELECT
    ft.date_id,
    ft.vendor_id,
    COUNT(*) AS trips_count,
    SUM(ft.fare_amount) AS total_fare_amount,
    SUM(ft.tip_amount) AS total_tip_amount,
    SUM(ft.total_amount) AS total_total_amount,
    SUM(ft.trip_distance) AS total_distance,
    AVG(ft.fare_amount) AS avg_fare_amount,
    AVG(ft.tip_amount) AS avg_tip_amount,
    AVG(ft.tip_ratio) AS avg_tip_ratio
FROM fact_trip ft
WHERE ft.is_outlier = FALSE  -- Exclure les outliers des agrégations
GROUP BY ft.date_id, ft.vendor_id
ORDER BY ft.date_id, ft.vendor_id;

-- ============================================================================
--  2) fact_daily_pickup_zone : Agrégation par jour et zone de pickup
-- ============================================================================

TRUNCATE TABLE fact_daily_pickup_zone;

INSERT INTO fact_daily_pickup_zone (
    date_id,
    pickup_location_id,
    trips_count,
    total_fare_amount,
    total_tip_amount,
    total_total_amount,
    total_distance,
    avg_fare_amount,
    avg_tip_amount,
    avg_tip_ratio
)
SELECT
    ft.date_id,
    ft.pickup_location_id,
    COUNT(*) AS trips_count,
    SUM(ft.fare_amount) AS total_fare_amount,
    SUM(ft.tip_amount) AS total_tip_amount,
    SUM(ft.total_amount) AS total_total_amount,
    SUM(ft.trip_distance) AS total_distance,
    AVG(ft.fare_amount) AS avg_fare_amount,
    AVG(ft.tip_amount) AS avg_tip_amount,
    AVG(ft.tip_ratio) AS avg_tip_ratio
FROM fact_trip ft
WHERE ft.is_outlier = FALSE
  AND ft.pickup_location_id IS NOT NULL
GROUP BY ft.date_id, ft.pickup_location_id
ORDER BY ft.date_id, ft.pickup_location_id;

-- ============================================================================
--  3) fact_daily_dropoff_zone : Agrégation par jour et zone de dropoff
-- ============================================================================

TRUNCATE TABLE fact_daily_dropoff_zone;

INSERT INTO fact_daily_dropoff_zone (
    date_id,
    dropoff_location_id,
    trips_count,
    total_fare_amount,
    total_tip_amount,
    total_total_amount,
    total_distance,
    avg_fare_amount,
    avg_tip_amount,
    avg_tip_ratio
)
SELECT
    ft.date_id,
    ft.dropoff_location_id,
    COUNT(*) AS trips_count,
    SUM(ft.fare_amount) AS total_fare_amount,
    SUM(ft.tip_amount) AS total_tip_amount,
    SUM(ft.total_amount) AS total_total_amount,
    SUM(ft.trip_distance) AS total_distance,
    AVG(ft.fare_amount) AS avg_fare_amount,
    AVG(ft.tip_amount) AS avg_tip_amount,
    AVG(ft.tip_ratio) AS avg_tip_ratio
FROM fact_trip ft
WHERE ft.is_outlier = FALSE
  AND ft.dropoff_location_id IS NOT NULL
GROUP BY ft.date_id, ft.dropoff_location_id
ORDER BY ft.date_id, ft.dropoff_location_id;

-- ============================================================================
--  4) Vérification des agrégations
-- ============================================================================

DO $$
DECLARE
    v_fact_trip_count BIGINT;
    v_fact_trip_clean_count BIGINT;
    v_vendor_daily_count INTEGER;
    v_pickup_zone_count INTEGER;
    v_dropoff_zone_count INTEGER;
    v_vendor_daily_trips BIGINT;
    v_pickup_zone_trips BIGINT;
    v_dropoff_zone_trips BIGINT;
BEGIN
    -- Comptage des courses
    SELECT COUNT(*) INTO v_fact_trip_count FROM fact_trip;
    SELECT COUNT(*) INTO v_fact_trip_clean_count FROM fact_trip WHERE is_outlier = FALSE;

    -- Comptage des lignes agrégées
    SELECT COUNT(*) INTO v_vendor_daily_count FROM fact_vendor_daily;
    SELECT COUNT(*) INTO v_pickup_zone_count FROM fact_daily_pickup_zone;
    SELECT COUNT(*) INTO v_dropoff_zone_count FROM fact_daily_dropoff_zone;

    -- Comptage des trips dans les agrégations
    SELECT SUM(trips_count) INTO v_vendor_daily_trips FROM fact_vendor_daily;
    SELECT SUM(trips_count) INTO v_pickup_zone_trips FROM fact_daily_pickup_zone;
    SELECT SUM(trips_count) INTO v_dropoff_zone_trips FROM fact_daily_dropoff_zone;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Aggregation Summary';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'fact_trip (total):        % rows', v_fact_trip_count;
    RAISE NOTICE 'fact_trip (clean):        % rows', v_fact_trip_clean_count;
    RAISE NOTICE '';
    RAISE NOTICE 'fact_vendor_daily:        % rows (% trips)', v_vendor_daily_count, v_vendor_daily_trips;
    RAISE NOTICE 'fact_daily_pickup_zone:   % rows (% trips)', v_pickup_zone_count, v_pickup_zone_trips;
    RAISE NOTICE 'fact_daily_dropoff_zone:  % rows (% trips)', v_dropoff_zone_count, v_dropoff_zone_trips;
    RAISE NOTICE '========================================';

    -- Vérification de cohérence
    IF v_vendor_daily_trips != v_fact_trip_clean_count THEN
        RAISE WARNING 'Mismatch in vendor_daily aggregation: expected %, got %',
            v_fact_trip_clean_count, v_vendor_daily_trips;
    END IF;

    IF v_pickup_zone_trips != v_fact_trip_clean_count THEN
        RAISE WARNING 'Mismatch in pickup_zone aggregation: expected %, got %',
            v_fact_trip_clean_count, v_pickup_zone_trips;
    END IF;

    IF v_dropoff_zone_trips != v_fact_trip_clean_count THEN
        RAISE WARNING 'Mismatch in dropoff_zone aggregation: expected %, got %',
            v_fact_trip_clean_count, v_dropoff_zone_trips;
    END IF;

    -- Success
    RAISE NOTICE 'Aggregation completed successfully!';
END $$;

-- ============================================================================
--  5) Exemples de requêtes analytiques sur les tables agrégées
-- ============================================================================

-- Top 10 vendors par revenu total
-- SELECT
--     v.vendor_name,
--     SUM(fvd.total_total_amount) AS total_revenue,
--     SUM(fvd.trips_count) AS total_trips,
--     AVG(fvd.avg_fare_amount) AS avg_fare
-- FROM fact_vendor_daily fvd
-- JOIN dim_vendor v ON fvd.vendor_id = v.vendor_id
-- GROUP BY v.vendor_name
-- ORDER BY total_revenue DESC
-- LIMIT 10;

-- Top 10 zones de pickup par nombre de courses
-- SELECT
--     dl.borough,
--     dl.zone,
--     SUM(fdp.trips_count) AS total_trips,
--     SUM(fdp.total_total_amount) AS total_revenue,
--     AVG(fdp.avg_fare_amount) AS avg_fare
-- FROM fact_daily_pickup_zone fdp
-- JOIN dim_location dl ON fdp.pickup_location_id = dl.location_id
-- GROUP BY dl.borough, dl.zone
-- ORDER BY total_trips DESC
-- LIMIT 10;

-- Evolution journalière des courses (séries temporelles)
-- SELECT
--     dd.full_date,
--     dd.day_name,
--     dd.is_weekend,
--     SUM(fvd.trips_count) AS total_trips,
--     SUM(fvd.total_total_amount) AS total_revenue,
--     AVG(fvd.avg_tip_ratio) AS avg_tip_ratio
-- FROM fact_vendor_daily fvd
-- JOIN dim_date dd ON fvd.date_id = dd.date_id
-- GROUP BY dd.full_date, dd.day_name, dd.is_weekend
-- ORDER BY dd.full_date;

-- ============================================================================
--  Fin de aggregation.sql
-- ============================================================================
