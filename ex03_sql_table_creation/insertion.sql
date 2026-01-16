-- ============================================================================
--  insertion.sql
--  Population des dimensions statiques (Reference Data)
-- ============================================================================

SET search_path TO dw;

-- 1) dim_vendor
-- Les IDs sont standards dans le dataset NYC TLC
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
                                                    (1, 'Creative Mobile Technologies, LLC'),
                                                    (2, 'VeriFone Inc.'),
                                                    (6, 'Myle Technologies Inc'),
                                                    (7, 'Helix')
ON CONFLICT (vendor_id) DO NOTHING;

-- 2) dim_ratecode
-- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride
INSERT INTO dim_ratecode (ratecode_id, description) VALUES
                                                        (1, 'Standard rate'),
                                                        (2, 'JFK'),
                                                        (3, 'Newark'),
                                                        (4, 'Nassau or Westchester'),
                                                        (5, 'Negotiated fare'),
                                                        (6, 'Group ride'),
                                                        (99, 'Unknown')
ON CONFLICT (ratecode_id) DO NOTHING;

-- 3) dim_payment_type
-- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
INSERT INTO dim_payment_type (payment_type_id, description) VALUES
                                                                (0,'Flex Fare trip'),
                                                                (1, 'Credit card'),
                                                                (2, 'Cash'),
                                                                (3, 'No charge'),
                                                                (4, 'Dispute'),
                                                                (5, 'Unknown'),
                                                                (6, 'Voided trip')

ON CONFLICT (payment_type_id) DO NOTHING;

-- 4) dim_date - Sera générée automatiquement après chargement de fact_trip
-- (voir populate_dim_date.sql qui extrait les dates réelles des données)

-- 5) dim_location
-- Le fichier taxi_zone_lookup.csv doit être copié dans /tmp/ du conteneur PostgreSQL
-- avant d'exécuter ce script (voir run_spark_docker.sh)
COPY dim_location(location_id, borough, zone, service_zone)
FROM '/tmp/taxi_zone_lookup.csv' DELIMITER ',' CSV HEADER;

-- ============================================================================
--  6) Vérification des dimensions chargées
-- ============================================================================

DO $$
DECLARE
    v_vendor_count INTEGER;
    v_ratecode_count INTEGER;
    v_payment_count INTEGER;
    v_date_count INTEGER;
    v_location_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_vendor_count FROM dim_vendor;
    SELECT COUNT(*) INTO v_ratecode_count FROM dim_ratecode;
    SELECT COUNT(*) INTO v_payment_count FROM dim_payment_type;
    SELECT COUNT(*) INTO v_date_count FROM dim_date;  -- Sera 0 ici, rempli après fact_trip
    SELECT COUNT(*) INTO v_location_count FROM dim_location;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Dimensions Reference Data - Summary';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'dim_vendor:       % rows', v_vendor_count;
    RAISE NOTICE 'dim_ratecode:     % rows', v_ratecode_count;
    RAISE NOTICE 'dim_payment_type: % rows', v_payment_count;
    RAISE NOTICE 'dim_date:         % rows', v_date_count;
    RAISE NOTICE 'dim_location:     % rows', v_location_count;
    RAISE NOTICE '========================================';

    -- Assertions pour garantir l'intégrité
    IF v_vendor_count < 2 THEN
        RAISE EXCEPTION 'Insufficient vendors loaded (expected >= 2, got %)', v_vendor_count;
    END IF;

    IF v_ratecode_count < 6 THEN
        RAISE EXCEPTION 'Insufficient ratecodes loaded (expected >= 6, got %)', v_ratecode_count;
    END IF;

    IF v_payment_count < 6 THEN
        RAISE EXCEPTION 'Insufficient payment types loaded (expected >= 6, got %)', v_payment_count;
    END IF;

    -- dim_date sera remplie après fact_trip, pas de vérification ici
    IF v_date_count > 0 THEN
        RAISE NOTICE 'dim_date already has % rows', v_date_count;
    END IF;

    IF v_location_count < 260 THEN
        RAISE WARNING 'Location count seems low (expected ~265, got %). Check taxi_zone_lookup.csv', v_location_count;
    END IF;
END $$;