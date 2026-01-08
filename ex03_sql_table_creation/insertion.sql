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

-- 4) dim_date (On génère par exemple 5 ans de dates)
-- Astuce SQL pour générer un calendrier sans le taper à la main
-- 4) dim_date (On étend à 10 ans pour couvrir jusqu'à 2030)
INSERT INTO dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    TO_CHAR(datum, 'Month') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 365*10) AS SEQUENCE(DAY)) DQ -- <--- MODIFICATION ICI (365*10)
ORDER BY 1
ON CONFLICT (date_id) DO NOTHING;

-- 5) dim_location
-- ATTENTION : Normalement, ceci vient du fichier taxi_zone_lookup.csv.
-- Pour l'exercice SQL, tu peux soit utiliser la commande COPY si le fichier est dans le conteneur,
-- soit insérer quelques valeurs fictives pour tester, soit charger le CSV via Python/Spark.
-- Voici la commande idéale si tu copies le csv dans le conteneur Docker :
-- COPY dim_location(location_id, borough, zone, service_zone)
-- FROM '/taxi_zone_lookup.csv' DELIMITER ',' CSV HEADER;