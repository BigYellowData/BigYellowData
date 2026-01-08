-- ============================================================================
--  Data Warehouse NYC Taxi - creation.sql
--  Modèle en constellation avec 4 tables de faits
--  SGBD : PostgreSQL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw;

-- ============================================================================
--  0) Nettoyage préalable : DROP TABLES dans le bon ordre
-- ============================================================================

DROP TABLE IF EXISTS fact_daily_dropoff_zone;
DROP TABLE IF EXISTS fact_daily_pickup_zone;
DROP TABLE IF EXISTS fact_vendor_daily;
DROP TABLE IF EXISTS fact_trip;

DROP TABLE IF EXISTS dim_payment_type;
DROP TABLE IF EXISTS dim_ratecode;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_date;

-- ============================================================================
--  1) DIMENSIONS
-- ============================================================================

-- 1.1 dim_date : 1 ligne = 1 jour
CREATE TABLE dim_date (
                          date_id        INTEGER PRIMARY KEY,   -- ex: 20250901
                          full_date      DATE NOT NULL,
                          year           INTEGER NOT NULL,
                          month          INTEGER NOT NULL,
                          day            INTEGER NOT NULL,
                          day_of_week    INTEGER NOT NULL,      -- 1 = lundi (ISO)
                          day_name       VARCHAR(20),
                          month_name     VARCHAR(20),
                          is_weekend     BOOLEAN
);

-- 1.2 dim_location : 1 ligne = 1 zone TLC
CREATE TABLE dim_location (
                              location_id   INTEGER PRIMARY KEY,     -- = PULocationID / DOLocationID
                              borough       VARCHAR(50),
                              zone          VARCHAR(100),
                              service_zone  VARCHAR(50)
);

-- 1.3 dim_vendor
CREATE TABLE dim_vendor (
                            vendor_id    INTEGER PRIMARY KEY,
                            vendor_name  VARCHAR(100)
);

-- 1.4 dim_ratecode
CREATE TABLE dim_ratecode (
                              ratecode_id  INTEGER PRIMARY KEY,
                              description  VARCHAR(100)
);

-- 1.5 dim_payment_type
CREATE TABLE dim_payment_type (
                                  payment_type_id  INTEGER PRIMARY KEY,
                                  description      VARCHAR(50)
);

-- ============================================================================
--  2) FACT PRINCIPALE : fact_trip (grain = 1 course)
-- ============================================================================

CREATE TABLE fact_trip (
                           trip_id              BIGSERIAL PRIMARY KEY,

    -- Clés étrangères vers les dimensions
                           date_id              INTEGER NOT NULL REFERENCES dim_date(date_id),
                           vendor_id            INTEGER NOT NULL REFERENCES dim_vendor(vendor_id),
                           ratecode_id          INTEGER REFERENCES dim_ratecode(ratecode_id),
                           payment_type_id      INTEGER REFERENCES dim_payment_type(payment_type_id),
                           pickup_location_id   INTEGER REFERENCES dim_location(location_id),
                           dropoff_location_id  INTEGER REFERENCES dim_location(location_id),

    -- Timestamps détaillés
                           tpep_pickup_datetime   TIMESTAMP NOT NULL,
                           tpep_dropoff_datetime  TIMESTAMP NOT NULL,

    -- Attributs simples
                           passenger_count        INTEGER NOT NULL,
                           store_and_fwd_flag     CHAR(1),

    -- Mesures de la course
                           trip_distance          DOUBLE PRECISION,
                           fare_amount            DOUBLE PRECISION,
                           extra                  DOUBLE PRECISION,
                           mta_tax                DOUBLE PRECISION,
                           tip_amount             DOUBLE PRECISION,
                           tolls_amount           DOUBLE PRECISION,
                           improvement_surcharge  DOUBLE PRECISION,
                           congestion_surcharge   DOUBLE PRECISION,
                           airport_fee            DOUBLE PRECISION,
                           cbd_congestion_fee     DOUBLE PRECISION,
                           total_amount           DOUBLE PRECISION,

    -- Mesures dérivées
                           trip_duration_minutes  DOUBLE PRECISION,
                           avg_speed_mph          DOUBLE PRECISION,
                           tip_ratio              DOUBLE PRECISION,

    -- Flag qualité
                           is_outlier             BOOLEAN DEFAULT FALSE,
                            outlier_reason         VARCHAR(255)
);

CREATE INDEX idx_fact_trip_date        ON fact_trip(date_id);
CREATE INDEX idx_fact_trip_vendor      ON fact_trip(vendor_id);
CREATE INDEX idx_fact_trip_pickup_loc  ON fact_trip(pickup_location_id);
CREATE INDEX idx_fact_trip_dropoff_loc ON fact_trip(dropoff_location_id);
CREATE INDEX idx_fact_trip_payment     ON fact_trip(payment_type_id);

-- ============================================================================
--  3) FACT AGRÉGÉE : fact_vendor_daily (1 vendeur x 1 jour)
-- ============================================================================

CREATE TABLE fact_vendor_daily (
                                   date_id    INTEGER NOT NULL REFERENCES dim_date(date_id),
                                   vendor_id  INTEGER NOT NULL REFERENCES dim_vendor(vendor_id),

                                   trips_count         INTEGER,
                                   total_fare_amount   DOUBLE PRECISION,
                                   total_tip_amount    DOUBLE PRECISION,
                                   total_total_amount  DOUBLE PRECISION,
                                   total_distance      DOUBLE PRECISION,

                                   avg_fare_amount     DOUBLE PRECISION,
                                   avg_tip_amount      DOUBLE PRECISION,
                                   avg_tip_ratio       DOUBLE PRECISION,

                                   PRIMARY KEY (date_id, vendor_id)
);

-- ============================================================================
--  4) FACT AGRÉGÉE : fact_daily_pickup_zone (1 jour x 1 zone de pickup)
-- ============================================================================

CREATE TABLE fact_daily_pickup_zone (
                                        date_id            INTEGER NOT NULL REFERENCES dim_date(date_id),
                                        pickup_location_id INTEGER NOT NULL REFERENCES dim_location(location_id),

                                        trips_count         INTEGER,
                                        total_fare_amount   DOUBLE PRECISION,
                                        total_tip_amount    DOUBLE PRECISION,
                                        total_total_amount  DOUBLE PRECISION,
                                        total_distance      DOUBLE PRECISION,

                                        avg_fare_amount     DOUBLE PRECISION,
                                        avg_tip_amount      DOUBLE PRECISION,
                                        avg_tip_ratio       DOUBLE PRECISION,

                                        PRIMARY KEY (date_id, pickup_location_id)
);

-- ============================================================================
--  5) FACT AGRÉGÉE : fact_daily_dropoff_zone (1 jour x 1 zone de dropoff)
-- ============================================================================

CREATE TABLE fact_daily_dropoff_zone (
                                         date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),
                                         dropoff_location_id INTEGER NOT NULL REFERENCES dim_location(location_id),

                                         trips_count         INTEGER,
                                         total_fare_amount   DOUBLE PRECISION,
                                         total_tip_amount    DOUBLE PRECISION,
                                         total_total_amount  DOUBLE PRECISION,
                                         total_distance      DOUBLE PRECISION,

                                         avg_fare_amount     DOUBLE PRECISION,
                                         avg_tip_amount      DOUBLE PRECISION,
                                         avg_tip_ratio       DOUBLE PRECISION,

                                         PRIMARY KEY (date_id, dropoff_location_id)
);

-- ============================================================================
--  Fin de creation.sql
-- ============================================================================

