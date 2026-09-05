-- =============================================================================
-- Hive Integration: External Table Definitions & Analytical Queries
-- Database: india_crime_db
-- =============================================================================

CREATE DATABASE IF NOT EXISTS india_crime_db;
USE india_crime_db;

-- -----------------------------------------------------------------------------
-- 1. External Table: Raw Ingested Crime Records
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS crimes_raw (
    DISTRICT STRING,
    MURDER INT,
    ATTEMPT_TO_MURDER INT,
    CULPABLE_HOMICIDE INT,
    RAPE INT,
    KIDNAPPING_ABDUCTION INT,
    DACOITY INT,
    ROBBERY INT,
    BURGLARY INT,
    THEFT INT,
    AUTO_THEFT INT,
    RIOTS INT,
    CHEATING INT,
    ARSON INT,
    HURT INT,
    DOWRY_DEATHS INT,
    ASSAULT_ON_WOMEN INT,
    INSULT_TO_MODESTY_OF_WOMEN INT,
    CRUELTY_BY_HUSBAND INT,
    TOTAL_IPC_CRIMES INT
)
PARTITIONED BY (YEAR INT, STATE_UT STRING)
STORED AS PARQUET
LOCATION '/data/crimes/';

-- -----------------------------------------------------------------------------
-- 2. External Table: Cleaned Crime Records with Geospatial Coordinates
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS crimes_clean (
    DISTRICT STRING,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    MURDER INT,
    ATTEMPT_TO_MURDER INT,
    CULPABLE_HOMICIDE INT,
    RAPE INT,
    KIDNAPPING_ABDUCTION INT,
    DACOITY INT,
    ROBBERY INT,
    BURGLARY INT,
    THEFT INT,
    AUTO_THEFT INT,
    RIOTS INT,
    CHEATING INT,
    ARSON INT,
    HURT INT,
    DOWRY_DEATHS INT,
    ASSAULT_ON_WOMEN INT,
    INSULT_TO_MODESTY_OF_WOMEN INT,
    CRUELTY_BY_HUSBAND INT,
    TOTAL_IPC_CRIMES INT
)
PARTITIONED BY (YEAR INT, STATE_UT STRING)
STORED AS PARQUET
LOCATION '/data/crimes_clean/';

-- -----------------------------------------------------------------------------
-- 3. External Table: Feature-Engineered Dataset for ML
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS crimes_features (
    DISTRICT STRING,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    TOTAL_IPC_CRIMES INT,
    VIOLENT_CRIMES INT,
    PROPERTY_CRIMES INT,
    WOMEN_CRIMES INT,
    ECONOMIC_CRIMES INT,
    OTHER_CRIMES INT,
    VIOLENT_CRIME_RATIO DOUBLE,
    PROPERTY_CRIME_RATIO DOUBLE,
    WOMEN_CRIME_RATIO DOUBLE,
    ECONOMIC_CRIME_RATIO DOUBLE,
    DISTRICT_RISK_SCORE DOUBLE,
    STATE_RISK_SCORE DOUBLE,
    HIGH_SEVERITY_FLAG INT,
    LOCATION_CLUSTER INT
)
PARTITIONED BY (YEAR INT, STATE_UT STRING)
STORED AS PARQUET
LOCATION '/data/crimes_features/';

-- =============================================================================
-- Analytical Hive Queries
-- =============================================================================

-- Query 1: Top 10 High Crime Districts by Total IPC Crimes
SELECT 
    STATE_UT, 
    DISTRICT, 
    SUM(TOTAL_IPC_CRIMES) AS total_crimes,
    SUM(VIOLENT_CRIMES) AS total_violent,
    ROUND(SUM(VIOLENT_CRIMES) * 100.0 / SUM(TOTAL_IPC_CRIMES), 2) AS violent_percentage
FROM crimes_features
GROUP BY STATE_UT, DISTRICT
ORDER BY total_crimes DESC
LIMIT 10;

-- Query 2: Year-wise Total Crime and Category Trends Across India
SELECT 
    YEAR,
    SUM(TOTAL_IPC_CRIMES) AS national_total_crimes,
    SUM(VIOLENT_CRIMES) AS national_violent_crimes,
    SUM(PROPERTY_CRIMES) AS national_property_crimes,
    SUM(WOMEN_CRIMES) AS national_crimes_against_women,
    ROUND(AVG(DISTRICT_RISK_SCORE), 2) AS avg_district_risk
FROM crimes_features
GROUP BY YEAR
ORDER BY YEAR ASC;

-- Query 3: State-Wise Violent Crime vs Property Crime Distribution
SELECT 
    STATE_UT,
    SUM(VIOLENT_CRIMES) AS total_violent,
    SUM(PROPERTY_CRIMES) AS total_property,
    SUM(WOMEN_CRIMES) AS total_women_crimes,
    SUM(TOTAL_IPC_CRIMES) AS total_crimes,
    ROUND(SUM(VIOLENT_CRIMES) * 1.0 / NULLIF(SUM(PROPERTY_CRIMES), 0), 3) AS violent_to_property_ratio
FROM crimes_features
GROUP BY STATE_UT
ORDER BY total_crimes DESC
LIMIT 15;
