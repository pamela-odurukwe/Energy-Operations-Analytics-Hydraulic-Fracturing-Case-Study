-- Step 1: Create database
CREATE DATABASE IF NOT EXISTS fracfocus_db;
USE fracfocus_db;

-- Step 2: Table creation
-- Watersource table
DROP TABLE IF EXISTS WaterSource;
CREATE TABLE WaterSource (
    WaterSourceId VARCHAR(36) NOT NULL,
    DisclosureId VARCHAR(36) NOT NULL,
    APINumber VARCHAR(30),
    StateName VARCHAR(255),
    CountyName VARCHAR(255),
    OperatorName VARCHAR(255),
    WellName VARCHAR(255),
    Description VARCHAR(255),
    Percent DECIMAL(10,2),
    PRIMARY KEY (WaterSourceId)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/WaterSource_1.csv' 
INTO TABLE WaterSource
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(WaterSourceId, DisclosureId, APINumber, StateName, CountyName, OperatorName, WellName, Description, @raw_percent)
SET Percent = NULLIF(REPLACE(@raw_percent, '%', ''), '');

-- Disclosure_List table
DROP TABLE IF EXISTS DisclosureList_1;
CREATE TABLE DisclosureList_1 (
    DisclosureId VARCHAR(50) NOT NULL,
    JobStartDate_Raw VARCHAR(50), -- Temporary string hold for the date
    JobEndDate_Raw VARCHAR(50),   -- Temporary string hold for the date
    JobStartDate DATETIME,        -- The final clean date
    JobEndDate DATETIME,          -- The final clean date
    APINumber VARCHAR(50),
    StateName VARCHAR(50),
    CountyName VARCHAR(50),
    OperatorName VARCHAR(100),
    WellName VARCHAR(100),
    Latitude DECIMAL (12,9),
    Longitude DECIMAL (12,9),
    Projection VARCHAR(50),
    TVD INT,
    TotalBaseWaterVolume BIGINT,
    TotalBaseNonWaterVolume BIGINT,
    FFVersion INT,
    FederalWell VARCHAR(50),
    IndianWell VARCHAR(50),
    PRIMARY KEY (DisclosureId)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DisclosureList_1.csv' 
INTO TABLE DisclosureList_1
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, @raw_lat, @raw_long, Projection,  @raw_tvd, @raw_water_vol, @raw_non_water_vol, FFVersion, FederalWell, IndianWell)
SET 
    -- 1. Format the dates
    JobStartDate = STR_TO_DATE(NULLIF(@raw_start, ''), '%c/%e/%Y %r'),
    JobEndDate = STR_TO_DATE(NULLIF(@raw_end, ''), '%c/%e/%Y %r'),
    
    -- 2. Handle empty integers (convert '' to NULL)
    TVD = NULLIF(@raw_tvd, ''),
    TotalBaseWaterVolume = NULLIF(@raw_water_vol, ''),
    TotalBaseNonWaterVolume = NULLIF(@raw_non_water_vol, ''),
    
    -- 3. Handle missing Decimals (Latitude/Longitude)
    Latitude = NULLIF(@raw_lat, ''),
    Longitude = NULLIF(@raw_long, '');

-- Registry tables 1- 15
DROP TABLE IF EXISTS Registry;

CREATE TABLE Registry (
    DisclosureId VARCHAR(50) NOT NULL,
    JobStartDate_Raw VARCHAR(50), 
    JobEndDate_Raw VARCHAR(50),   
    JobStartDate DATETIME,        
    JobEndDate DATETIME,          
    APINumber VARCHAR(50),
    StateName VARCHAR(50),
    CountyName VARCHAR(50),
    OperatorName VARCHAR(100),
    WellName VARCHAR(255),
    Latitude DECIMAL(12,9),
    Longitude DECIMAL(12,9),
    Projection VARCHAR(50),
    TVD INT,
    TotalBaseWaterVolume BIGINT,
    TotalBaseNonWaterVolume BIGINT,
    FFVersion INT,
    FederalWell VARCHAR(50),
    IndianWell VARCHAR(50),
    PurposeId VARCHAR(50),
    TradeName VARCHAR(300),
    Supplier VARCHAR(300),
    Purpose VARCHAR(500),
    IngredientsId VARCHAR(50),
    CASNumber VARCHAR(50),
    IngredientName VARCHAR(500),
    IngredientCommonName VARCHAR(500),
    PercentHighAdditive VARCHAR(50),
    PercentHFJob VARCHAR(50),
    IngredientComment VARCHAR(1000),
    IngredientMSDS VARCHAR(50),
    MassIngredient DECIMAL(15,4), 
    ClaimantCompany VARCHAR(100)
);
    -- Step 1: Clear the existing 'dirty' data
TRUNCATE TABLE Registry;

-- Step 2: Update the numeric columns to handle high precision
ALTER TABLE Registry 
MODIFY COLUMN MassIngredient DOUBLE,
MODIFY COLUMN PercentHighAdditive DOUBLE,
MODIFY COLUMN PercentHFJob DOUBLE,
--  Latitude/Longitude are also optimized
MODIFY COLUMN Latitude DOUBLE,
MODIFY COLUMN Longitude DOUBLE,
MODIFY COLUMN ClaimantCompany VARCHAR(255);

SET SESSION sql_mode = '';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_1.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_2.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
   
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_3.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_4.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_5.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_6.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_7.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_8.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_9.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_10.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_11.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_12.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_13.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_14.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
    LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/FracFocusRegistry_15.csv' 
INTO TABLE Registry 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS 
(
  DisclosureId, @raw_start, @raw_end, APINumber, StateName, CountyName, OperatorName, WellName, 
  @raw_lat, @raw_long, Projection, @raw_tvd, @raw_water, @raw_non_water, FFVersion, FederalWell, 
  IndianWell, PurposeId, TradeName, Supplier, Purpose, IngredientsId, CASNumber, IngredientName, 
  IngredientCommonName, @raw_high_add, @raw_hf_job, IngredientComment, IngredientMSDS, 
  @raw_mass, ClaimantCompany
) 
SET  
    -- Date Formatting
    JobStartDate = STR_TO_DATE(NULLIF(TRIM(@raw_start), ''), '%c/%e/%Y %r'), 
    JobEndDate = STR_TO_DATE(NULLIF(TRIM(@raw_end), ''), '%c/%e/%Y %r'), 
    
    -- Numeric Data (Using DOUBLE for precision)
    Latitude = NULLIF(TRIM(@raw_lat), ''),
    Longitude = NULLIF(TRIM(@raw_long), ''),
    TVD = NULLIF(TRIM(@raw_tvd), ''), 
    TotalBaseWaterVolume = NULLIF(TRIM(@raw_water), ''), 
    TotalBaseNonWaterVolume = NULLIF(TRIM(@raw_non_water), ''),
    PercentHighAdditive = NULLIF(TRIM(@raw_high_add), ''),
    PercentHFJob = NULLIF(TRIM(@raw_hf_job), ''),
    MassIngredient = NULLIF(TRIM(@raw_mass), '');
    
-- This adds a new column 'id' at the very beginning of the table 
-- and automatically fills it with 1, 2, 3...
ALTER TABLE Registry 
ADD COLUMN ID INT AUTO_INCREMENT FIRST,
ADD PRIMARY KEY (ID);

-- the shape of the data
DESCRIBE Registry;
DESCRIBE disclosurelist_1;
DESCRIBE watersource;

-- the initial assessment
SELECT * FROM watersource;
SELECT * FROM disclosurelist_1;
SELECT * FROM registry;

-- check for duplicates
SELECT WaterSourceId, COUNT(*)
FROM WaterSource
GROUP BY WaterSourceId
HAVING COUNT(*) > 1;

CREATE INDEX idx_disclosure_id ON Registry (DisclosureId);

-- exploring the data
SELECT COUNT(*)
FROM registry;

SELECT COUNT(DISTINCT StateName) FROM Registry;
SELECT COUNT(DISTINCT OperatorName) FROM Registry;

SELECT 
    WellName, 
    SUM(Percent) AS Total_Percentage
FROM WaterSource
GROUP BY WellName;

SELECT 
    OperatorName, 
    WellName, 
    Description, 
    Percent
FROM WaterSource
WHERE StateName = 'Texas';

SELECT 
    OperatorName, 
    Description AS Source_Type, 
    COUNT(*) AS Number_of_Records,
    ROUND(AVG(Percent), 2) AS Avg_Composition_Percentage
FROM WaterSource
GROUP BY OperatorName, Description
ORDER BY OperatorName ASC, Avg_Composition_Percentage DESC;

-- to verify the "health" of the relationship between your tables before building visuals in Power BI. Zero orphaned rows means  Registry and Disclosure tables are perfectly synced
SELECT 
    (SELECT COUNT(*) FROM Registry) AS Total_Registry_Rows,
    (SELECT COUNT(*) FROM Registry r 
     LEFT JOIN DisclosureList_1 d ON r.DisclosureId = d.DisclosureId 
     WHERE d.DisclosureId IS NULL) AS Orphaned_Registry_Rows;
     
SELECT 
    (SELECT COUNT(*) FROM WaterSource) AS Total_Water_Rows,
    (SELECT COUNT(*) FROM WaterSource w 
     LEFT JOIN DisclosureList_1 d ON w.DisclosureId = d.DisclosureId 
     WHERE d.DisclosureId IS NULL) AS Orphaned_Water_Rows;

-- the chemical fact view
CREATE OR REPLACE VIEW registry_view AS
SELECT
    d.DisclosureId,
    d.APINumber,
    d.WellName,
    d.OperatorName,
    d.StateName,
    d.Latitude,
    d.Longitude,
    d.JobStartDate,
    d.JobEndDate,
    CASE
        WHEN d.JobStartDate IS NOT NULL AND d.JobEndDate IS NOT NULL
             AND d.JobEndDate >= d.JobStartDate
        THEN DATEDIFF(d.JobEndDate, d.JobStartDate)
        ELSE NULL
    END AS Job_Duration_Days,
    d.TVD,
    d.TotalBaseWaterVolume,
    d.TotalBaseWaterVolume / NULLIF(d.TVD, 0) AS Water_Intensity_Gal_Ft,

    CASE
        WHEN UPPER(TRIM(r.Supplier)) LIKE '%BAKER%' THEN 'BAKER HUGHES'
        WHEN UPPER(TRIM(r.Supplier)) IN ('N/A', 'LISTED ABOVE', 'MULTIPLE SUPPLIERS') THEN 'NON-DISCLOSED'
        WHEN r.Supplier IS NULL OR TRIM(r.Supplier) = '' THEN 'MISSING DATA'
        ELSE UPPER(TRIM(r.Supplier))
    END AS Standardized_Supplier,

    UPPER(TRIM(r.IngredientName)) AS Clean_Ingredient_Name,
    r.MassIngredient,
    r.Purpose
FROM DisclosureList_1 d
JOIN Registry r
  ON d.DisclosureId = r.DisclosureId
WHERE
    r.MassIngredient > 0
    AND d.TVD > 0
    AND d.JobStartDate <= CURDATE();

-- This allows you to analyze water origins without affecting the chemical totals.
CREATE OR REPLACE VIEW watersource_view AS
SELECT 
    d.DisclosureId,
    w.Description AS Water_Type,
    w.Percent AS Water_Source_Percent
FROM DisclosureList_1 d
JOIN WaterSource w ON d.DisclosureId = w.DisclosureId;

-- Instead of pulling WellName, Latitude, and JobDates into both your Chemical and Water views, you should have one single "Master List" of Wells.

-- Top 10 Chemicals
SELECT 
    IngredientName, 
    CASNumber, 
    COUNT(*) AS Frequency,
    ROUND(SUM(MassIngredient) / 2000, 2) AS Total_Mass_Tons
FROM Registry
WHERE IngredientName NOT IN ('Water', 'Sand', 'Silica Substrate') -- Filter out the obvious stuff
GROUP BY IngredientName, CASNumber
ORDER BY Total_Mass_Tons DESC
LIMIT 10;

-- It calculates how much water is being used per foot of depth ($TVD$).
SELECT 
    StateName,
    CountyName,
    FORMAT(AVG(TotalBaseWaterVolume), 0) AS Avg_Water_Per_Well_Gallons,
    FORMAT(AVG(TVD), 0) AS Avg_Depth_FT,
    ROUND(AVG(TotalBaseWaterVolume) / NULLIF(AVG(TVD), 0), 2) AS Gallons_Per_Foot_Depth
FROM DisclosureList_1
WHERE TotalBaseWaterVolume > 0 AND TVD > 0
GROUP BY StateName, CountyName
ORDER BY AVG(TotalBaseWaterVolume) DESC
LIMIT 10;

SELECT 
    MIN(JobStartDate) AS first_date,
    MAX(JobStartDate) AS last_date
FROM disclosurelist_1
WHERE YEAR(JobStartDate) < curdate(); 
#Operator Efficiency
SELECT 
    d.OperatorName,
    FORMAT(COUNT(DISTINCT d.DisclosureId), 0) AS Well_Count,
    FORMAT(AVG(d.TVD), 0) AS Avg_Depth_FT,
    FORMAT(SUM(r.MassIngredient) / 2000, 0) AS Total_Chemicals_Tons
FROM DisclosureList_1 d
JOIN Registry r ON d.DisclosureId = r.DisclosureId
GROUP BY d.OperatorName
ORDER BY SUM(r.MassIngredient) DESC
LIMIT 10;

-- Time-Series" Trend Since we cleaned those dates, we can now see if chemical usage is increasing year-over-year.
SELECT 
    YEAR(JobStartDate) AS Job_Year,
    COUNT(DISTINCT DisclosureId) AS Total_Jobs,
    ROUND(SUM(MassIngredient) / 2000, 2) AS Annual_Mass_Tons
FROM Registry
WHERE JobStartDate IS NOT NULL 
  AND YEAR(JobStartDate) BETWEEN 2010 AND 2024 -- Filtering out date outliers
GROUP BY Job_Year
ORDER BY Job_Year;

-- companies often list ingredients as "Proprietary" or "Trade Secret" to protect their formulas. Highlighting this shows you understand the industry's competitive landscape.
SELECT 
    OperatorName,
    COUNT(*) AS Total_Ingredients,
    SUM(CASE WHEN IngredientName LIKE '%Proprietary%' 
              OR IngredientName LIKE '%Confidential%' 
              OR IngredientName LIKE '%Trade Secret%' THEN 1 ELSE 0 END) AS Secret_Ingredients,
    ROUND((SUM(CASE WHEN IngredientName LIKE '%Proprietary%' 
                    OR IngredientName LIKE '%Confidential%' 
                    OR IngredientName LIKE '%Trade Secret%' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS Percent_Secret
FROM Registry
GROUP BY OperatorName
HAVING Total_Ingredients > 1000 -- Focus on major players
ORDER BY Percent_Secret DESC
LIMIT 10;

CREATE OR REPLACE VIEW disclosure_view AS
SELECT 
    DisclosureId,
    APINumber,
    WellName,
    OperatorName,
    StateName,
    CountyName,
    Latitude,
    Longitude,
    FederalWell,
    IndianWell,
    JobStartDate,
    JobEndDate,
    DATEDIFF(JobEndDate, JobStartDate) AS Job_Duration_Days,
    TVD,
    TotalBaseWaterVolume,
    TotalBaseNonWaterVolume,
    CASE 
        WHEN TVD > 0 THEN TotalBaseWaterVolume / TVD 
        ELSE 0 
    END AS Water_Intensity_Gal_Ft
FROM DisclosureList_1
WHERE TVD > 0
AND JobStartDate <= CURDATE();

#to confirm jobs with erroenous dates
SELECT JobStartDate, COUNT(*) 
FROM registry 
WHERE YEAR(JobStartDate) > 2026 
GROUP BY JobStartDate;

CREATE INDEX idx_disclosure_id ON Registry (DisclosureId);

CREATE TABLE Registry_Lookup AS

SELECT * FROM registry_lookup;

CREATE OR REPLACE VIEW v_analytical_master AS
SELECT 
    -- 1. Full Well Identification
    d.DisclosureId,
    d.APINumber,
    d.WellName,
    d.OperatorName,
    d.StateName,
    d.CountyName,
    d.Latitude,
    d.Longitude,
    d.FederalWell,
    d.IndianWell,
    
    -- 2. Complete Timeline
    d.JobStartDate,
    d.JobEndDate,
    DATEDIFF(d.JobEndDate, d.JobStartDate) AS Job_Duration_Days,

    -- 3. Well Technical Specs
    d.TVD,
    d.TotalBaseWaterVolume,
    d.TotalBaseNonWaterVolume,

    -- 4. Standardized Supplier & Chemical Data
    CASE 
        WHEN r.Supplier LIKE '%BAKER%' THEN 'BAKER HUGHES'
        WHEN r.Supplier IN ('N/A', 'Listed Above', 'Multiple Suppliers') THEN 'NON-DISCLOSED'
        WHEN r.Supplier IS NULL OR r.Supplier = '' THEN 'MISSING DATA'
        ELSE UPPER(TRIM(r.Supplier)) 
    END AS Standardized_Supplier,
    
    UPPER(TRIM(r.IngredientName)) AS Clean_Ingredient_Name,
    r.CASNumber,
    r.Purpose,
    r.MassIngredient,
    r.PercentHighAdditive,

    -- 5. Water Source Details
    w.Description AS Water_Type,
    w.Percent AS Water_Source_Percent,

    -- 6. Calculated Efficiency Metrics
    CASE 
        WHEN d.TVD > 0 THEN d.TotalBaseWaterVolume / d.TVD 
        ELSE 0 
    END AS Water_Intensity_Gal_Ft

FROM DisclosureList_1 d
JOIN Registry r ON d.DisclosureId = r.DisclosureId
LEFT JOIN WaterSource w ON d.DisclosureId = w.DisclosureId
WHERE r.MassIngredient > 0 
  AND d.TVD > 0;