--Create Database
CREATE DATABASE SDG_DATA;
GO

USE SDG_DATA;
GO

--check columns
SELECT TOP 5 * FROM EG_FEC_RNEW;
SELECT TOP 5 * FROM EN_ATM_GHGT_AIP;
SELECT TOP 5 * FROM EN_ATM_GHGT_NAIP;


--Check Nulls
--get column names
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_NAME IN ('EG_FEC_RNEW', 'EN_ATM_GHGT_AIP', 'EN_ATM_GHGT_NAIP')
ORDER BY 
    TABLE_NAME, COLUMN_NAME;

--count nulls per column
DECLARE @tableName NVARCHAR(100);
DECLARE @columnName NVARCHAR(100);
DECLARE @sql NVARCHAR(MAX);

-- Temporary table to store results
IF OBJECT_ID('tempdb..#NullCounts') IS NOT NULL DROP TABLE #NullCounts;
CREATE TABLE #NullCounts (
    TableName NVARCHAR(100),
    ColumnName NVARCHAR(100),
    NullCount INT
);

-- Cursor to loop through all columns of the 3 tables
DECLARE column_cursor CURSOR FOR
SELECT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN ('EG_FEC_RNEW', 'EN_ATM_GHGT_AIP', 'EN_ATM_GHGT_NAIP');

OPEN column_cursor;
FETCH NEXT FROM column_cursor INTO @tableName, @columnName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'INSERT INTO #NullCounts (TableName, ColumnName, NullCount)
                SELECT ''' + @tableName + ''', ''' + @columnName + ''', COUNT(*) 
                FROM ' + @tableName + ' 
                WHERE [' + @columnName + '] IS NULL';

    EXEC sp_executesql @sql;

    FETCH NEXT FROM column_cursor INTO @tableName, @columnName;
END

CLOSE column_cursor;
DEALLOCATE column_cursor;

-- Show results
SELECT * FROM #NullCounts
ORDER BY TableName, ColumnName;

--Remove null columns
-- Drop unnecessary NULL columns from EG_FEC_RNEW
ALTER TABLE EG_FEC_RNEW
DROP COLUMN column33, column34, column35, column36, column37, column38, column39, 
            column40, column41, column42, column43, column44, column45, column46, 
            column47, column48, column49, column50, column51, column52;

-- Drop unnecessary NULL columns from EN_ATM_GHGT_AIP
ALTER TABLE EN_ATM_GHGT_AIP
DROP COLUMN column32, column33, column34, column35, column36, column37, column38,
            column39, column40, column41, column42, column43, column44, column45,
            column46, column47, column48, column49, column50, column51, column52;

-- Drop unnecessary NULL columns from EN_ATM_GHGT_NAIP
ALTER TABLE EN_ATM_GHGT_NAIP
DROP COLUMN column28, column29, column30, column31, column32, column33, column34,
            column35, column36, column37, column38, column39, column40, column41,
            column42, column43, column44, column45, column46, column47, column48,
            column49, column50, column51, column52;

--Doubell Check
--count nulls per column
DECLARE @tableName NVARCHAR(100);
DECLARE @columnName NVARCHAR(100);
DECLARE @sql NVARCHAR(MAX);

-- Temporary table to store results
IF OBJECT_ID('tempdb..#NullCounts') IS NOT NULL DROP TABLE #NullCounts;
CREATE TABLE #NullCounts (
    TableName NVARCHAR(100),
    ColumnName NVARCHAR(100),
    NullCount INT
);

-- Cursor to loop through all columns of the 3 tables
DECLARE column_cursor CURSOR FOR
SELECT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN ('EG_FEC_RNEW', 'EN_ATM_GHGT_AIP', 'EN_ATM_GHGT_NAIP');

OPEN column_cursor;
FETCH NEXT FROM column_cursor INTO @tableName, @columnName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'INSERT INTO #NullCounts (TableName, ColumnName, NullCount)
                SELECT ''' + @tableName + ''', ''' + @columnName + ''', COUNT(*) 
                FROM ' + @tableName + ' 
                WHERE [' + @columnName + '] IS NULL';

    EXEC sp_executesql @sql;

    FETCH NEXT FROM column_cursor INTO @tableName, @columnName;
END

CLOSE column_cursor;
DEALLOCATE column_cursor;

-- Show results
SELECT * FROM #NullCounts
ORDER BY TableName, ColumnName;

--Unpivot Year Columns
SELECT 
    GeoAreaCode AS Country_Code,
    GeoAreaName AS Country_Name,
    Indicator AS Indicator_Name,
    Units,
    REPLACE([Year], '_', '') AS Year,
    Value
FROM
(
    SELECT GeoAreaCode, GeoAreaName, Indicator, Units,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022]
    FROM EG_FEC_RNEW
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022])
) AS unpvt;

--Combine CO₂ Tables
-- Create a new table for combined CO2 emissions
IF OBJECT_ID('dbo.CO2_Emissions', 'U') IS NOT NULL
    DROP TABLE dbo.CO2_Emissions;

CREATE TABLE dbo.CO2_Emissions (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Indicator_Name NVARCHAR(255),
    Year INT,
    Value FLOAT,
    Units NVARCHAR(50),
    Country_Type NVARCHAR(50)
);

-- Insert Annex I CO2 emissions
INSERT INTO dbo.CO2_Emissions (Country_Code, Country_Name, Indicator_Name, Year, Value, Units, Country_Type)
SELECT 
    GeoAreaCode AS Country_Code,
    GeoAreaName AS Country_Name,
    Indicator AS Indicator_Name,
    CAST(REPLACE([Year], '_', '') AS INT) AS Year,
    Value,
    Units,
    'Annex I' AS Country_Type
FROM
(
    SELECT GeoAreaCode, GeoAreaName, Indicator, Units,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017]
    FROM EN_ATM_GHGT_AIP
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017])
) AS unpvt;

-- Insert Non-Annex I CO2 emissions
INSERT INTO dbo.CO2_Emissions (Country_Code, Country_Name, Indicator_Name, Year, Value, Units, Country_Type)
SELECT 
    GeoAreaCode AS Country_Code,
    GeoAreaName AS Country_Name,
    Indicator AS Indicator_Name,
    CAST(REPLACE([Year], '_', '') AS INT) AS Year,
    Value,
    Units,
    'Non-Annex I' AS Country_Type
FROM
(
    SELECT GeoAreaCode, GeoAreaName, Indicator, Units,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017]
    FROM EN_ATM_GHGT_NAIP
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017])
) AS unpvt;

-- Preview the combined table
SELECT TOP 50 * FROM dbo.CO2_Emissions
ORDER BY Country_Name, Year;


--Double-check the Renewable Energy table
-- Create table for Renewable Energy (long format)
IF OBJECT_ID('dbo.Renewable_Energy', 'U') IS NOT NULL
    DROP TABLE dbo.Renewable_Energy;

CREATE TABLE dbo.Renewable_Energy (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Indicator_Name NVARCHAR(255),
    Year INT,
    Value FLOAT,
    Units NVARCHAR(50)
);

-- Insert unpivoted data, only numeric values
INSERT INTO dbo.Renewable_Energy (Country_Code, Country_Name, Indicator_Name, Year, Value, Units)
SELECT 
    GeoAreaCode AS Country_Code,
    GeoAreaName AS Country_Name,
    Indicator AS Indicator_Name,
    CAST(REPLACE([Year], '_', '') AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value,
    Units
FROM
(
    SELECT GeoAreaCode, GeoAreaName, Indicator, Units,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022]
    FROM EG_FEC_RNEW
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022])
) AS unpvt
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;  -- filter out non-numeric values

-- Preview the data
SELECT TOP 50 * FROM dbo.Renewable_Energy
ORDER BY Country_Name, Year;

--Check Data Type
USE SDG_DATA;
GO

SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN (
    'Clean_GHG_AIP',
    'Clean_GHG_NAIP',
    'Clean_RenewableEnergy',
    'CO2_Emissions',
    'EG_FEC_RNEW',
    'EN_ATM_GHGT_AIP',
    'EN_ATM_GHGT_NAIP',
    'Renewable_Energy'
)
ORDER BY TABLE_NAME, COLUMN_NAME;
GO

--USE SDG_DATA;
GO

--===========================
-- 1. Clean CO2 Emissions (Combined)
--===========================
IF OBJECT_ID('dbo.CO2_Emissions_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.CO2_Emissions_Clean;

CREATE TABLE dbo.CO2_Emissions_Clean (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Country_Type NVARCHAR(50),
    Indicator_Name NVARCHAR(255),
    Units NVARCHAR(50),
    Year INT,
    Value FLOAT
);

-- Insert Annex I
INSERT INTO dbo.CO2_Emissions_Clean (Country_Code, Country_Name, Country_Type, Indicator_Name, Units, Year, Value)
SELECT 
    Country_Code,
    Country_Name,
    Country_Type,
    Indicator_Name,
    Units,
    CAST(Year AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM dbo.CO2_Emissions
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 2. Clean Renewable Energy
--===========================
IF OBJECT_ID('dbo.Renewable_Energy_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.Renewable_Energy_Clean;

CREATE TABLE dbo.Renewable_Energy_Clean (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Indicator_Name NVARCHAR(255),
    Units NVARCHAR(50),
    Year INT,
    Value FLOAT
);

INSERT INTO dbo.Renewable_Energy_Clean (Country_Code, Country_Name, Indicator_Name, Units, Year, Value)
SELECT 
    Country_Code,
    Country_Name,
    Indicator_Name,
    Units,
    CAST(Year AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM dbo.Renewable_Energy
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 3. Clean GHG Annex I
--===========================
IF OBJECT_ID('dbo.Clean_GHG_AIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_AIP;

CREATE TABLE dbo.Clean_GHG_AIP (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Country NVARCHAR(200),
    Year INT,
    Emissions FLOAT
);

INSERT INTO dbo.Clean_GHG_AIP (Country, Year, Emissions)
SELECT 
    Country,
    CAST(Year AS INT),
    TRY_CAST(Emissions AS FLOAT)
FROM dbo.Clean_GHG_AIP
WHERE TRY_CAST(Emissions AS FLOAT) IS NOT NULL;

--===========================
-- 4. Clean GHG Non-Annex I
--===========================
IF OBJECT_ID('dbo.Clean_GHG_NAIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_NAIP;

CREATE TABLE dbo.Clean_GHG_NAIP (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Country NVARCHAR(200),
    Year INT,
    Emissions FLOAT
);

INSERT INTO dbo.Clean_GHG_NAIP (Country, Year, Emissions)
SELECT 
    Country,
    CAST(Year AS INT),
    TRY_CAST(Emissions AS FLOAT)
FROM dbo.Clean_GHG_NAIP
WHERE TRY_CAST(Emissions AS FLOAT) IS NOT NULL;

--===========================
-- 5. Clean EG_FEC_RNEW Raw Data
--===========================
IF OBJECT_ID('dbo.EG_FEC_RNEW_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.EG_FEC_RNEW_Clean;

CREATE TABLE dbo.EG_FEC_RNEW_Clean (
    Country NVARCHAR(200),
    Year INT,
    Value FLOAT
);

-- Unpivot and convert to float
INSERT INTO dbo.EG_FEC_RNEW_Clean (Country, Year, Value)
SELECT 
    GeoAreaName AS Country,
    CAST(REPLACE([Year], '_', '') AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM
(
    SELECT GeoAreaName,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022]
    FROM EG_FEC_RNEW
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022])
) AS unpvt
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 6. Verify all cleaned tables
--===========================

SELECT TOP 10 * FROM dbo.CO2_Emissions_Clean ORDER BY Country_Name, Year;
SELECT TOP 10 * FROM dbo.Renewable_Energy_Clean ORDER BY Country_Name, Year;
SELECT TOP 10 * FROM dbo.Clean_GHG_AIP ORDER BY Country, Year;
SELECT TOP 10 * FROM dbo.Clean_GHG_NAIP ORDER BY Country, Year;
SELECT TOP 10 * FROM dbo.EG_FEC_RNEW_Clean ORDER BY Country, Year;


--full script to standardize all tables’ data types
USE SDG_DATA;
GO

--===========================
-- 1. Clean CO2 Emissions (Combined)
--===========================
IF OBJECT_ID('dbo.CO2_Emissions_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.CO2_Emissions_Clean;

CREATE TABLE dbo.CO2_Emissions_Clean (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Country_Type NVARCHAR(50),
    Indicator_Name NVARCHAR(255),
    Units NVARCHAR(50),
    Year INT,
    Value FLOAT
);

-- Insert Annex I
INSERT INTO dbo.CO2_Emissions_Clean (Country_Code, Country_Name, Country_Type, Indicator_Name, Units, Year, Value)
SELECT 
    Country_Code,
    Country_Name,
    Country_Type,
    Indicator_Name,
    Units,
    CAST(Year AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM dbo.CO2_Emissions
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 2. Clean Renewable Energy
--===========================
IF OBJECT_ID('dbo.Renewable_Energy_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.Renewable_Energy_Clean;

CREATE TABLE dbo.Renewable_Energy_Clean (
    Country_Code INT,
    Country_Name NVARCHAR(255),
    Indicator_Name NVARCHAR(255),
    Units NVARCHAR(50),
    Year INT,
    Value FLOAT
);

INSERT INTO dbo.Renewable_Energy_Clean (Country_Code, Country_Name, Indicator_Name, Units, Year, Value)
SELECT 
    Country_Code,
    Country_Name,
    Indicator_Name,
    Units,
    CAST(Year AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM dbo.Renewable_Energy
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 3. Clean GHG Annex I
--===========================
IF OBJECT_ID('dbo.Clean_GHG_AIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_AIP;

CREATE TABLE dbo.Clean_GHG_AIP (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Country NVARCHAR(200),
    Year INT,
    Emissions FLOAT
);

INSERT INTO dbo.Clean_GHG_AIP (Country, Year, Emissions)
SELECT 
    Country,
    CAST(Year AS INT),
    TRY_CAST(Emissions AS FLOAT)
FROM dbo.Clean_GHG_AIP
WHERE TRY_CAST(Emissions AS FLOAT) IS NOT NULL;

--===========================
-- 4. Clean GHG Non-Annex I
--===========================
IF OBJECT_ID('dbo.Clean_GHG_NAIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_NAIP;

CREATE TABLE dbo.Clean_GHG_NAIP (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Country NVARCHAR(200),
    Year INT,
    Emissions FLOAT
);

INSERT INTO dbo.Clean_GHG_NAIP (Country, Year, Emissions)
SELECT 
    Country,
    CAST(Year AS INT),
    TRY_CAST(Emissions AS FLOAT)
FROM dbo.Clean_GHG_NAIP
WHERE TRY_CAST(Emissions AS FLOAT) IS NOT NULL;

--===========================
-- 5. Clean EG_FEC_RNEW Raw Data
--===========================
IF OBJECT_ID('dbo.EG_FEC_RNEW_Clean', 'U') IS NOT NULL
    DROP TABLE dbo.EG_FEC_RNEW_Clean;

CREATE TABLE dbo.EG_FEC_RNEW_Clean (
    Country NVARCHAR(200),
    Year INT,
    Value FLOAT
);

-- Unpivot and convert to float
INSERT INTO dbo.EG_FEC_RNEW_Clean (Country, Year, Value)
SELECT 
    GeoAreaName AS Country,
    CAST(REPLACE([Year], '_', '') AS INT) AS Year,
    TRY_CAST(Value AS FLOAT) AS Value
FROM
(
    SELECT GeoAreaName,
           [_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
           [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
           [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022]
    FROM EG_FEC_RNEW
) p
UNPIVOT
(
    Value FOR [Year] IN 
    ([_2000], [_2001], [_2002], [_2003], [_2004], [_2005], [_2006], [_2007],
     [_2008], [_2009], [_2010], [_2011], [_2012], [_2013], [_2014], [_2015],
     [_2016], [_2017], [_2018], [_2019], [_2020], [_2021], [_2022])
) AS unpvt
WHERE TRY_CAST(Value AS FLOAT) IS NOT NULL;

--===========================
-- 6. Verify all cleaned tables
--===========================

SELECT TOP 10 * FROM dbo.CO2_Emissions_Clean ORDER BY Country_Name, Year;
SELECT TOP 10 * FROM dbo.Renewable_Energy_Clean ORDER BY Country_Name, Year;
SELECT TOP 10 * FROM dbo.Clean_GHG_AIP ORDER BY Country, Year;
SELECT TOP 10 * FROM dbo.Clean_GHG_NAIP ORDER BY Country, Year;
SELECT TOP 10 * FROM dbo.EG_FEC_RNEW_Clean ORDER BY Country, Year;

--Get the Row Count
USE SDG_DATA;
GO

SELECT 
    t.name AS Table_Name,
    SUM(p.rows) AS Row_Count
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.object_id = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE 
    t.name IN (
        'Clean_GHG_AIP',
        'Clean_GHG_NAIP',
        'Clean_RenewableEnergy',
        'CO2_Emissions',
        'EG_FEC_RNEW',
        'EN_ATM_GHGT_AIP',
        'EN_ATM_GHGT_NAIP',
        'Renewable_Energy',
        'CO2_Emissions_Clean',
        'Renewable_Energy_Clean',
        'EG_FEC_RNEW_Clean'
    )
GROUP BY 
    t.name
ORDER BY 
    t.name;

--Drop unwanted tables
USE SDG_DATA;
GO

-- Drop empty tables
IF OBJECT_ID('dbo.Clean_GHG_AIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_AIP;

IF OBJECT_ID('dbo.Clean_GHG_NAIP', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_GHG_NAIP;

IF OBJECT_ID('dbo.Clean_RenewableEnergy', 'U') IS NOT NULL
    DROP TABLE dbo.Clean_RenewableEnergy;


--Double Check
USE SDG_DATA;
GO

-- Temporary table to store table info
IF OBJECT_ID('tempdb..#TableInfo') IS NOT NULL DROP TABLE #TableInfo;
CREATE TABLE #TableInfo (
    TableName NVARCHAR(255),
    ColumnName NVARCHAR(255),
    DataType NVARCHAR(50),
    MaxLength INT,
    [RowCount] BIGINT,
    [NullCount] BIGINT
);

DECLARE @tableName NVARCHAR(255);
DECLARE @columnName NVARCHAR(255);
DECLARE @dataType NVARCHAR(50);
DECLARE @maxLength INT;
DECLARE @sql NVARCHAR(MAX);

-- Cursor for tables
DECLARE table_cursor CURSOR FOR
SELECT name 
FROM sys.tables
WHERE name IN (
    'CO2_Emissions',
    'CO2_Emissions_Clean',
    'EG_FEC_RNEW',
    'EG_FEC_RNEW_Clean',
    'EN_ATM_GHGT_AIP',
    'EN_ATM_GHGT_NAIP',
    'Renewable_Energy',
    'Renewable_Energy_Clean'
);

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @tableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Cursor for columns of the current table
    DECLARE column_cursor CURSOR FOR
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @tableName;

    OPEN column_cursor;
    FETCH NEXT FROM column_cursor INTO @columnName, @dataType, @maxLength;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = '
        INSERT INTO #TableInfo (TableName, ColumnName, DataType, MaxLength, [RowCount], [NullCount])
        SELECT 
            ''' + @tableName + ''', 
            ''' + @columnName + ''', 
            ''' + @dataType + ''', 
            ' + ISNULL(CAST(@maxLength AS NVARCHAR), 'NULL') + ', 
            (SELECT COUNT(*) FROM ' + @tableName + '), 
            (SELECT COUNT(*) FROM ' + @tableName + ' WHERE [' + @columnName + '] IS NULL)
        ';
        EXEC sp_executesql @sql;

        FETCH NEXT FROM column_cursor INTO @columnName, @dataType, @maxLength;
    END

    CLOSE column_cursor;
    DEALLOCATE column_cursor;

    FETCH NEXT FROM table_cursor INTO @tableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- Show results
SELECT * FROM #TableInfo
ORDER BY TableName, ColumnName;

