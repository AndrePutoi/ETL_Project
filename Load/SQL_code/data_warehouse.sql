CREATE TABLE DimCountry
(
  IS03_Code VARCHAR(10) PRIMARY KEY,
  Country VARCHAR(100) NOT NULL,
  Region VARCHAR(100) NOT NULL,
  Income_Level VARCHAR(50) NOT NULL,
  Lending_Type VARCHAR(50) NOT NULL,
  Capital VARCHAR(100) NOT NULL,
  Latitude DECIMAL(9,6) NOT NULL,
  Longitude DECIMAL(9,6) NOT NULL,
  Continent VARCHAR(100) NOT NULL
);

CREATE TABLE DimTime
(
  YEAR INT PRIMARY KEY,
  Decade INT NOT NULL,
  Century INT NOT NULL,
  Leap_Year BIT NOT NULL,
  Decade_Pos INT NOT NULL
);

CREATE TABLE DimIndicator
(
  WB_Code VARCHAR(20) PRIMARY KEY,
  Indicator_name VARCHAR(200) NOT NULL,
  Description TEXT NOT NULL,
  Source VARCHAR(100) NOT NULL,
  Periodicity VARCHAR(50) NOT NULL,
  Topics VARCHAR(200) NOT NULL
);

CREATE TABLE FactIndicators
(
  Value DECIMAL(28,10) NOT NULL,
  YEAR INT NOT NULL,
  WB_Code VARCHAR(20) NOT NULL,
  IS03_Code VARCHAR(10) NOT NULL,
  FOREIGN KEY (YEAR) REFERENCES DimTime(YEAR),
  FOREIGN KEY (WB_Code) REFERENCES DimIndicator(WB_Code),
  FOREIGN KEY (IS03_Code) REFERENCES DimCountry(IS03_Code)
);

CREATE INDEX idx_fact_year ON FactIndicators(YEAR);
CREATE INDEX idx_fact_wb_code ON FactIndicators(WB_Code);
CREATE INDEX idx_fact_country ON FactIndicators(IS03_Code);