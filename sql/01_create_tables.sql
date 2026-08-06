USE CryptoDataPipeline;
GO

-- 1. Dimenzija: statični podaci o coin-u
CREATE TABLE dbo.DimCoin (
    CoinId VARCHAR(100) PRIMARY KEY,
    Symbol VARCHAR(20),
    Name VARCHAR(100),
    ImageUrl VARCHAR(500)
);

-- 2. Log pipeline-a
CREATE TABLE dbo.PipelineRunLog (
    BatchId INT IDENTITY(1,1) PRIMARY KEY,
    StartedAt DATETIME DEFAULT GETDATE(),
    FinishedAt DATETIME NULL,
    Status VARCHAR(20),          -- 'Running', 'Success', 'Failed'
    RowsInserted INT NULL,
    ErrorMessage VARCHAR(MAX) NULL
);

-- 3. Fact tabela: metrike koje se menjaju svakim povlacenjem
CREATE TABLE dbo.FactCoinMarketData (
    FactId BIGINT IDENTITY(1,1) PRIMARY KEY,
    CoinId VARCHAR(100) FOREIGN KEY REFERENCES dbo.DimCoin(CoinId),
    CurrentPrice DECIMAL(20,8),
    MarketCap BIGINT,
    MarketCapRank INT,
    TotalVolume BIGINT,
    High24h DECIMAL(20,8),
    Low24h DECIMAL(20,8),
    PriceChange24h DECIMAL(20,8),
    PriceChangePercentage24h DECIMAL(10,4),
    CirculatingSupply DECIMAL(30,4),
    Ath DECIMAL(20,8),
    AthDate DATETIME,
    Atl DECIMAL(20,8),
    AtlDate DATETIME,
    ApiLastUpdated DATETIME,
    FetchedAt DATETIME DEFAULT GETDATE(),
    BatchId INT FOREIGN KEY REFERENCES dbo.PipelineRunLog(BatchId)
);
