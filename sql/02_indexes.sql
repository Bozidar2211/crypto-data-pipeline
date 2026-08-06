USE CryptoDataPipeline;
GO

-- Indeks za brze pretrage po CoinId + FetchedAt
CREATE INDEX IX_FactCoinMarketData_CoinId_FetchedAt 
ON dbo.FactCoinMarketData (CoinId, FetchedAt);
