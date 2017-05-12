SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap, `Summary Quote`
FROM "companylist.csv"
WHERE Sector = 'Basic Industries'
ORDER BY Symbol
LIMIT 5;