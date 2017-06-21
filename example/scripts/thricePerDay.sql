INSERT INTO 'fixed-data.csv' (Symbol, Name, Sector, Industry, LastTrade) 
SELECT Symbol, Name, Sector, Industry, LastSale
FROM 'companylist.csv'
WHERE Industry = 'Oil/Gas Transmission'