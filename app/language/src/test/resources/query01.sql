SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM Customers
WHERE Industry = 'Oil/Gas Transmission'
UNION
SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM Customers
WHERE Industry = 'Computer Manufacturing'