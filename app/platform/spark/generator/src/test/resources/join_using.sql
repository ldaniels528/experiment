BEGIN

--------------------------------------------------
--      Table Definitions
--------------------------------------------------

    CREATE EXTERNAL TABLE Customers (
        customer_id UUID,
        firstName STRING,
        lastName STRING,
        middleInit STRING
    )
    STORED AS
        INPUTFORMAT 'parquet'
        OUTPUTFORMAT 'parquet'
    LOCATION './dataSets/customers/parquet/';

    CREATE EXTERNAL TABLE Addresses (
        address_id UUID,
        address1 STRING,
        address2 STRING,
        city STRING,
        state STRING,
        zipCode STRING
    )
    STORED AS
        INPUTFORMAT 'parquet'
        OUTPUTFORMAT 'parquet'
    LOCATION './dataSets/customer-addresses/parquet/';

    CREATE EXTERNAL TABLE CustomerAddresses (
        address_id UUID,
        customer_id UUID
    )
    STORED AS
        INPUTFORMAT 'parquet'
        OUTPUTFORMAT 'parquet'
    LOCATION './dataSets/addresses/parquet/';

--------------------------------------------------
--      Transformation
--------------------------------------------------

    SELECT C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
    FROM Customers as C
    JOIN CustomerAddresses as CA USING customerId
    JOIN Addresses as A USING addressId
    WHERE C.firstName = 'Lawrence' AND C.lastName = 'Daniels';

END;