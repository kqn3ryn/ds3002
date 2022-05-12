INSERT INTO [dbo].[dim_customers]
(	[customerKey],
	[customerName],
	[customerType],
	[country],
	[city],
	[state],
	[zipCode],
	[region],
    [orderID])
SELECT 	[CustomerID],
	[CustomerName],
	[Segment],
	[Country],
	[City],
	[State],
	[PostalCode],
	[Region],
    [OrderID]
FROM [dbo].[superstore_data]

INSERT INTO [dbo].[dim_products]
(	[productKey],
	[productCategory],
	[productSubCategory],
	[productName],
	[price],
	[quantity],
	[discount])
SELECT 	[ProductID],
	[Category],
	[SubCategory],
	[ProductName],
	[Sales],
	[Quantity],
	[Discount]
FROM [dbo].[superstore_data]

INSERT INTO [dbo].[dim_orders]
(   [orderKey],
	[orderDate],
	[shipDate],
	[shipMode],
	[profit],
    [productID])
SELECT 	[OrderID],
	[OrderDate],
	[ShipDate],
	[ShipMode],
	[Profit],
    [ProductID]
FROM [dbo].[superstore_data]

-- NOTE: table was not populated due to azure free trial restrictions
INSERT INTO [dbo].[fact_superstore]
(   [customerID],
	[customerName],
	[customerType],
	[country],
	[city],
	[state],
	[zipCode],
	[region],
    [orderID],
	[orderDate],
	[shipDate],
	[shipMode],
    [productID],
	[category],
	[subCategory],
	[productName],
	[price],
	[quantity],
	[discount],
	[profit]
)
SELECT c.customerKey,
	c.customerName,
    c.customerType,
    c.country,
    c.city,
    c.state,
    c.zipCode,
    c.region,
    o.orderKey,
    o.orderDate,
    o.shipDate,
    o.shipMode,
    p.productKey,
    p.productCategory,
    p.productSubCategory,
    p.productName,
    p.price,
    p.quantity,
    p.discount,
    o.profit
FROM [dbo].[dim_customers] AS c
INNER JOIN [dbo].[dim_orders] AS o
ON c.orderID = o.orderKey
RIGHT OUTER JOIN [dbo].[dim_products] AS p
ON o.productID = p.productKey