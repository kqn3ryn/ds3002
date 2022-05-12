SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[dim_customers](
	[customerKey] [nvarchar](max) NULL,
	[customerName] [nvarchar](max) NULL,
	[customerType] [nvarchar](max) NULL,
	[country] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[state] [nvarchar](max) NULL,
	[zipCode] [nvarchar](max) NULL,
	[region] [nvarchar](max) NULL,
    [orderID] [nvarchar](max) NULL,
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_products](
	[productKey] [nvarchar](max) NULL,
	[productCategory] [nvarchar](max) NULL,
	[productSubCategory] [nvarchar](max) NULL,
	[productName] [nvarchar](max) NULL,
	[price] [nvarchar](max) NULL,
	[quantity] [nvarchar](max) NULL,
	[discount] [nvarchar](max) NULL,
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_orders](
	[orderKey] [nvarchar](max) NULL,
	[orderDate] [nvarchar](max) NULL,
	[shipDate] [nvarchar](max) NULL,
	[shipMode] [nvarchar](max) NULL,
	[profit] [nvarchar](max) NULL,
    [productID] [nvarchar](max) NULL,
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


-- NOTE: table was created successfully but not populated due to azure free trial restrictions
CREATE TABLE [dbo].[fact_superstore] (
	[customerID] [nvarchar](max) NULL,
	[customerName] [nvarchar](max) NULL,
	[customerType] [nvarchar](max) NULL,
	[country] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[state] [nvarchar](max) NULL,
	[zipCode] [nvarchar](max) NULL,
	[region] [nvarchar](max) NULL,
    [orderID] [nvarchar](max) NULL,
	[orderDate] [nvarchar](max) NULL,
	[shipDate] [nvarchar](max) NULL,
	[shipMode] [nvarchar](max) NULL,
    [productID] [nvarchar](max) NULL,
	[category] [nvarchar](max) NULL,
	[subCategory] [nvarchar](max) NULL,
	[productName] [nvarchar](max) NULL,
	[price] [nvarchar](max) NULL,
	[quantity] [nvarchar](max) NULL,
	[discount] [nvarchar](max) NULL,
	[profit] [nvarchar](max) NULL,
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

