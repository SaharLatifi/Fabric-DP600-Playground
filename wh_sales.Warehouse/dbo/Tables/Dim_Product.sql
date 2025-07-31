CREATE TABLE [dbo].[Dim_Product] (

	[ProductKey] int NOT NULL, 
	[ProductAltKey] varchar(20) NULL, 
	[ProductName] varchar(50) NOT NULL, 
	[Category] varchar(50) NULL, 
	[ListPrice] decimal(5,2) NULL
);