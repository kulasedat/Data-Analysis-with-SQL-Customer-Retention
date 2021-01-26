
-- CUSTOMER RETENTION PROJECT 

-- DATA ANALYSIS

-- Checking all tables to examine everything is as it should be

SELECT * FROM [dbo].[cust_dimen];
SELECT * FROM [dbo].[market_fact];
SELECT * FROM [dbo].[orders_dimen];
SELECT * FROM [dbo].[prod_dimen];
		UPDATE [dbo].[prod_dimen]
		SET [Prod_id] = 'Prod_16' where [Prod_id] = ' RULERS AND TRIMMERS,Prod_16'
SELECT * FROM [dbo].[prod_dimen];
SELECT * FROM [dbo].[shipping_dimen];

-- Join all the tables and create a new table called combined_table. (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

SELECT * INTO combined_table FROM
		(
		SELECT
		mf.[Ord_id], mf.[Prod_id], mf.[Ship_id], mf.[Cust_id], mf.[Sales], mf.[Discount], 
		mf.[Order_Quantity], mf.[Profit], mf.[Shipping_Cost], mf.[Product_Base_Margin],
		cd.[Customer_Name], cd.[Province], cd.[Region], cd.[Customer_Segment], 
		od.[Order_Date], od.[Order_Priority],
		pd.[Product_Category], pd.[Product_Sub_Category],
		sd.[Ship_Mode], sd.[Ship_Date]
		FROM [dbo].[market_fact] mf 
		INNER JOIN [dbo].[cust_dimen] cd ON mf.[Cust_id] = cd.[Cust_id]
		INNER JOIN [dbo].[orders_dimen] od ON od.[Ord_id] = mf.[Ord_id]
		INNER JOIN [dbo].[prod_dimen] pd ON pd.[Prod_id] = mf.[Prod_id]
		INNER JOIN [dbo].[shipping_dimen] sd ON sd.[Ship_id] = mf.[Ship_id]
		) ct

SELECT * FROM [dbo].[combined_table];

-- Find the top 3 customers who have the maximum count of orders.

SELECT TOP 3 [Cust_id], [Customer_Name], COUNT(DISTINCT[Ord_id]) AS Number_of_orders
FROM [dbo].[combined_table]
GROUP BY [Cust_id], [Customer_Name]
ORDER BY 3 DESC


-- Create a new column at combined_table as DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.

ALTER TABLE [dbo].[combined_table]
ADD DaysTakenForDelivery INT;

UPDATE [dbo].[combined_table]
SET DaysTakenForDelivery = DATEDIFF(d,  [Order_Date],  [Ship_Date]);

SELECT [Order_Date],[Ship_Date],[DaysTakenForDelivery] FROM [dbo].[combined_table];



-- Find the customer whose order took the maximum time to get delivered.

SELECT [Customer_Name], [Order_Date], [Ship_Date], [DaysTakenForDelivery]
FROM [dbo].[combined_table]
WHERE [DaysTakenForDelivery] IN 
	(
	SELECT MAX([DaysTakenForDelivery]) 
	FROM [dbo].[combined_table]
	);


-- Retrieve total sales made by each product from the data (use Window function)

SELECT DISTINCT [Prod_id], SUM([Sales]) OVER (PARTITION BY [Prod_id]) AS total_sales
FROM [dbo].[combined_table];

ALTER TABLE [dbo].[combined_table] ALTER COLUMN [Sales] FLOAT

SELECT DISTINCT [Prod_id], SUM([Sales]) OVER (PARTITION BY [Prod_id]) AS total_sales
FROM [dbo].[combined_table];

-- Retrieve total profit made from each product from the data (use windows function)

SELECT DISTINCT [Prod_id], SUM([Profit]) OVER (PARTITION BY [Prod_id]) AS total_profit
FROM [dbo].[combined_table];

ALTER TABLE [dbo].[combined_table] ALTER COLUMN [Profit] FLOAT

SELECT DISTINCT [Prod_id], SUM([Profit]) OVER (PARTITION BY [Prod_id]) AS total_profit
FROM [dbo].[combined_table]
ORDER BY total_profit DESC;



-- Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011

SELECT COUNT(DISTINCT [Cust_id])
FROM [dbo].[combined_table]
WHERE YEAR([Order_Date]) = 2011 AND MONTH([Order_Date]) = 01

ALTER TABLE [dbo].[combined_table] ALTER COLUMN [Order_Date] DATE

SELECT COUNT(DISTINCT [Cust_id])
FROM [dbo].[combined_table]
WHERE YEAR([Order_Date]) = 2011 AND MONTH([Order_Date]) = 01

SELECT DISTINCT YEAR([Order_Date]) AS 'Year',
	   MONTH([Order_Date]) AS 'Month',
	   COUNT([Cust_id]) OVER (PARTITION BY MONTH([Order_Date]) ORDER BY MONTH([Order_Date])) AS Total_Unique_Customers 
FROM [dbo].[combined_table]
WHERE YEAR([Order_Date]) = 2011 AND [Cust_id] IN 
			(
			SELECT DISTINCT [Cust_id]			
			FROM [dbo].[combined_table]
			WHERE YEAR([Order_Date]) = 2011 AND MONTH([Order_Date]) = 01
			);


-- CUSTOMER RETENTION ANALYSIS

-- Create a view where each user’s visits are logged by month, 
-- allowing for the possibility that these will have occurred over multiple years since whenever business started operations.

CREATE VIEW user_visit_by_month AS
SELECT [Cust_id], count_by_month, CONVERT (DATE, month + '-01') Month_Date
FROM 
	(SELECT  cust_id, SUBSTRING(CAST(order_date AS VARCHAR), 1,7) AS [Month], 
	COUNT(*) as count_by_month 
	FROM [dbo].[combined_table]
	GROUP BY cust_id, SUBSTRING(CAST(order_date AS VARCHAR), 1,7)
	) vbm

SELECT * FROM user_visit_by_month;



-- Identify the time lapse between each visit. 
-- So, for each person and for each month, we see when the next visit is.

CREATE VIEW time_lapse_vbm AS
SELECT *, LEAD(Month_Date) OVER (PARTITION BY [Cust_id] ORDER BY Month_Date) AS next_month
FROM user_visit_by_month

SELECT * FROM time_lapse_vbm;
   


-- Calculate the time gaps between visits.

CREATE VIEW time_gap_visits AS
SELECT *, DATEDIFF(MONTH, Month_Date, next_month) AS time_gap
FROM time_lapse_vbm

SELECT * FROM time_gap_visits;



-- Categorise the customer with time gap 1 as retained, 
-- >1 as irregular and NULL as churned.

CREATE VIEW customer_segment AS
SELECT DISTINCT[Cust_id], avrg_time_gap, 
	CASE
		WHEN avrg_time_gap <= 1 THEN 'RETAINED'
		WHEN avrg_time_gap > 1 THEN 'IRREGULAR'
		WHEN avrg_time_gap IS NULL THEN 'CHURN'
		ELSE 'NA'
	END AS customer_segment_value
FROM 
	(SELECT [Cust_id], AVG(time_gap) 
	OVER (PARTITION BY [Cust_id]) AS avrg_time_gap
	FROM time_gap_visits) atg


SELECT * FROM customer_segment;


-- Calculate the retention month wise.

CREATE VIEW retention_by_month AS
SELECT DISTINCT next_month AS retention_rate_by_month, SUM(time_gap)
OVER (PARTITION BY next_month) AS retention_sum_by_month
FROM time_gap_visits
WHERE time_gap <= 1


SELECT * FROM 
retention_by_month
ORDER BY retention_sum_by_month DESC;





