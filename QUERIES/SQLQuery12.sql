------------------DATA ANALYSIS -----------------
SELECT * FROM INFORMATION_SCHEMA.TABLES

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='dim_customers'

select * from gold.dim_customers
select * from gold.dim_products

select distinct country 
from gold.dim_customers

select distinct category from gold.dim_products

select distinct category,subcategory,product_name from gold.dim_products

order by category, subcategory,product_name

-- Find the date of the first and last order
-- How many years of sales are available
SELECT
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS order_range_months
FROM gold.fact_sales;


-- Find the youngest and the oldest customer
SELECT
    MIN(birthdate) AS oldest_birthdate,
    DATEDIFF(year, MIN(birthdate), GETDATE()) AS oldest_age,
    MAX(birthdate) AS youngest_birthdate,
    DATEDIFF(year, MAX(birthdate), GETDATE()) AS youngest_age
FROM gold.dim_customers;




-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales;

-- Find the average selling price
SELECT AVG(price) AS avg_price FROM gold.fact_sales;

-- Find the Total number of Orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales;
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales;

-- Find the total number of products
SELECT COUNT(product_name) AS total_products FROM gold.dim_products;
SELECT COUNT(DISTINCT product_name) AS total_products FROM gold.dim_products;

-- Find the total number of customers
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers;

-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales;

-- Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers;


-- Find total customers by countries
SELECT
  country,
  COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT
  gender,
  COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;





-- total products by category-----
SELECT
  category,
  COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;


------avg cost per category----------
SELECT
  category,
  AVG(cost) AS avg_costs
FROM gold.dim_products
GROUP BY category
ORDER BY avg_costs DESC;


-----total revenue per category---------

SELECT
  p.category,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;


----------- total revenue per customer---------------

SELECT
  c.customer_key,
  c.first_name,
  c.last_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC;


------------distribution of sold items accross country--------

SELECT
  c.country,
  SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;




-- Which 5 products generate the highest revenue?
SELECT TOP 5
  p.product_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;



-- What are the 5 worst-performing products in terms of sales?----
SELECT TOP 5
  p.product_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC;


------- What are the 5 best-performing products in terms of sales? using window func-----
SELECT *
FROM (
    SELECT 
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) t
WHERE rank_products <= 5;



------- What are the 5 worst-performing products in terms of sales? using window func-----


SELECT *
FROM (
    SELECT 
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) ASC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) t
WHERE rank_products <= 5;


-- Find the top 10 customers who have generated the highest revenue---
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;


-- The 3 customers with the fewest orders
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders ASC;



--------grouped by year --------------

SELECT
    YEAR(order_date) AS order_year,
    SUM(sales_amount) AS total_Sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);


--------grouped by month --------------

SELECT
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_Sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);




-- Calculate the total sales per month
-- and the running total of sales over time
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM (
  SELECT
    DATETRUNC(month, order_date) AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(month, order_date)
) t;



----- partitioning by month-------
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (
    PARTITION BY order_date
    ORDER BY order_date
  ) AS running_total_sales
FROM (
  SELECT
    DATETRUNC(month, order_date) AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(month, order_date)
) t;


---- partition by year but makes no sense------
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (
    PARTITION BY order_date
    ORDER BY order_date
  ) AS running_total_sales
FROM (
  SELECT
    DATETRUNC(YEAR, order_date) AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(year, order_date)
) t;

---- without partition by and year-----
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (
    ORDER BY order_date
  ) AS running_total_sales
FROM (
  SELECT
    DATETRUNC(YEAR, order_date) AS order_date,
    SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(year, order_date)
) t;



-- Calculate the total sales per year
-- Also calculate the running total of sales over time
-- And the moving average of the average price over time

SELECT
  order_date,                         -- Truncated to the year level (e.g., 2010-01-01)
  total_sales,                        -- Total sales for that year
  SUM(total_sales) OVER (
    ORDER BY order_date
  ) AS running_total_sales,          -- Cumulative sales up to that year
  AVG(avg_price) OVER (
    ORDER BY order_date
  ) AS moving_average_price          -- Moving average of the average prices across years
FROM (
  SELECT
    DATETRUNC(year, order_date) AS order_date,   -- Truncate full dates to just the year
    SUM(sales_amount) AS total_sales,            -- Total sales per year
    AVG(price) AS avg_price                      -- Average product price per year
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(year, order_date)           -- Grouping at the year level
) t;






-- Step 1: Create a CTE to calculate yearly sales for each product
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY
        YEAR(f.order_date),
        p.product_name
)

-- Step 2: Analyze performance vs. average and previous year
SELECT
    order_year,
    product_name,
    current_sales,

    -- Calculate average sales across years for the same product
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,

    -- Difference from the average
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,

    -- Label sales performance compared to average
    CASE
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,

    -- Previous year sales using LAG
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,

    -- Difference from previous year
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,

    -- Label sales trend from previous year
    CASE
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change

FROM yearly_product_sales
ORDER BY product_name, order_year;







-- Create a Common Table Expression (CTE) to calculate total sales by category
WITH category_sales AS (
    SELECT 
        category,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
    GROUP BY category
)
SELECT 
    category,
    total_sales,

    -- Calculate overall total sales (same value for all rows)
    SUM(total_sales) OVER () AS overall_sales,

    -- Calculate percentage = (category sales / total sales) * 100 and format with '%'
    CONCAT(
        ROUND(
            (CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2
        ),
        '%'
    ) AS percentage_of_total

FROM category_sales
ORDER BY total_sales DESC;



---------- cost --------
with product_segments as (

SELECT 
    product_key,
    product_name,
    cost,
    
    -- Create a custom column based on product cost
    CASE 
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END AS cost_range

FROM gold.dim_products
)



SELECT 
    cost_range,
    COUNT(product_key) AS total_products

FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;






/* 
  Group customers into three segments based on their spending behavior:
    - VIP: Customers with at least 12 months of history and spending more than €5,000.
    - Regular: Customers with at least 12 months of history but spending €5,000 or less.
    - New: Customers with a lifespan less than 12 months.
  This part only prepares customer-level metrics like total spending and lifespan.
*/
WITH customer_spending AS (
  SELECT
    c.customer_key,
    SUM(f.sales_amount) AS total_spending,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
  GROUP BY c.customer_key
)



SELECT
  customer_segment,
  COUNT(customer_key) AS total_customers
FROM (
  SELECT
    customer_key,
    CASE 
      WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
      WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
      ELSE 'New'
    END AS customer_segment
  FROM customer_spending
) t
GROUP BY customer_segment
ORDER BY total_customers DESC;






/*
==============================================================================
Customer Report
==============================================================================

Purpose:
  - This report consolidates key customer metrics and behaviors.

Highlights:
1. Gathers essential fields such as names, ages, and transaction details.
2. Segments customers into categories (VIP, Regular, New) and age groups.
3. Aggregates customer-level metrics:
   - total orders
   - total sales
   - total quantity purchased
   - total products
   - lifespan (in months)
4. Calculates valuable KPIs:
   - recency (months since last order)
   - average order value
   - average monthly spend
==============================================================================
*/create view gold.report_customers as with base_query as (SELECT
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    DATEDIFF(year, c.birthdate, GETDATE()) AS age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
)
, cust_agg as (
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_products,
    MAX(order_date) AS last_order_date,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query 
group by 
    customer_key,
    customer_number,
    customer_name,
    age
)


SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and above'
    END AS age_group,
    
    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
	datediff(month,last_order_date,getdate()) as recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
    lifespan,
	case when total_sales=0 then 0
	     else total_sales/total_orders
	end as avg_order_value,

	case when lifespan=0 then total_sales
	     else total_sales/lifespan
	end as avg_monthly_spend

FROM cust_agg;



/*
====================================================================
Product Report
====================================================================
Purpose:
 - This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
   - total orders
   - total sales
   - total quantity sold
   - total customers (unique)
   - lifespan (in months)
4. Calculates valuable KPIs:
   - recency (months since last sale)
   - average order revenue (AOR)
   - average monthly revenue
====================================================================
*/
-- Make sure this is the only statement in the batch, or run it alone

CREATE VIEW gold.report_products AS
WITH base_query AS (
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),
product_aggregations AS (
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(
            AVG(CAST(sales_amount AS FLOAT)) / NULLIF(AVG(quantity), 0),
            1
        ) AS avg_selling_price
    FROM base_query
    GROUP BY product_key, product_name, category, subcategory, cost
)
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
    CASE
        WHEN total_sales > 50000 THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,

    -- Average Order Revenue (AOR)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,

    -- Average Monthly Revenue
    CASE 
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue

FROM product_aggregations;

