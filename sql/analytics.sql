SELECT SUM(revenue) AS total_revenue
FROM retail_db.sales;

SELECT
    product_name,
    SUM(quantity) AS total_quantity,
    SUM(revenue) AS total_revenue
FROM retail_db.sales
GROUP BY product_name
ORDER BY total_revenue DESC;

SELECT
    customer_name,
    city,
    SUM(revenue) AS total_spending
FROM retail_db.sales
GROUP BY customer_name, city
ORDER BY total_spending DESC;

SELECT
    category,
    SUM(revenue) AS category_revenue
FROM retail_db.sales
GROUP BY category
ORDER BY category_revenue DESC;

SELECT
    sale_date,
    COUNT(*) AS total_transactions,
    SUM(revenue) AS daily_revenue
FROM retail_db.sales
GROUP BY sale_date
ORDER BY sale_date;

SELECT
    customer_name,
    SUM(revenue) AS total_spending
FROM retail_db.sales
GROUP BY customer_name
ORDER BY total_spending DESC
LIMIT 1;

-- Query 7: Rank products based on revenue

SELECT
    product_name,
    SUM(revenue) AS total_revenue,
    RANK() OVER (
        PARTITION BY product_name
        ORDER BY SUM(revenue) DESC
    ) AS revenue_rank
FROM retail_db.sales
GROUP BY product_name;