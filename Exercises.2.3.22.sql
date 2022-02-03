-- -----------------------------------------------------
/* SQL Exercises 2/3/22 
Katie Nguyen
kqn3ryn
*/
-- -----------------------------------------------------

/* 1. Get Product name and  quantity/unit */
SELECT product_name, quantity_per_unit
FROM products;

/* 2. Get current product list (Product ID and name) */
SELECT id, product_name
FROM products;

/* 3. Get discontinued product list (Product ID and name) */
SELECT id, product_name
FROM products
WHERE discontinued = 1;

/* 4. Get most expensive and least expensive Product list (name and unit price) */
SELECT distinct product_name, unit_price
FROM products
JOIN order_details
ON order_details.product_id = products.id
WHERE unit_price = 
	(SELECT MAX(unit_price)
    FROM order_details)
OR unit_price = 
	(SELECT MIN(unit_price)
    FROM order_details);

/* 5. Get product list (id, name, unit price) where current products cost less than $20 */
SELECT distinct product_id, product_name, unit_price
FROM products
JOIN order_details
ON order_details.product_id = products.id
WHERE unit_price < 20;

/* 6. Get product list (id, name, unit price) where current products cost between $15 and $25 */
SELECT distinct product_id, product_name, unit_price
FROM products
JOIN order_details
ON order_details.product_id = products.id
WHERE unit_price > 15 AND unit_price < 25;

/* 7. Get product list (name, unit price) of above average price */
SELECT distinct product_name, unit_price
FROM products
JOIN order_details
ON order_details.product_id = products.id
WHERE unit_price > 
	(SELECT AVG(unit_price)
    FROM order_details);

/* 8. Get product list (name, unit price) 10 most expensive */
SELECT distinct product_name, unit_price
FROM products
JOIN order_details
ON order_details.product_id = products.id
ORDER BY unit_price DESC
LIMIT 10;

/* 9. Get count of current and discontinued products */
SELECT discontinued, COUNT(*) AS 'current' 
FROM products 
GROUP BY discontinued;

/* 10. Get product list (name, units on order, units in stock) of stock that is less than the quantity on order */
SELECT products.product_name, order_details.quantity, inventory_transactions.quantity 
FROM products 
JOIN order_details 
JOIN inventory_transactions 
ON products.id=order_details.product_id AND order_details.product_id=inventory_transactions.product_id 
WHERE inventory_transactions.quantity < order_details.quantity;
