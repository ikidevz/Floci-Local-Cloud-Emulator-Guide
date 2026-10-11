SELECT customer_id, count(*) AS orders, round(sum(amount), 2) AS revenue
FROM orders_clean
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10
