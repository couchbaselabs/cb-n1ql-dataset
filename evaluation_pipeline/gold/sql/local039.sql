SELECT
    category.name
FROM
    category
INNER JOIN film_category USING (category_id)
INNER JOIN film USING (film_id)
INNER JOIN inventory USING (film_id)
INNER JOIN rental USING (inventory_id)
INNER JOIN customer USING (customer_id)
INNER JOIN address USING (address_id)
INNER JOIN city USING (city_id)
WHERE
    LOWER(city.city) LIKE 'a%' OR city.city LIKE '%-%'
GROUP BY
    category.name
ORDER BY
    SUM(CAST((julianday(rental.return_date) - julianday(rental.rental_date)) * 24 AS INTEGER)) DESC
LIMIT
    1;
//corrected query
    SELECT category.name 
FROM `pagila`.`pagila_scope`.`category` AS category 
INNER JOIN `pagila`.`pagila_scope`.`film_category` AS film_category 
    ON category.category_id = film_category.category_id 
INNER JOIN `pagila`.`pagila_scope`.`film` AS film 
    ON film_category.film_id = film.film_id 
INNER JOIN `pagila`.`pagila_scope`.`inventory` AS inventory 
    ON film.film_id = inventory.film_id 
INNER JOIN `pagila`.`pagila_scope`.`rental` AS rental 
    ON inventory.inventory_id = rental.inventory_id 
INNER JOIN `pagila`.`pagila_scope`.`customer` AS customer 
    ON rental.customer_id = customer.customer_id 
INNER JOIN `pagila`.`pagila_scope`.`address` AS address 
    ON customer.address_id = address.address_id 
INNER JOIN `pagila`.`pagila_scope`.`city` AS city 
    ON address.city_id = city.city_id 
WHERE LOWER(city.city) LIKE 'a%' OR city.city LIKE '%-%' 
GROUP BY category.name 
-- Calculates exact fractional hours and truncates, mimicking julianday logic exactly
ORDER BY SUM(TRUNC((STR_TO_MILLIS(rental.return_date) - STR_TO_MILLIS(rental.rental_date)) / 3600000)) DESC 
LIMIT 1;