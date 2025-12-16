-- (1)
-- Business Scale Analysis: Count Total Customers 
SELECT 
    COUNT(*) AS total_customers 
FROM 
    customer; 

-- Business Scale Analysis: Count Total Films 
SELECT 
    COUNT(*) AS total_films 
FROM 
    film; 


-- (2)
-- Stakeholder Identification: Identify Actors with Most Film Appearances 
SELECT 
    A.actor_id, 
    A.first_name, 
    A.last_name, 
    COUNT(FA.film_id) AS film_count 
FROM 
    film_actor AS FA 
JOIN 
    actor AS A ON FA.actor_id = A.actor_id 
GROUP BY 
    A.actor_id, 
    A.first_name, 
    A.last_name 
ORDER BY 
    film_count DESC; 

-- (3)
-- Geographic Footprint: Count Customers per Country 
SELECT 
    co.Country, 
    COUNT(cust.customer_id) AS customer_count 
FROM 
    customer AS cust 
JOIN 
    address AS a ON cust.address_id = a.address_id 
JOIN 
    city AS ci ON a.city_id = ci.city_id 
JOIN 
    country AS co ON ci.country_id = co.country_id 
GROUP BY 
    co.Country 
ORDER BY 
    customer_count DESC; 

-- Geographic Footprint: store address 
SELECT 
    s.store_id, 
    a.address, 
    a.district, 
    ci.city, 
    co.country 
FROM 
    store AS s 
JOIN 
    address AS a ON s.address_id = a.address_id 
JOIN 
    city AS ci ON a.city_id = ci.city_id 
JOIN 
    country AS co ON ci.country_id = co.country_id; 

-- Calculate Total Sales per Store
 SELECT 
    S.store_id, 
    SUM(P.amount) AS total_sales 
FROM 
    payment AS P 
JOIN 
    rental AS R ON P.rental_id = R.rental_id 
JOIN 
    inventory AS I ON R.inventory_id = I.inventory_id 
JOIN 
    store AS S ON I.store_id = S.store_id 
GROUP BY 
    S.store_id 
ORDER BY 
    total_sales DESC; 


-- (4)
-- Timeline: Find Earliest and Latest Rental Dates 
SELECT 
    MIN(rental_date) AS earliest_rental, 
    MAX(rental_date) AS latest_rental 
FROM 
    rental; 

-- Timeline: Count Rentals per Month 
SELECT 
    YEAR(rental_date) AS year, 
    MONTH(rental_date) AS month, 
    COUNT(*) AS rental_count 
FROM 
    rental 
GROUP BY 
    YEAR(rental_date), 
    MONTH(rental_date) 
ORDER BY 
    year ASC, 
    month ASC; 


-- (5)
-- Value Analysis: Calculate Total Revenue 
SELECT 
    SUM(amount) AS total_revenue 
FROM 
    payment; 

-- Value Analysis: Calculate Total Revenue per Film Category 
SELECT 
    C.name AS category_name, 
    SUM(P.amount) AS total_revenue 
FROM 
    film AS F 
JOIN 
    film_category AS FC ON F.film_id = FC.film_id 
JOIN 
    category AS C ON FC.category_id = C.category_id 
JOIN 
    inventory AS I ON F.film_id = I.film_id 
JOIN 
    rental AS R ON I.inventory_id = R.inventory_id 
JOIN 
    payment AS P ON R.rental_id = P.rental_id 
GROUP BY 
    C.name 
ORDER BY 
    total_revenue DESC; 

-- Actor Film Category Count  
SELECT 
    A.first_name, 
    A.last_name, 
    C.name AS category_name, 
    COUNT(F.film_id) AS film_count 
FROM 
    actor AS A 
JOIN 
    film_actor AS FA ON A.actor_id = FA.actor_id 
JOIN 
    film AS F ON FA.film_id = F.film_id 
JOIN 
    film_category AS FC ON F.film_id = FC.film_id 
JOIN 
    category AS C ON FC.category_id = C.category_id 
GROUP BY 
    A.first_name, 
    A.last_name, 
    C.name 
ORDER BY 
    A.first_name ASC, 
    A.last_name ASC, 
    film_count DESC; 
