USE ecommerce;

-- ============================================================
-- SEED CUSTOMERS (~100,000 rows)
-- ============================================================
DROP PROCEDURE IF EXISTS seed_customers;
DELIMITER $$
CREATE PROCEDURE seed_customers()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE fname VARCHAR(100);
    DECLARE lname VARCHAR(100);
    DECLARE city_val VARCHAR(100);
    DECLARE state_val VARCHAR(50);

    SET @fnames = 'James,Mary,John,Patricia,Robert,Jennifer,Michael,Linda,William,Barbara,David,Elizabeth,Richard,Susan,Joseph,Jessica,Thomas,Sarah,Charles,Karen,Christopher,Lisa,Daniel,Nancy,Matthew,Betty,Anthony,Margaret,Mark,Sandra,Donald,Ashley,Steven,Dorothy,Paul,Kimberly,Andrew,Emily,Kenneth,Donna,Joshua,Michelle,Kevin,Carol,Brian,Amanda,George,Melissa,Timothy,Deborah';
    SET @lnames = 'Smith,Johnson,Williams,Brown,Jones,Garcia,Miller,Davis,Rodriguez,Martinez,Hernandez,Lopez,Gonzalez,Wilson,Anderson,Thomas,Taylor,Moore,Jackson,Martin,Lee,Perez,Thompson,White,Harris,Sanchez,Clark,Ramirez,Lewis,Robinson,Walker,Young,Allen,King,Wright,Scott,Torres,Nguyen,Hill,Flores,Green,Adams,Nelson,Baker,Hall,Rivera,Campbell,Mitchell,Carter,Roberts';
    SET @cities = 'New York,Los Angeles,Chicago,Houston,Phoenix,Philadelphia,San Antonio,San Diego,Dallas,San Jose,Austin,Jacksonville,Fort Worth,Columbus,Indianapolis,Charlotte,San Francisco,Seattle,Denver,Washington,Nashville,Oklahoma City,Las Vegas,Louisville,Portland,Memphis,Baltimore,Milwaukee,Albuquerque,Tucson';
    SET @states = 'NY,CA,IL,TX,AZ,PA,TX,CA,TX,CA,TX,FL,TX,OH,IN,NC,CA,WA,CO,DC,TN,OK,NV,KY,OR,TN,MD,WI,NM,AZ';

    START TRANSACTION;
    WHILE i <= 100000 DO
        SET fname = ELT(1 + FLOOR(RAND() * 50), 'James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Barbara','David','Elizabeth','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen','Christopher','Lisa','Daniel','Nancy','Matthew','Betty','Anthony','Margaret','Mark','Sandra','Donald','Ashley','Steven','Dorothy','Paul','Kimberly','Andrew','Emily','Kenneth','Donna','Joshua','Michelle','Kevin','Carol','Brian','Amanda','George','Melissa','Timothy','Deborah');
        SET lname = ELT(1 + FLOOR(RAND() * 50), 'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson','Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores','Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts');
        SET city_val = ELT(1 + FLOOR(RAND() * 30), 'New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Indianapolis','Charlotte','San Francisco','Seattle','Denver','Washington','Nashville','Oklahoma City','Las Vegas','Louisville','Portland','Memphis','Baltimore','Milwaukee','Albuquerque','Tucson');
        SET state_val = ELT(1 + FLOOR(RAND() * 30), 'NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA','TX','FL','TX','OH','IN','NC','CA','WA','CO','DC','TN','OK','NV','KY','OR','TN','MD','WI','NM','AZ');

        INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, created_at)
        VALUES (
            fname,
            lname,
            CONCAT(LOWER(fname), '.', LOWER(lname), i, '@example.com'),
            CONCAT('555-', LPAD(FLOOR(RAND()*10000), 4, '0'), '-', LPAD(FLOOR(RAND()*10000), 4, '0')),
            CONCAT(FLOOR(RAND()*9999 + 1), ' Main St'),
            city_val,
            state_val,
            LPAD(FLOOR(RAND()*99999), 5, '0'),
            DATE_SUB(NOW(), INTERVAL FLOOR(RAND()*1000) DAY)
        );
        SET i = i + 1;
        IF i % 5000 = 0 THEN
            COMMIT;
            START TRANSACTION;
        END IF;
    END WHILE;
    COMMIT;
END$$
DELIMITER ;

-- ============================================================
-- SEED PRODUCTS (~1,000 rows)
-- ============================================================
DROP PROCEDURE IF EXISTS seed_products;
DELIMITER $$
CREATE PROCEDURE seed_products()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE categories VARCHAR(500);
    DECLARE cat VARCHAR(100);

    START TRANSACTION;
    WHILE i <= 1000 DO
        SET cat = ELT(1 + FLOOR(RAND() * 10), 'Electronics','Clothing','Books','Home & Garden','Sports','Toys','Food & Beverage','Health & Beauty','Automotive','Office Supplies');

        INSERT INTO products (name, description, price, stock, category, sku, weight_kg)
        VALUES (
            CONCAT(cat, ' Product ', i),
            CONCAT('High quality ', LOWER(cat), ' product with excellent features and durability. Item number ', i, '. Perfect for everyday use.'),
            ROUND(RAND() * 999 + 0.99, 2),
            FLOOR(RAND() * 10000),
            cat,
            CONCAT('SKU-', LPAD(i, 6, '0')),
            ROUND(RAND() * 50 + 0.1, 3)
        );
        SET i = i + 1;
    END WHILE;
    COMMIT;
END$$
DELIMITER ;

-- ============================================================
-- SEED ORDERS (~400,000 rows)
-- ============================================================
DROP PROCEDURE IF EXISTS seed_orders;
DELIMITER $$
CREATE PROCEDURE seed_orders()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE cust_id INT;
    DECLARE prod_id INT;
    DECLARE qty INT;
    DECLARE uprice DECIMAL(10,2);
    DECLARE status_val VARCHAR(20);
    DECLARE batch_size INT DEFAULT 1000;
    DECLARE total INT DEFAULT 400000;

    START TRANSACTION;
    WHILE i <= total DO
        SET cust_id = FLOOR(RAND() * 100000) + 1;
        SET prod_id = FLOOR(RAND() * 1000) + 1;
        SET qty = FLOOR(RAND() * 10) + 1;
        SET uprice = ROUND(RAND() * 999 + 0.99, 2);
        SET status_val = ELT(1 + FLOOR(RAND() * 5), 'pending','processing','shipped','delivered','cancelled');

        INSERT INTO orders (customer_id, product_id, quantity, unit_price, total_amount, status, order_date, shipping_address)
        VALUES (
            cust_id,
            prod_id,
            qty,
            uprice,
            ROUND(qty * uprice, 2),
            status_val,
            DATE_SUB(NOW(), INTERVAL FLOOR(RAND()*365) DAY),
            CONCAT(FLOOR(RAND()*9999 + 1), ' Shipping Lane, City, ST ', LPAD(FLOOR(RAND()*99999), 5, '0'))
        );
        SET i = i + 1;
        IF i % 5000 = 0 THEN
            COMMIT;
            START TRANSACTION;
        END IF;
    END WHILE;
    COMMIT;
END$$
DELIMITER ;

-- Run seeding procedures
CALL seed_customers();
CALL seed_products();
CALL seed_orders();

-- Cleanup procedures
DROP PROCEDURE IF EXISTS seed_customers;
DROP PROCEDURE IF EXISTS seed_products;
DROP PROCEDURE IF EXISTS seed_orders;
