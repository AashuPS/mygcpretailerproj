-- Step 1: Create the customers Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `gcp-retailerproj-23032025.silver_dataset.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at TIMESTAMP,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

-- Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `gcp-retailerproj-23032025.silver_dataset.customers` target
USING 
  (SELECT DISTINCT
    *,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    COALESCE(is_quarantined, 
      CASE 
        WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE
        ELSE FALSE
      END
    ) AS is_quarantined,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `gcp-retailerproj-23032025.bronze_dataset.customers`) source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN MATCHED AND 
            (
             target.name != source.name OR
             target.email != source.email OR
             target.updated_at != source.updated_at
            ) 
    THEN UPDATE SET 
        target.is_active = FALSE,
        target.effective_end_date = CURRENT_TIMESTAMP();

-- Step 3: Insert New or Updated Records
INSERT INTO `gcp-retailerproj-23032025.silver_dataset.customers`
(customer_id, name, email, updated_at, is_quarantined, effective_start_date, effective_end_date, is_active)
SELECT customer_id, name, email, updated_at, is_quarantined, effective_start_date, effective_end_date, is_active
FROM `gcp-retailerproj-23032025.bronze_dataset.customers`
WHERE customer_id NOT IN (SELECT customer_id FROM `gcp-retailerproj-23032025.silver_dataset.customers` WHERE is_active = TRUE);

-- Step 1: Create the orders Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `gcp-retailerproj-23032025.silver_dataset.orders`
(
    order_id INT64,
    customer_id INT64,
    order_date TIMESTAMP,
    total_amount FLOAT64,
    updated_at TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

-- Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `gcp-retailerproj-23032025.silver_dataset.orders` target
USING 
  (SELECT DISTINCT
    *,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `gcp-retailerproj-23032025.bronze_dataset.orders`) source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN MATCHED AND 
            (
             target.customer_id != source.customer_id OR
             target.order_date != source.order_date OR
             target.total_amount != source.total_amount OR
             target.updated_at != source.updated_at
            ) 
    THEN UPDATE SET 
        target.is_active = FALSE,
        target.effective_end_date = CURRENT_TIMESTAMP();

-- Step 3: Insert New or Updated Records
INSERT INTO `gcp-retailerproj-23032025.silver_dataset.orders`
(order_id, customer_id, order_date, total_amount, updated_at, effective_start_date, effective_end_date, is_active)
SELECT order_id, customer_id, order_date, total_amount, updated_at, effective_start_date, effective_end_date, is_active
FROM `gcp-retailerproj-23032025.bronze_dataset.orders`
WHERE order_id NOT IN (SELECT order_id FROM `gcp-retailerproj-23032025.silver_dataset.orders` WHERE is_active = TRUE);

-- Step 1: Create the products Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `gcp-retailerproj-23032025.silver_dataset.products`
(
  product_id INT64,
  name STRING,
  category_id INT64,
  price FLOAT64,
  updated_at TIMESTAMP,
  is_quarantined BOOL
);

-- Step 2: Delete Outdated Records
DELETE FROM `gcp-retailerproj-23032025.silver_dataset.products`
WHERE product_id NOT IN (SELECT product_id FROM `gcp-retailerproj-23032025.bronze_dataset.products`);

-- Step 3: Insert New or Updated Records
INSERT INTO `gcp-retailerproj-23032025.silver_dataset.products`
(product_id, name, category_id, price, updated_at, is_quarantined)
SELECT 
  product_id, name, category_id, price, CAST(updated_at AS TIMESTAMP),
  CASE 
    WHEN category_id IS NULL OR name IS NULL THEN TRUE
    ELSE FALSE
  END AS is_quarantined
FROM `gcp-retailerproj-23032025.bronze_dataset.products`;

-- Step 1: Create the product_suppliers Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `gcp-retailerproj-23032025.silver_dataset.product_suppliers`
(
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

-- Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `gcp-retailerproj-23032025.silver_dataset.product_suppliers` target
USING 
  (SELECT 
    *,
    CAST(last_updated AS TIMESTAMP) AS last_updated,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `gcp-retailerproj-23032025.bronze_dataset.product_suppliers`
  WHERE supplier_id IN (SELECT supplier_id FROM `gcp-retailerproj-23032025.silver_dataset.suppliers`)
    AND product_id IN (SELECT product_id FROM `gcp-retailerproj-23032025.silver_dataset.products`)
  ) source
ON target.supplier_id = source.supplier_id 
   AND target.product_id = source.product_id 
   AND target.is_active = TRUE
WHEN MATCHED AND 
            (
             target.supply_price != source.supply_price OR
             target.last_updated != source.last_updated
            ) 
    THEN UPDATE SET 
        target.is_active = FALSE,
        target.effective_end_date = CURRENT_TIMESTAMP();

-- Step 3: Insert New or Updated Records
INSERT INTO `gcp-retailerproj-23032025.silver_dataset.product_suppliers`
(supplier_id, product_id, supply_price, last_updated, effective_start_date, effective_end_date, is_active)
SELECT supplier_id, product_id, supply_price, last_updated, effective_start_date, effective_end_date, is_active
FROM `gcp-retailerproj-23032025.bronze_dataset.product_suppliers`
WHERE (supplier_id, product_id) NOT IN 
  (SELECT supplier_id, product_id FROM `gcp-retailerproj-23032025.silver_dataset.product_suppliers` WHERE is_active = TRUE);
----------------------------------------------------------------------------------------------------------------------------------------