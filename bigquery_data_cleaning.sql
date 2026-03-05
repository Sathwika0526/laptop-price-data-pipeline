-- Create cleaned Silver table from Bronze raw data
CREATE OR REPLACE TABLE `gold-cycling-482105-e7.silver_laptops12.cleaned_laptops` AS

SELECT

  -- Keep product name
  Name,

  -- Convert price columns to numeric
  CAST(`Discounted Price` AS FLOAT64) AS discounted_price,
  CAST(`Actual Price` AS FLOAT64) AS actual_price,

  -- Extract numeric percentage from text like "21% OFF"
  SAFE_CAST(REGEXP_EXTRACT(Saving, r'(\d+)') AS INT64) AS saving_percent,

  -- Standardize rating and review columns
  CAST(Rating AS FLOAT64) AS rating,
  CAST(Reviews AS INT64) AS reviews,

  -- Normalize brand name (lowercase + trim)
  TRIM(LOWER(Brand)) AS brand,

  -- Clean text fields
  TRIM(Model) AS model,
  TRIM(Core) AS core,
  TRIM(SSD) AS ssd

FROM `gold-cycling-482105-e7.bronze_laptops12.raw_laptops`

-- Exclude records without a product name
WHERE Name IS NOT NULL;



-- Create table for records that fail validation checks
CREATE OR REPLACE TABLE `gold-cycling-482105-e7.silver_laptops12.rejected_laptops` AS

SELECT
  *,
  
  -- Identify why the record was rejected
  CASE
    WHEN discounted_price IS NULL OR actual_price IS NULL THEN 'missing_price'
    WHEN discounted_price > actual_price THEN 'discount_gt_actual'
    WHEN saving_percent IS NOT NULL AND (saving_percent < 0 OR saving_percent > 100) THEN 'saving_percent_out_of_range'
    WHEN discounted_price < 0 OR actual_price < 0 THEN 'negative_price'
    ELSE 'other'
  END AS rejection_reason

FROM `gold-cycling-482105-e7.silver_laptops12.cleaned_laptops`

-- Apply validation conditions
WHERE discounted_price IS NULL
   OR actual_price IS NULL
   OR discounted_price > actual_price
   OR (saving_percent IS NOT NULL AND (saving_percent < 0 OR saving_percent > 100))
   OR discounted_price < 0
   OR actual_price < 0;


   -- Create table containing only valid and trusted records
CREATE OR REPLACE TABLE `gold-cycling-482105-e7.silver_laptops12.valid_laptops` AS

SELECT *
FROM `gold-cycling-482105-e7.silver_laptops12.cleaned_laptops`

-- Keep only records that pass validation rules
WHERE discounted_price IS NOT NULL
  AND actual_price IS NOT NULL
  AND discounted_price <= actual_price
  AND (saving_percent IS NULL OR saving_percent BETWEEN 0 AND 100)
  AND discounted_price >= 0
  AND actual_price >= 0;


-- Compare row counts across Silver tables
SELECT
  (SELECT COUNT(*) FROM `gold-cycling-482105-e7.silver_laptops12.cleaned_laptops`) AS cleaned_rows,
  (SELECT COUNT(*) FROM `gold-cycling-482105-e7.silver_laptops12.valid_laptops`) AS valid_rows,
  (SELECT COUNT(*) FROM `gold-cycling-482105-e7.silver_laptops12.rejected_laptops`) AS rejected_rows;


  -- Create an enhanced Silver table with normalized SSD as a numeric column (GB)
CREATE OR REPLACE TABLE `gold-cycling-482105-e7.silver_laptops12.valid_laptops_v2` AS

SELECT
  Name,
  discounted_price,
  actual_price,
  saving_percent,
  rating,
  reviews,
  brand,
  model,
  core,
  ssd,

  -- Extract the first number found in the SSD text (e.g., "512GB" -> 512)
  SAFE_CAST(REGEXP_EXTRACT(LOWER(ssd), r'(\d+)') AS INT64) AS ssd_gb,

  -- Flag records where SSD is missing or not parseable
  CASE
    WHEN ssd IS NULL OR TRIM(ssd) = '' THEN 0
    WHEN REGEXP_EXTRACT(LOWER(ssd), r'(\d+)') IS NULL THEN 0
    ELSE 1
  END AS has_valid_ssd

FROM `gold-cycling-482105-e7.silver_laptops12.valid_laptops`;


-- Check how many records got a parsed SSD value
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(ssd_gb IS NOT NULL) AS parsed_ssd_rows,
  COUNTIF(ssd_gb IS NULL) AS missing_or_unparsed_ssd_rows
FROM `gold-cycling-482105-e7.silver_laptops12.valid_laptops_v2`;