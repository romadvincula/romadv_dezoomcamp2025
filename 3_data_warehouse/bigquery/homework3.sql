-- Setup
SELECT COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.homework3_external_table` LIMIT 1000;


CREATE OR REPLACE TABLE `trips_data_all.homework3_table` AS
SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
FROM `trips_data_all.homework3_external_table`;

SELECT COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.homework3_table` LIMIT 1000;

-- 1.

SELECT COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.homework3_external_table` LIMIT 1000;

-- 2.

-- For external table: data read estimated amount 0 MB, actual 155.12 MB 
SELECT COUNT(DISTINCT(PULocationID)) FROM `trips_data_all.homework3_external_table` LIMIT 10;

-- For regular non-clustered, non-partitioned table: data read est. 155.12 MB, actual 155.12 MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `trips_data_all.homework3_table` LIMIT 10;

-- 3.

SELECT PULocationID FROM `trips_data_all.homework3_table`;
-- est. read: 155.12 MB

SELECT PULocationID, DOLocationID FROM `trips_data_all.homework3_table`;
-- est. read: 310.24 MB

-- The 2nd query 2 columns so it needs to scan and read 2 columns of data compared to only 1 in the first query.

-- 4.
SELECT COUNT(*)
FROM `trips_data_all.homework3_table`
WHERE fare_amount = 0;

-- 5.

CREATE OR REPLACE TABLE `trips_data_all.homework3_partitioned_clustered_table`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge
FROM `trips_data_all.homework3_external_table`;

-- 6.
SELECT tpep_dropoff_datetime
FROM `trips_data_all.homework3_partitioned_clustered_table`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY tpep_dropoff_datetime DESC 
LIMIT 100;

SELECT DISTINCT(VendorID)
FROM `trips_data_all.homework3_table`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- est. read: 310.24 MB

SELECT DISTINCT(VendorID)
FROM `trips_data_all.homework3_partitioned_clustered_table`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- est. read 26.84 MB

-- 7.

-- 8.

-- Bonus
SELECT COUNT(*)
FROM `trips_data_all.homework3_table`;
-- est. read: 0 MB, since bigquery stores tables by column, it needs a specific column name to be able to estimate th read


SELECT COUNT(VendorID)
FROM `trips_data_all.homework3_table`;
