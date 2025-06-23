SELECT 'fhv', COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.external_fhv_nytaxi` LIMIT 1000;
SELECT 'green', COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.external_green_nytaxi` LIMIT 1000;
SELECT 'yellow', COUNT(*)/1000000 FROM `dtc-de-course-462612.trips_data_all.external_yellow_nytaxi` LIMIT 1000;