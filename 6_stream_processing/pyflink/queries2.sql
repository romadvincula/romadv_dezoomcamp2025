select * from processed_events_aggregated
order by event_hour, test_data;


select count(*) from processed_greentrips
--where date(lpep_dropoff_datetime) > '2019-01-01'
--order by dolocationid, lpep_pickup_datetime
limit 100;


select * from processed_greentrips_aggregated
order by trip_count desc;

--DROP TABLE processed_greentrips_aggregated;
CREATE TABLE processed_greentrips_aggregated (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            avg_trip_distance FLOAT,
            total_trip_distance FLOAT
        )