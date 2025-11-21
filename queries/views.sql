-- NOT DONE 
-- views
CREATE OR REPLACE VIEW gold.v_trips_full AS
SELECT * FROM gold.fact_trips;

CREATE OR REPLACE VIEW gold.v_trips_masked AS
SELECT
    trip_id,
    start_station_id,
    end_station_id,
    '***' AS user_type,      -- pseudonymization
    start_time
FROM gold.fact_trips;

-- give rights 
GRANT SELECT ON gold.v_trips_full TO analyst_full;
GRANT SELECT ON gold.v_trips_masked TO analyst_limited;