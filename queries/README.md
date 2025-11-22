# ClickHouse Roles Grants, and Masking Logic
The code is available in `queries/roles.sql` and `queries/views.sql`
### Create two roles in ClickHouse (e.g., analyst_full, analyst_limited)
```
docker exec -i clickhouse clickhouse-client -u default --password clickhouse --multiquery < queries/roles.sql
docker exec -it clickhouse clickhouse-client -u default --password clickhouse
SHOW USERS;
SHOW ROLES;
```
### Create analytical views on top of your gold schema tables.
```
docker exec -i clickhouse clickhouse-client -u default --password clickhouse --multiquery < queries/views.sql
```

### Pseudonymized Data
We don't really have sensitive data, so we randomly masked these columns: 
1. `fact_bike_ride.ride_id`: id is pseudonymized by keeping only the last 3 digits. E.g '123456' → '***123'
2. `dim_station.lat`: latitude is rounded to 2 decimal places to reduce location precision. lat → lat_approx = ROUND(lat, 2)
3. `dim_station.lng`: longitude is rounded to 2 decimal places to reduce location precision. lng → lng_approx = ROUND(lng, 2)
4. `dim_time.weekday`: Instead of showing the exact date or full timestamp, the time dimension exposes only a high-level category: 'weekday', 'weekend',

### How different roles can query same/similar data
#### Full access user:
```
docker exec -it clickhouse clickhouse-client -u user_full  --password full
```
```
-- 1. full info about the bike ride
SELECT * FROM default_gold.v_bike_ride_full LIMIT 5
```
<img width="auto" height="auto" alt="full" src="docs/screenshots/full_1.png" />

```
-- 2. Are there consistent differences in usage between weekdays and weekends?
SELECT * FROM default_gold.v_usage_daily_full
```
<img width="auto" height="auto" alt="full" src="docs/screenshots/full_2.png" />

```
-- 3. Do members or casual riders travel farther in terms of distance?
SELECT user_type_name, AVG(trip_minutes) FROM default_gold.v_bike_ride_full GROUP BY user_type_name
```
<img width="auto" height="auto" alt="full" src="docs/screenshots/full_3.png" />

#### Limited access user:
```
docker exec -it clickhouse clickhouse-client -u user_limited  --password limited
```
```
-- 1. full info about the bike ride (limited version)
SELECT * FROM default_gold.v_bike_ride_limited LIMIT 5
```
<img width="auto" height="auto" alt="limited" src="docs/screenshots/limited_1.png" />

```
-- 2. Are there consistent differences in usage between weekdays and weekends?
SELECT * FROM default_gold.v_usage_daily_limited
```
<img width="auto" height="auto" alt="limited" src="docs/screenshots/limited_2.png" />

```
-- 3. Do members or casual riders travel farther in terms of distance?
SELECT user_type_name, AVG(trip_minutes) FROM default_gold.v_bike_ride_limited GROUP BY user_type_name
```
<img width="auto" height="auto" alt="limited" src="docs/screenshots/limited_3.png" />
