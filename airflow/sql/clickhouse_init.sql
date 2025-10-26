CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.weather_raw
(
  date                 Date,
  temperature_max      Float32,
  temperature_min      Float32,
  rain_sum             Float32,
  precipitation_hours  UInt8,
  _ingested_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date);

CREATE TABLE IF NOT EXISTS bronze.trips_raw
(
  ride_id            String,
  started_at         DateTime64(3),
  ended_at           DateTime64(3),
  start_station_id   String,
  start_station_name String,
  start_lat          Float64,
  start_lng          Float64,
  end_station_id     String,
  end_station_name   String,
  end_lat            Float64,
  end_lng            Float64,
  member_casual      LowCardinality(String),
  bike_type          LowCardinality(String),
  _ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (toDate(started_at), start_station_id, ride_id);
