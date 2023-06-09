# Beginner Exercises
## Exercise 1:
 Find the total number of flights by carrier for the year 2019 from the US Flights dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT carrier, COUNT(*) as flight_count
FROM `bigquery-public-data`.airline_ontime_data.flights
WHERE EXTRACT(YEAR FROM date) = 2019
GROUP BY carrier
ORDER BY flight_count DESC
```

</details>

## Exercise 2:
 Find the top 10 airports with the highest number of flights for the year 2020 from the US Flights dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT ORIGIN, COUNT(*) as flight_count
FROM `bigquery-public-data`.airline_ontime_data.flights
WHERE EXTRACT(YEAR FROM date) = 2020
GROUP BY ORIGIN
ORDER BY flight_count DESC
LIMIT 10
```

</details>

## Exercise 3:
 Find the average arrival delay and departure delay for each airline for the year 2020 from the US Flights dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT carrier, AVG(arr_delay) as avg_arr_delay, AVG(dep_delay) as avg_dep_delay
FROM `bigquery-public-data`.airline_ontime_data.flights
WHERE EXTRACT(YEAR FROM date) = 2020
GROUP BY carrier
ORDER BY carrier ASC
```
</details>

## Exercise 4:
 Find the total number of bike trips per day for the year 2020 from the Bay Area Bike Share dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT DATE(start_date) as trip_date, COUNT(*) as trip_count
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE EXTRACT(YEAR FROM start_date) = 2020
GROUP BY trip_date
ORDER BY trip_date ASC
```

</details>

## Exercise 5:
 Find the total revenue earned by each taxi type for the year 2019 from the Chicago Taxi Trips dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT taxi_type, SUM(fare) as total_revenue
FROM `bigquery-public-data`.chicago_taxi_trips.taxi_trips
WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2019
GROUP BY taxi_type
ORDER BY total_revenue DESC
```

</details>

## Exercise 6:
 Find the number of weather stations in each state from the NOAA GSOD dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT state, COUNT(*) as station_count
FROM `bigquery-public-data`.noaa_gsod.stations
GROUP BY state
ORDER BY state ASC
```

</details>

## Exercise 7:
 Find the top 10 busiest subway stations in New York City for the month of December 2020 from the NYC MTA dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT station, SUM(traffic) as total_traffic
FROM `bigquery-public-data`.new_york_subway_data.turnstiles
WHERE EXTRACT(MONTH FROM date) = 12 AND EXTRACT(YEAR FROM date) = 2020
GROUP BY station
ORDER BY total_traffic DESC
LIMIT 10
```

</details>

## Exercise 8:
 Find the average precipitation for each month of the year 2020 in the Seattle area from the NOAA GSOD dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT EXTRACT(MONTH FROM date) as month, AVG(prcp) as avg_precipitation
FROM `bigquery-public-data`.noaa_gsod.gsod2020
WHERE stn = '725945' AND prcp IS NOT NULL
GROUP BY month
ORDER BY month ASC
```

</details>

## Exercise 9:
 Find the top 10 most popular cuisines in New York City from the Yelp dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT categories, COUNT(*) as restaurant_count
FROM `bigquery-public-data`.new_york.yelp_businesses
WHERE categories LIKE '%Restaurants%' AND city = 'New York'
GROUP BY categories
ORDER BY restaurant_count DESC
LIMIT 10
```

</details>

## Exercise 10:
 Find the total number of births by day of the week from the natality dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT CAST(EXTRACT(DAYOFWEEK FROM date_of_birth) AS STRING) as day_of_week, COUNT(*) as birth_count
FROM `bigquery-public-data`.samples.natality
GROUP BY day_of_week
ORDER BY day_of_week ASC
```

</details>

## Exercise 11:
 Find the average price of a hotel room by hotel star rating in San Francisco from the hotels dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT star_rating, AVG(price_per_night) as avg_price
FROM `bigquery-public-data`.san_francisco_hotels.hotel_reviews
WHERE city = 'San Francisco'
GROUP BY star_rating
ORDER BY star_rating ASC
```

</details>

## Exercise 12:
 Find the number of bike trips taken by subscribers and customers for each day of the week from the Bay Area Bike Share dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT DATE(start_date) as trip_date, subscriber_type, COUNT(*) as trip_count
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
GROUP BY trip_date, subscriber_type
ORDER BY trip_date ASC, subscriber_type ASC
```

</details>

## Exercise 13:
 Find the total number of flights by destination state for the year 2019 from the US Flights dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT DEST_STATE_NM as destination_state, COUNT(*) as flight_count
FROM `bigquery-public-data`.airline_ontime_data.flights
WHERE EXTRACT(YEAR FROM date) = 2019
GROUP BY destination_state
ORDER BY flight_count DESC
```

</details>

## Exercise 14:
 Find the top 10 most popular bike share stations in San Francisco for the year 2020 from the Bay Area Bike Share dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT start_station_name, COUNT(*) as trip_count
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE EXTRACT(YEAR FROM start_date) = 2020
GROUP BY start_station_name
ORDER BY trip_count DESC
LIMIT 10
```

</details>


## Exercise 15:
 Find the number of flights that were delayed for more than 15 minutes in the United States in 2020.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT COUNT(*) AS num_delayed_flights
FROM `bigquery-public-data`.usa_names.usa_1910_2013
WHERE dep_delay > 15 AND EXTRACT(YEAR FROM fl_date) = 2020
```

</details>

## Exercise 16:
 Find the total number of accidents in New York City in 2019 using the Motor Vehicle Collisions dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT COUNT(*) AS num_accidents
FROM `bigquery-public-data`.new_york_mv_collisions.nypd_mv_collisions
WHERE EXTRACT(YEAR FROM crash_date) = 2019 AND borough = 'NEW YORK'
```

</details>

## Exercise 17:
 Find the most common baby names in the United States in 2020.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT name, COUNT(*) AS count
FROM `bigquery-public-data`.usa_names.usa_1910_2013
WHERE EXTRACT(YEAR FROM year) = 2020
GROUP BY name
ORDER BY count DESC
LIMIT 10
```

</details>

## Exercise 18:
 Find the total number of rides taken on Chicago's Divvy bike sharing system in 2020.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT COUNT(*) AS num_rides
FROM `bigquery-public-data`.chicago_divvy_bicycle_sharing_system.divvy_trips
WHERE EXTRACT(YEAR FROM start_time) = 2020
```

</details>

## Exercise 19:
 Find the average temperature in New York City during the month of January 2020 using the NOAA Global Historical Climatology Network dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT AVG(avg_temp) AS avg_temp
FROM `bigquery-public-data`.ghcn_d.ghcnd_2020
WHERE id LIKE '%USW%' AND EXTRACT(MONTH FROM date) = 1 AND EXTRACT(YEAR FROM date) = 2020
```

</details>

# Advanced Exercises


## Exercise 1:
 Find the top 10 most common co-occurrences of two hashtags in tweets from the 2016 US Presidential Election using the public dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
WITH hashtags AS (
  SELECT id, 
    LOWER(hashtag) AS hashtag
  FROM `bigquery-public-data`.samples.natality
  CROSS JOIN UNNEST(SPLIT(text, ' ')) AS hashtag
  WHERE REGEXP_CONTAINS(text, r'(?i)#(election2016|trump|clinton)')
    AND LOWER(hashtag) NOT IN ('#election2016', '#trump', '#clinton')
)

SELECT hashtag1, hashtag2, COUNT(*) AS cooccurrence_count
FROM (
  SELECT h1.hashtag AS hashtag1, h2.hashtag AS hashtag2
  FROM hashtags h1
  JOIN hashtags h2
  ON h1.id = h2.id
    AND h1.hashtag < h2.hashtag
)
GROUP BY hashtag1, hashtag2
ORDER BY cooccurrence_count DESC
LIMIT 10
```
</details>

## Exercise 2:
 Find the average number of songs added to users' playlists per day from the Million Playlist Dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT DATE(timestamp) AS date, 
  COUNT(*) AS total_songs_added, 
  COUNT(DISTINCT playlist_id) AS total_playlists_updated,
  COUNT(*) / COUNT(DISTINCT playlist_id) AS avg_songs_per_playlist
FROM `bigquery-public-data`.spotify_million_playlist_dataset.playlists
CROSS JOIN UNNEST(tracks) AS track
GROUP BY date
ORDER BY date ASC
```

</details>

## Exercise 3:
 Find the percentage of customers who made a repeat purchase within 30 days of their initial purchase from the Google Merchandise Store dataset.

Solution:
<details>
  <summary>Click to show.</summary>
  
```sql
WITH customer_activity AS (
  SELECT fullVisitorId,
    DATE(MIN(t.date)) AS first_purchase_date,
    DATE(MAX(t.date)) AS most_recent_purchase_date
  FROM `bigquery-public-data`.google_analytics_sample.ecommerce_transactions t
  WHERE t.v2ProductName IS NOT NULL
  GROUP BY fullVisitorId
), repeat_customers AS (
  SELECT COUNT(DISTINCT r.fullVisitorId) AS repeat_customer_count
  FROM customer_activity r
  JOIN customer_activity f
  ON r.fullVisitorId = f.fullVisitorId
    AND r.most_recent_purchase_date > f.first_purchase_date
    AND DATE_DIFF(r.most_recent_purchase_date, f.first_purchase_date, DAY) <= 30
)

SELECT 
  ROUND(repeat_customer_count / COUNT(DISTINCT fullVisitorId) * 100, 2) AS repeat_customer_percentage
FROM repeat_customers
CROSS JOIN (SELECT COUNT(DISTINCT fullVisitorId) FROM customer_activity) AS total_customers
```

</details>

## Exercise 4:
 Find the top 10 most common co-occurrences of two hashtags in tweets from the 2016 US Presidential Election using the public dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
WITH hashtags AS (
  SELECT id, 
    LOWER(hashtag) AS hashtag
  FROM `bigquery-public-data`.samples.natality
  CROSS JOIN UNNEST(SPLIT(text, ' ')) AS hashtag
  WHERE REGEXP_CONTAINS(text, r'(?i)#(election2016|trump|clinton)')
    AND LOWER(hashtag) NOT IN ('#election2016', '#trump', '#clinton')
)

SELECT hashtag1, hashtag2, COUNT(*) AS cooccurrence_count
FROM (
  SELECT h1.hashtag AS hashtag1, h2.hashtag AS hashtag2
  FROM hashtags h1
  JOIN hashtags h2
  ON h1.id = h2.id
    AND h1.hashtag < h2.hashtag
)
GROUP BY hashtag1, hashtag2
ORDER BY cooccurrence_count DESC
LIMIT 10
```

</details>

## Exercise 5:
 Find the average number of songs added to users' playlists per day from the Million Playlist Dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
SELECT DATE(timestamp) AS date, 
  COUNT(*) AS total_songs_added, 
  COUNT(DISTINCT playlist_id) AS total_playlists_updated,
  COUNT(*) / COUNT(DISTINCT playlist_id) AS avg_songs_per_playlist
FROM `bigquery-public-data`.spotify_million_playlist_dataset.playlists
CROSS JOIN UNNEST(tracks) AS track
GROUP BY date
ORDER BY date ASC
```

</details>

## Exercise 6:
 Calculate the average duration of phone calls and texts for each user in a call and text logs dataset, where the duration of each call and text is stored in a separate row.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
WITH all_communications AS (
  SELECT user_id, communication_type, communication_time
  FROM call_logs
  UNION ALL
  SELECT user_id, communication_type, communication_time
  FROM text_logs
),
user_total_duration AS (
  SELECT user_id, 
    SUM(IF(communication_type = 'call', communication_time, 0)) AS total_call_duration,
    SUM(IF(communication_type = 'text', communication_time, 0)) AS total_text_duration,
    COUNT(DISTINCT IF(communication_type = 'call', communication_time, NULL)) AS num_calls,
    COUNT(DISTINCT IF(communication_type = 'text', communication_time, NULL)) AS num_texts
  FROM all_communications
  GROUP BY user_id
)
SELECT user_id,
  total_call_duration / num_calls AS avg_call_duration,
  total_text_duration / num_texts AS avg_text_duration
FROM user_total_duration
```

</details>

## Exercise 7:
 Find the top 10 most frequently purchased items that are commonly purchased together from an online store dataset.

Solution:
<details>
  <summary>Click to show.</summary>

```sql
WITH order_items AS (
  SELECT order_id, item_id
  FROM order_details
),
item_pairs AS (
  SELECT o1.item_id AS item1, o2.item_id AS item2
  FROM order_items o1
  JOIN order_items o2 ON o1.order_id = o2.order_id
    AND o1.item_id < o2.item_id
),
item_pair_counts AS (
  SELECT item1, item2, COUNT(*) AS pair_count
  FROM item_pairs
  GROUP BY item1, item2
),
item_counts AS (
  SELECT item_id, COUNT(*) AS item_count
  FROM order_items
  GROUP BY item_id
)
SELECT i1.name AS item1_name,
  i2.name AS item2_name,
  item_pair_counts.pair_count,
  item_counts.item_count,
  item_pair_counts.pair_count / item_counts.item_count AS lift
FROM item_pair_counts
JOIN items i1 ON item_pair_counts.item1 = i1.id
JOIN items i2 ON item_pair_counts.item2 = i2.id
JOIN item_counts ON item_pair_counts.item1 = item_counts.item_id
WHERE item_pair_counts.pair_count > 10
ORDER BY lift DESC
LIMIT 10
```

</details>
