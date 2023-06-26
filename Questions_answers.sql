-- 1. What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring) - Ans: winter
WITH seasons AS (
SELECT season as a, count(pickup_time) as total
FROM `leafy-stock-390108.NYC.fhv_tripdata`
group by season
UNION ALL
SELECT season as a, count(pickup_time) as total
FROM `leafy-stock-390108.NYC.green_tripdata`
group by season
UNION ALL
SELECT season as a, count(pickup_time) as total
FROM `leafy-stock-390108.NYC.yellow_tripdata`
group by season
)
SELECT s.a,sum(s.total) as tot
FROM seasons s
group by s.a
order by tot desc limit 1;


--2. What period of the day has the highest pickup number - Ans: Evening
WITH pickup AS (
SELECT pickup_timeofday as period, count(pickup_timeofday) as tot
FROM `leafy-stock-390108.NYC.fhv_tripdata`
group by pickup_timeofday
UNION ALL
SELECT pickup_timeofday as period, count(pickup_timeofday) as tot
FROM `leafy-stock-390108.NYC.green_tripdata`
group by pickup_timeofday
UNION ALL
SELECT pickup_timeofday as period, count(pickup_timeofday) as tot
FROM `leafy-stock-390108.NYC.yellow_tripdata`
group by pickup_timeofday
)

SELECT  p.period, sum(p.tot) as total
from pickup p
group by p.period
order by total desc limit 1;

--3. What day of the week (Monday- Sunday) has the highest pickup number - Ans: Thursday
WITH pickup AS (
SELECT day_of_week as day, count(day_of_week) as tot
FROM `leafy-stock-390108.NYC.fhv_tripdata`
group by day_of_week
UNION ALL
SELECT day_of_week as day, count(day_of_week) as tot
FROM `leafy-stock-390108.NYC.green_tripdata`
group by day_of_week
UNION ALL
SELECT day_of_week as day, count(day_of_week) as tot
FROM `leafy-stock-390108.NYC.yellow_tripdata`
group by day_of_week
)

SELECT  p.day, sum(p.tot) as total
from pickup p
group by p.day
order by total desc limit 1;


--4.  What zone has the highest total amount of paid - Ans: 	Midtown Center
WITH amt as (
SELECT z.Zone as zo,sum(g.total_amount) as amount
FROM `leafy-stock-390108.NYC.zone`  z
JOIN `leafy-stock-390108.NYC.green_tripdata`  g
ON z.LocationID = g.dropoff_location_id
group by z.Zone
UNION ALL
SELECT z.Zone as zo ,sum(y.total_amount) as amount
FROM `leafy-stock-390108.NYC.zone`  z
JOIN `leafy-stock-390108.NYC.yellow_tripdata`  y
ON z.LocationID = y.dropoff_location_id
group by z.Zone
)

SELECT  a.zo, sum(a.amount) as total
from amt a
group by a.zo
order by total desc limit 1


