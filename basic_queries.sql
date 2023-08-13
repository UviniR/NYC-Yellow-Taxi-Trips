-- Create a  Big Query table for your GCP project using the pre-processed NYC Taxi Dataset
-- Access the editable looker dashboard via: https://lookerstudio.google.com/reporting/175f3277-b2cf-4420-81bf-15a4de2bbbfe
-- Use below SQL commands one by one to add data to the dashboard

--  Passengers vs Journeys
--  To get the sums as `Total Trips` `Total Passengers` & `Total Distance (km)` and  `Passengers & Trips` table
SELECT passenger_count,trip_distance, pickup_date
FROM `perceptive-map-390607.Taxi_rides.journeys`; -- replace with the Big Query table path 

--  Payment types
--  To get the `Revenue by Payment Type` doughnut chart 
SELECT Payment_type,total_amount, pickup_date
FROM `perceptive-map-390607.Taxi_rides.journeys`;

--  Payment vs Date
--  To get `Drill Down of Total Fare as Distance Fare & Other Charges` time series chart
SELECT pickup_date, fare_amount, other_charges,total_amount
FROM `perceptive-map-390607.Taxi_rides.journeys`
ORDER BY pickup_date;

--  date
--  To get `Daily Trips` time series chart
SELECT pickup_date
FROM `perceptive-map-390607.Taxi_rides.journeys`
ORDER BY pickup_date;

-- drop-offs
-- To get the `Drop-offs beyond New York` map 
SELECT CONCAT(dropoff_latitude, ",", dropoff_longitude) AS dropoff,pickup_date
FROM `perceptive-map-390607.Taxi_rides.journeys`
WHERE dropoff_latitude NOT BETWEEN 40.4961 AND 40.9155
  AND dropoff_longitude NOT BETWEEN -74.2556 AND -73.7004
  AND dropoff_latitude < 80
  AND dropoff_latitude > -60
  AND dropoff_longitude > -100
  AND dropoff_longitude < -10;

--  Pick-up
--  To get the `Pick-ups in New York` map 
SELECT CONCAT(pickup_latitude, ",", pickup_longitude) AS location, pickup_date
from `perceptive-map-390607.Taxi_rides.journeys`;