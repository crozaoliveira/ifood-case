USE CATALOG workspace;
USE SCHEMA ifood_case;

SELECT
    PickupHour,
    ROUND(AVG(PassengerCount), 2) AS AvgPassengerCount
FROM ny_tlc_silver
WHERE PickupYear = 2023
  AND PickupMonth = 2
GROUP BY PickupHour
ORDER BY PickupHour;