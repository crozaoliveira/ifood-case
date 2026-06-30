USE CATALOG workspace;
USE SCHEMA ifood_case;

SELECT * 
FROM ny_yellow_silver
WHERE PickupMonth = 2
LIMIT 100;

SELECT
    PickupYear,
    PickupMonth,
    ROUND(AVG(TotalAmount), 2) AS AvgTotalAmount
FROM ny_yellow_silver
GROUP BY PickupYear, PickupMonth
ORDER BY PickupYear, PickupMonth;

SELECT
    PickupHour,
    ROUND(AVG(PassengerCount), 2) AS AvgPassengerCount
FROM workspace.ifood_case.ny_yellow_silver
WHERE PickupYear = 2023
  AND PickupMonth = 5
GROUP BY PickupHour
ORDER BY PickupHour;