USE CATALOG workspace;
USE SCHEMA ifood_case;

SELECT
    PickupHour,
    SUM(CASE WHEN TaxiCategory = 'YELLOW' THEN 1 ELSE 0 END) AS YELLOW,
    SUM(CASE WHEN TaxiCategory = 'GREEN' THEN 1 ELSE 0 END) AS GREEN
FROM ny_tlc_silver
WHERE PickupYear = 2023
  AND PickupMonth = 2
GROUP BY PickupHour
ORDER BY PickupHour;