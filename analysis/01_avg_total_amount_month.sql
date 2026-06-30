USE CATALOG workspace;
USE SCHEMA ifood_case;

SELECT
    PickupYear,
    PickupMonth,
    ROUND(AVG(TotalAmount), 2) AS AvgTotalAmount
FROM ny_yellow_silver
GROUP BY PickupYear, PickupMonth
ORDER BY PickupYear, PickupMonth;