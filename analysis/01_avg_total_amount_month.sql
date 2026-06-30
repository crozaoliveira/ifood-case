USE CATALOG workspace;
USE SCHEMA ifood_case;

SELECT
    PickupYear,
    PickupMonth,
    COUNT(*) AS TotalVendors,
    ROUND(AVG(TotalAmount), 2) AS AvgTotalAmount
FROM ny_tlc_silver
WHERE TaxiCategory = 'YELLOW'
GROUP BY PickupYear, PickupMonth
ORDER BY PickupYear, PickupMonth;