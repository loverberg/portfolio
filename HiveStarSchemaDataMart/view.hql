CREATE MATERIALIZED VIEW yellow_taxi.datamart AS
SELECT
    p.name payment_type,
    to_date(tp.tpep_pickup_datetime),
    ROUND(AVG(tp.tip_amount), 2) tips_average_amount,
    COUNT(*) passengers_total
FROM yellow_taxi.payment p
JOIN yellow_taxi.trip_part tp
    ON p.id = tp.payment_type
GROUP BY p.name, to_date(tp.tpep_pickup_datetime);
