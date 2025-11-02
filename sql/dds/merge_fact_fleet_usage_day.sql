WITH lead_src AS (
  SELECT id, plate, rental_start, rental_end
  FROM ods.ods_leads_hist
  WHERE is_current = TRUE
    AND rental_start IS NOT NULL
    AND rental_end   IS NOT NULL
    AND rental_end > rental_start
),
span AS (
  -- разложим интервалы аренды на сутки
  SELECT
    id AS lead_id,
    plate,
    (gs)::date AS usage_date
  FROM lead_src
  CROSS JOIN LATERAL generate_series(
      date_trunc('day', rental_start),
      date_trunc('day', rental_end - interval '1 second'),
      interval '1 day'
  ) AS gs
),
lk_car AS (
  SELECT id AS car_id, plate FROM dds.dim_car
),
agg AS (
  SELECT
    s.usage_date,
    c.car_id,
    s.plate,
    COUNT(*) AS rentals_cnt
  FROM span s
  JOIN lk_car c ON c.plate = s.plate
  GROUP BY s.usage_date, c.car_id, s.plate
)
INSERT INTO dds.fact_fleet_usage_day (usage_date, car_id, plate, rentals_cnt, is_rented, load_date)
SELECT usage_date, car_id, plate, rentals_cnt, TRUE, now()
FROM agg
ON CONFLICT (usage_date, car_id) DO UPDATE SET
  plate       = EXCLUDED.plate,
  rentals_cnt = EXCLUDED.rentals_cnt,
  is_rented   = EXCLUDED.is_rented,
  load_date   = now()
WHERE (dds.fact_fleet_usage_day.plate, dds.fact_fleet_usage_day.rentals_cnt, dds.fact_fleet_usage_day.is_rented)
   IS DISTINCT FROM (EXCLUDED.plate, EXCLUDED.rentals_cnt, EXCLUDED.is_rented);
