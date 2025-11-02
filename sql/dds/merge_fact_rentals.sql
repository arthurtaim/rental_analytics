WITH lead_src AS (
  SELECT
    l.id            AS lead_id,
    l.status_id,
    l.created_at,
    l.updated_at,
    l.model,
    l.plate,
    l.client_name,
    l.license,
    l.rental_start,
    l.rental_end,
    l.budget,
    l.dailyrate     AS dailyrate_text
  FROM ods.ods_leads_hist l
  WHERE l.is_current = TRUE
),
lk_car AS (
  SELECT id AS car_id, plate FROM dds.dim_car
),
lk_client AS (
  SELECT id AS client_id, license FROM dds.dim_client
),
lk_status AS (
  SELECT status_id, status_name FROM dds.dim_status
),
enriched AS (
  SELECT
    s.lead_id,
    c.car_id,
    cl.client_id,
    s.status_id,
    st.status_name,
    s.created_at,
    s.updated_at,
    s.rental_start,
    s.rental_end,
    s.budget,
    s.dailyrate_text
  FROM lead_src s
  LEFT JOIN lk_car    c  ON NULLIF(s.plate,'')   IS NOT NULL AND c.plate    = s.plate
  LEFT JOIN lk_client cl ON NULLIF(s.license,'') IS NOT NULL AND cl.license = s.license
  LEFT JOIN lk_status st ON st.status_id = s.status_id
)
INSERT INTO dds.fact_rentals (
  lead_id, car_id, client_id, status_id, status_name,
  created_at, updated_at, rental_start, rental_end, budget, dailyrate_text, load_date
)
SELECT
  lead_id, car_id, client_id, status_id, status_name,
  created_at, updated_at, rental_start, rental_end, budget, dailyrate_text, now()
FROM enriched
ON CONFLICT (lead_id) DO UPDATE SET
  car_id        = EXCLUDED.car_id,
  client_id     = EXCLUDED.client_id,
  status_id     = EXCLUDED.status_id,
  status_name   = EXCLUDED.status_name,
  created_at    = EXCLUDED.created_at,
  updated_at    = EXCLUDED.updated_at,
  rental_start  = EXCLUDED.rental_start,
  rental_end    = EXCLUDED.rental_end,
  budget        = EXCLUDED.budget,
  dailyrate_text= EXCLUDED.dailyrate_text,
  load_date     = now()
WHERE (dds.fact_rentals.car_id, dds.fact_rentals.client_id, dds.fact_rentals.status_id, dds.fact_rentals.status_name,
       dds.fact_rentals.created_at, dds.fact_rentals.updated_at, dds.fact_rentals.rental_start, dds.fact_rentals.rental_end,
       dds.fact_rentals.budget, dds.fact_rentals.dailyrate_text)
   IS DISTINCT FROM (EXCLUDED.car_id, EXCLUDED.client_id, EXCLUDED.status_id, EXCLUDED.status_name,
                     EXCLUDED.created_at, EXCLUDED.updated_at, EXCLUDED.rental_start, EXCLUDED.rental_end,
                     EXCLUDED.budget, EXCLUDED.dailyrate_text);
