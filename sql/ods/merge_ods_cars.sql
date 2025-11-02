BEGIN;

WITH src AS (
  SELECT id, model, color, year, plate, vin, dailyrate
  FROM stg.stg_cars
),
diff AS (
  SELECT o.id, o.valid_from
  FROM ods.ods_cars_hist o
  JOIN src s USING (id)
  WHERE o.is_current = TRUE
    AND (
      COALESCE(o.model,'')     <> COALESCE(s.model,'') OR
      COALESCE(o.color,'')     <> COALESCE(s.color,'') OR
      COALESCE(o.year,-1)      <> COALESCE(s.year,-1) OR
      COALESCE(o.plate,'')     <> COALESCE(s.plate,'') OR
      COALESCE(o.vin,'')       <> COALESCE(s.vin,'') OR
      COALESCE(o.dailyrate,-1) <> COALESCE(s.dailyrate,-1)
    )
)
UPDATE ods.ods_cars_hist o
SET valid_to = now(), is_current = FALSE
FROM diff d
WHERE o.id = d.id AND o.valid_from = d.valid_from AND o.is_current = TRUE;

WITH src AS (
  SELECT id, model, color, year, plate, vin, dailyrate FROM stg.stg_cars
),
need_insert AS (
  SELECT s.*
  FROM src s
  LEFT JOIN ods.ods_cars_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE o.id IS NULL

  UNION ALL

  SELECT s.*
  FROM src s
  JOIN ods.ods_cars_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE
    COALESCE(o.model,'')     <> COALESCE(s.model,'') OR
    COALESCE(o.color,'')     <> COALESCE(s.color,'') OR
    COALESCE(o.year,-1)      <> COALESCE(s.year,-1) OR
    COALESCE(o.plate,'')     <> COALESCE(s.plate,'') OR
    COALESCE(o.vin,'')       <> COALESCE(s.vin,'') OR
    COALESCE(o.dailyrate,-1) <> COALESCE(s.dailyrate,-1)
)
INSERT INTO ods.ods_cars_hist (
  id, model, color, year, plate, vin, dailyrate,
  valid_from, valid_to, is_current, load_date
)
SELECT
  id, model, color, year, plate, vin, dailyrate,
  now(), NULL, TRUE, now()
FROM need_insert;

COMMIT;
