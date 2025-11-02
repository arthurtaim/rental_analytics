BEGIN;

WITH src AS (
  SELECT
    id, name, status_id, created_at, updated_at,
    model, plate, dailyrate, client_name, license,
    rental_start, rental_end, budget
  FROM stg.stg_leads
),
diff AS (
  SELECT o.id, o.valid_from
  FROM ods.ods_leads_hist o
  JOIN src s USING (id)
  WHERE o.is_current = TRUE
    AND (
      COALESCE(o.name,'')        <> COALESCE(s.name,'') OR
      COALESCE(o.status_id,-1)   <> COALESCE(s.status_id,-1) OR
      COALESCE(o.created_at,'epoch'::timestamp) <> COALESCE(s.created_at,'epoch'::timestamp) OR
      COALESCE(o.updated_at,'epoch'::timestamp) <> COALESCE(s.updated_at,'epoch'::timestamp) OR
      COALESCE(o.model,'')       <> COALESCE(s.model,'') OR
      COALESCE(o.plate,'')       <> COALESCE(s.plate,'') OR
      COALESCE(o.dailyrate,'')   <> COALESCE(s.dailyrate,'') OR
      COALESCE(o.client_name,'') <> COALESCE(s.client_name,'') OR
      COALESCE(o.license,'')     <> COALESCE(s.license,'') OR
      COALESCE(o.rental_start,'epoch'::timestamp) <> COALESCE(s.rental_start,'epoch'::timestamp) OR
      COALESCE(o.rental_end,'epoch'::timestamp)   <> COALESCE(s.rental_end,'epoch'::timestamp) OR
      COALESCE(o.budget,-1)      <> COALESCE(s.budget,-1)
    )
)
UPDATE ods.ods_leads_hist o
SET valid_to = now(), is_current = FALSE
FROM diff d
WHERE o.id = d.id AND o.valid_from = d.valid_from AND o.is_current = TRUE;

WITH src AS (
  SELECT
    id, name, status_id, created_at, updated_at,
    model, plate, dailyrate, client_name, license,
    rental_start, rental_end, budget
  FROM stg.stg_leads
),
need_insert AS (
  SELECT s.*
  FROM src s
  LEFT JOIN ods.ods_leads_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE o.id IS NULL

  UNION ALL

  SELECT s.*
  FROM src s
  JOIN ods.ods_leads_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE
      COALESCE(o.name,'')        <> COALESCE(s.name,'') OR
      COALESCE(o.status_id,-1)   <> COALESCE(s.status_id,-1) OR
      COALESCE(o.created_at,'epoch'::timestamp) <> COALESCE(s.created_at,'epoch'::timestamp) OR
      COALESCE(o.updated_at,'epoch'::timestamp) <> COALESCE(s.updated_at,'epoch'::timestamp) OR
      COALESCE(o.model,'')       <> COALESCE(s.model,'') OR
      COALESCE(o.plate,'')       <> COALESCE(s.plate,'') OR
      COALESCE(o.dailyrate,'')   <> COALESCE(s.dailyrate,'') OR
      COALESCE(o.client_name,'') <> COALESCE(s.client_name,'') OR
      COALESCE(o.license,'')     <> COALESCE(s.license,'') OR
      COALESCE(o.rental_start,'epoch'::timestamp) <> COALESCE(s.rental_start,'epoch'::timestamp) OR
      COALESCE(o.rental_end,'epoch'::timestamp)   <> COALESCE(s.rental_end,'epoch'::timestamp) OR
      COALESCE(o.budget,-1)      <> COALESCE(s.budget,-1)
)
INSERT INTO ods.ods_leads_hist (
  id, name, status_id, created_at, updated_at,
  model, plate, dailyrate, client_name, license,
  rental_start, rental_end, budget,
  valid_from, valid_to, is_current, load_date
)
SELECT
  id, name, status_id, created_at, updated_at,
  model, plate, dailyrate, client_name, license,
  rental_start, rental_end, budget,
  now(), NULL, TRUE, now()
FROM need_insert;

COMMIT;
