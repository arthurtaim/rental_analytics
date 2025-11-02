BEGIN;

WITH src AS (
  SELECT id, gender, phone, email, nationality, birth_date, license, client_name
  FROM stg.stg_clients
),
diff AS (
  SELECT o.id, o.valid_from
  FROM ods.ods_clients_hist o
  JOIN src s USING (id)
  WHERE o.is_current = TRUE
    AND (
      COALESCE(o.gender,'')      <> COALESCE(s.gender,'') OR
      COALESCE(o.phone,'')       <> COALESCE(s.phone,'') OR
      COALESCE(o.email,'')       <> COALESCE(s.email,'') OR
      COALESCE(o.nationality,'') <> COALESCE(s.nationality,'') OR
      COALESCE(o.birth_date,'1900-01-01'::date) <> COALESCE(s.birth_date,'1900-01-01'::date) OR
      COALESCE(o.license,'')     <> COALESCE(s.license,'') OR
      COALESCE(o.client_name,'') <> COALESCE(s.client_name,'')
    )
)
UPDATE ods.ods_clients_hist o
SET valid_to = now(), is_current = FALSE
FROM diff d
WHERE o.id = d.id AND o.valid_from = d.valid_from AND o.is_current = TRUE;

WITH src AS (
  SELECT id, gender, phone, email, nationality, birth_date, license, client_name
  FROM stg.stg_clients
),
need_insert AS (
  SELECT s.*
  FROM src s
  LEFT JOIN ods.ods_clients_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE o.id IS NULL

  UNION ALL

  SELECT s.*
  FROM src s
  JOIN ods.ods_clients_hist o
    ON o.id = s.id AND o.is_current = TRUE
  WHERE
    COALESCE(o.gender,'')      <> COALESCE(s.gender,'') OR
    COALESCE(o.phone,'')       <> COALESCE(s.phone,'') OR
    COALESCE(o.email,'')       <> COALESCE(s.email,'') OR
    COALESCE(o.nationality,'') <> COALESCE(s.nationality,'') OR
    COALESCE(o.birth_date,'1900-01-01'::date) <> COALESCE(s.birth_date,'1900-01-01'::date) OR
    COALESCE(o.license,'')     <> COALESCE(s.license,'') OR
    COALESCE(o.client_name,'') <> COALESCE(s.client_name,'')
)
INSERT INTO ods.ods_clients_hist (
  id, gender, phone, email, nationality, birth_date, license, client_name,
  valid_from, valid_to, is_current, load_date
)
SELECT
  id, gender, phone, email, nationality, birth_date, license, client_name,
  now(), NULL, TRUE, now()
FROM need_insert;

COMMIT;
