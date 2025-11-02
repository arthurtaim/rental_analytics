WITH hist AS (
  SELECT
    id AS lead_id,
    status_id,
    valid_from,
    LAG(status_id)  OVER (PARTITION BY id ORDER BY valid_from) AS prev_status_id,
    LAG(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS prev_from
  FROM ods.ods_leads_hist
),
changes AS (
  SELECT
    lead_id,
    valid_from                                         AS change_ts,
    prev_status_id                                     AS from_status_id,
    status_id                                          AS to_status_id,
    EXTRACT(EPOCH FROM (valid_from - COALESCE(prev_from, valid_from)))::BIGINT AS dwell_seconds
  FROM hist
  WHERE prev_status_id IS NOT NULL
    AND status_id IS DISTINCT FROM prev_status_id
)
INSERT INTO dds.fact_lead_status_change
  (lead_id, change_ts, from_status_id, to_status_id, dwell_seconds, load_date)
SELECT
  lead_id, change_ts, from_status_id, to_status_id, dwell_seconds, now()
FROM changes
ON CONFLICT (lead_id, change_ts) DO UPDATE SET
  from_status_id = EXCLUDED.from_status_id,
  to_status_id   = EXCLUDED.to_status_id,
  dwell_seconds  = EXCLUDED.dwell_seconds,
  load_date      = now()
WHERE (dds.fact_lead_status_change.from_status_id, dds.fact_lead_status_change.to_status_id, dds.fact_lead_status_change.dwell_seconds)
   IS DISTINCT FROM (EXCLUDED.from_status_id, EXCLUDED.to_status_id, EXCLUDED.dwell_seconds);

