BEGIN;

-- Upsert из staging в основную таблицу
INSERT INTO dict.dict_status_leads (
  status_id, pipeline_id, pipeline_name, status_name, status_sort, is_archive, load_date
)
SELECT
  s.status_id, s.pipeline_id, s.pipeline_name, s.status_name, s.status_sort, s.is_archive, now()
FROM dict.dict_status_leads_stg s
ON CONFLICT (status_id) DO UPDATE SET
  pipeline_id   = EXCLUDED.pipeline_id,
  pipeline_name = EXCLUDED.pipeline_name,
  status_name   = EXCLUDED.status_name,
  status_sort   = EXCLUDED.status_sort,
  is_archive    = EXCLUDED.is_archive,
  load_date     = now()
WHERE (dict.dict_status_leads.pipeline_id, dict.dict_status_leads.pipeline_name,
       dict.dict_status_leads.status_name, dict.dict_status_leads.status_sort, dict.dict_status_leads.is_archive)
  IS DISTINCT FROM (EXCLUDED.pipeline_id, EXCLUDED.pipeline_name,
                    EXCLUDED.status_name, EXCLUDED.status_sort, EXCLUDED.is_archive);

-- Очистить staging
TRUNCATE TABLE dict.dict_status_leads_stg;

COMMIT;
