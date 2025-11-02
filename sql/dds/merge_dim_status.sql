INSERT INTO dds.dim_status (status_id, pipeline_id, pipeline_name, status_name, status_sort, is_archive, load_date)
SELECT status_id, pipeline_id, pipeline_name, status_name, status_sort, is_archive, now()
FROM dict.dict_status_leads
ON CONFLICT (status_id) DO UPDATE SET
  pipeline_id   = EXCLUDED.pipeline_id,
  pipeline_name = EXCLUDED.pipeline_name,
  status_name   = EXCLUDED.status_name,
  status_sort   = EXCLUDED.status_sort,
  is_archive    = EXCLUDED.is_archive,
  load_date     = now()
WHERE (dds.dim_status.pipeline_id, dds.dim_status.pipeline_name, dds.dim_status.status_name,
       dds.dim_status.status_sort, dds.dim_status.is_archive)
   IS DISTINCT FROM (EXCLUDED.pipeline_id, EXCLUDED.pipeline_name, EXCLUDED.status_name,
                     EXCLUDED.status_sort, EXCLUDED.is_archive);
