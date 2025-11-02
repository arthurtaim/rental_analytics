INSERT INTO dds.dim_client (id, gender, phone, email, nationality, birth_date, license, client_name, load_date)
SELECT id, gender, phone, email, nationality, birth_date, license, client_name, now()
FROM ods.ods_clients_hist
WHERE is_current = TRUE
ON CONFLICT (id) DO UPDATE SET
  gender      = EXCLUDED.gender,
  phone       = EXCLUDED.phone,
  email       = EXCLUDED.email,
  nationality = EXCLUDED.nationality,
  birth_date  = EXCLUDED.birth_date,
  license     = EXCLUDED.license,
  client_name = EXCLUDED.client_name,
  load_date   = now()
WHERE (dds.dim_client.gender, dds.dim_client.phone, dds.dim_client.email, dds.dim_client.nationality,
       dds.dim_client.birth_date, dds.dim_client.license, dds.dim_client.client_name)
   IS DISTINCT FROM (EXCLUDED.gender, EXCLUDED.phone, EXCLUDED.email, EXCLUDED.nationality,
                     EXCLUDED.birth_date, EXCLUDED.license, EXCLUDED.client_name);
