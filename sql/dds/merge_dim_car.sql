INSERT INTO dds.dim_car (id, model, color, year, plate, vin, dailyrate, load_date)
SELECT id, model, color, year, plate, vin, dailyrate, now()
FROM ods.ods_cars_hist
WHERE is_current = TRUE
ON CONFLICT (id) DO UPDATE SET
  model     = EXCLUDED.model,
  color     = EXCLUDED.color,
  year      = EXCLUDED.year,
  plate     = EXCLUDED.plate,
  vin       = EXCLUDED.vin,
  dailyrate = EXCLUDED.dailyrate,
  load_date = now()
WHERE (dds.dim_car.model, dds.dim_car.color, dds.dim_car.year, dds.dim_car.plate, dds.dim_car.vin, dds.dim_car.dailyrate)
   IS DISTINCT FROM (EXCLUDED.model, EXCLUDED.color, EXCLUDED.year, EXCLUDED.plate, EXCLUDED.vin, EXCLUDED.dailyrate);
