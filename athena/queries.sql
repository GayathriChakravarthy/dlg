SELECT "observationdate", "screentemperature", "region"
FROM "dlg"."parquet_files" all
inner join (SELECT max("screentemperature") max_temp
                             FROM "dlg"."parquet_files") hottest
on all."screentemperature" = hottest."max_temp"
ORDER BY "screentemperature" desc, "significantweathercode"
limit 1;

SELECT "observationdate", "screentemperature", "region"
FROM "dlg"."parquet_files"
ORDER BY "screentemperature" desc
limit 1;

SELECT "observationdate", "screentemperature", "region"
FROM "dlg"."parquet_files"
ORDER BY "screentemperature" desc, "significantweathercode"
limit 1;
