-- Cargar datos desde HDFS
data = LOAD 'hdfs:///user/hive/warehouse/dataset/data.csv' USING PigStorage(',') AS (
    branch_addr:chararray,
    branch_type:chararray,
    taken:int,
    target:chararray
);

-- 1. Obtener estad  sticas b  sicas (n  mero total de registros)
records_count = FOREACH (GROUP data ALL) GENERATE COUNT(data);

-- 2. Contar la frecuencia de cada tipo de branch
branch_counts = FOREACH (GROUP data BY branch_type) GENERATE group AS branch_type, COUNT(data) AS count;

dump data;
dump records_count;
dump branch_counts;
