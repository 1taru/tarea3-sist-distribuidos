-- Cargar el dataset
data = LOAD 'hdfs://454c55465c48:9000/user/root/data/data.csv' USING PigStorage(',') AS (
    branch_addr: chararray,
    branch_type: chararray,
    taken: int,
    target: chararray
);

-- Tomar una muestra aleatoria de tama  o 10
sample_10 = SAMPLE data 0.00000163;  -- Aproximadamente 10/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 10
total_records_sample_10 = FOREACH (GROUP sample_10 ALL) GENERATE COUNT(sample_10) AS total;
STORE total_records_sample_10 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_10';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 10
branch_frequency_sample_10 = FOREACH (GROUP sample_10 BY branch_type) GENERATE group AS branch_type, COUNT(sample_10) AS frequency;
STORE branch_frequency_sample_10 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_10';


-- Tomar una muestra aleatoria de tama  o 100
sample_100 = SAMPLE data 0.0000163;  -- Aproximadamente 100/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 100
total_records_sample_100 = FOREACH (GROUP sample_100 ALL) GENERATE COUNT(sample_100) AS total;
STORE total_records_sample_100 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_100';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 100
branch_frequency_sample_100 = FOREACH (GROUP sample_100 BY branch_type) GENERATE group AS branch_type, COUNT(sample_100) AS frequency;
STORE branch_frequency_sample_100 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_100';


-- Tomar una muestra aleatoria de tama  o 1000
sample_1000 = SAMPLE data 0.000163;  -- Aproximadamente 1000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 1000
total_records_sample_1000 = FOREACH (GROUP sample_1000 ALL) GENERATE COUNT(sample_1000) AS total;
STORE total_records_sample_1000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_1000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 1000
branch_frequency_sample_1000 = FOREACH (GROUP sample_1000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_1000) AS frequency;
STORE branch_frequency_sample_1000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_1000';



-- Tomar una muestra aleatoria de tama  o 10000
sample_10000 = SAMPLE data 0.00163;  -- Aproximadamente 10000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 10000
total_records_sample_10000 = FOREACH (GROUP sample_10000 ALL) GENERATE COUNT(sample_10000) AS total;
STORE total_records_sample_10000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_10000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 10000
branch_frequency_sample_10000 = FOREACH (GROUP sample_10000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_10000) AS frequency;
STORE branch_frequency_sample_10000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_10000';


-- Tomar una muestra aleatoria de tama  o 100000
sample_100000 = SAMPLE data 0.0163;  -- Aproximadamente 100000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 100000
total_records_sample_100000 = FOREACH (GROUP sample_100000 ALL) GENERATE COUNT(sample_100000) AS total;
STORE total_records_sample_100000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_100000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 100000
branch_frequency_sample_100000 = FOREACH (GROUP sample_100000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_100000) AS frequency;
STORE branch_frequency_sample_100000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_100000';

-- Tomar una muestra aleatoria de tama  o 1000000
sample_1000000 = SAMPLE data 0.163;  -- Aproximadamente 1000000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 1000000
total_records_sample_1000000 = FOREACH (GROUP sample_1000000 ALL) GENERATE COUNT(sample_1000000) AS total;
STORE total_records_sample_1000000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_1000000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 1000000
branch_frequency_sample_1000000 = FOREACH (GROUP sample_1000000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_1000000) AS frequency;
STORE branch_frequency_sample_1000000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_1000000';
-- Tomar una muestra aleatoria de tama  o 10000000
sample_10000000 = SAMPLE data 0.00163;  -- Aproximadamente 10000000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 10000000
total_records_sample_10000000 = FOREACH (GROUP sample_10000000 ALL) GENERATE COUNT(sample_10000000) AS total;
STORE total_records_sample_10000000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_10000000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 10000000
branch_frequency_sample_10000000 = FOREACH (GROUP sample_10000000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_10000000) AS frequency;
STORE branch_frequency_sample_10000000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_10000000';


-- Tomar una muestra aleatoria de tama  o 100000000
sample_100000000 = SAMPLE data 0.0163;  -- Aproximadamente 100000000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 100000000
total_records_sample_100000000 = FOREACH (GROUP sample_100000000 ALL) GENERATE COUNT(sample_100000000) AS total;
STORE total_records_sample_100000000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_100000000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 100000000
branch_frequency_sample_100000000 = FOREACH (GROUP sample_100000000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_100000000) AS frequency;
STORE branch_frequency_sample_100000000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_100000000';


-- Tomar una muestra aleatoria de tama  o 1000000000
sample_1000000000 = SAMPLE data 0.163;  -- Aproximadamente 1000000000/6126148

-- Obtener el n  mero total de registros en la muestra de tama  o 1000000000
total_records_sample_1000000000 = FOREACH (GROUP sample_1000000000 ALL) GENERATE COUNT(sample_1000000000) AS total;
STORE total_records_sample_1000000000 INTO 'hdfs://454c55465c48:9000/user/root/output/total_records_sample_1000000000';

-- Contar la frecuencia de cada tipo de branch en la muestra de tama  o 1000000000
branch_frequency_sample_1000000000 = FOREACH (GROUP sample_1000000000 BY branch_type) GENERATE group AS branch_type, COUNT(sample_1000000000) AS frequency;
STORE branch_frequency_sample_1000000000 INTO 'hdfs://454c55465c48:9000/user/root/output/branch_frequency_sample_1000000000';





