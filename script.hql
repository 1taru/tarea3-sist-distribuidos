-- 1. Obtener estad  sticas b  sicas: n  mero total de registros en dataset2
SELECT COUNT(*) AS total_records FROM dataset2;

-- 2. Contar la frecuencia de cada tipo de branch utilizando Hive
SELECT branch_type, COUNT(*) AS frequency FROM dataset2 GROUP BY branch_type;

-- 3. Analizar la relaci  n entre los tipos de branch y el valor de `taken` (1 o 0) utilizando Hive
SELECT branch_type,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count
FROM dataset2
GROUP BY branch_type;

-- 4. Calcular la proporci  n de registros con `taken` igual a 1 para cada tipo de branch
SELECT branch_type,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM dataset2
GROUP BY branch_type;

-- 5. Crear tablas en Hive para almacenar los resultados de los an  lisis realizados
CREATE TABLE IF NOT EXISTS branch_frequency2 (
  branch_type STRING,
  frequency BIGINT
);

CREATE TABLE IF NOT EXISTS branch_taken_analysis2 (
  branch_type STRING,
  taken_1_count BIGINT,
  taken_0_count BIGINT,
  total_count BIGINT
);

CREATE TABLE IF NOT EXISTS branch_taken_proportion2 (
  branch_type STRING,
  percentage_taken_1 DOUBLE
);

-- 6. Almacenar los conteos de frecuencia y las relaciones analizadas en tablas separadas en Hive

-- Almacenar la frecuencia de cada tipo de branch
INSERT INTO TABLE branch_frequency2
SELECT branch_type, COUNT(*) AS frequency
FROM dataset2
GROUP BY branch_type;

-- Almacenar la relaci  n entre los tipos de branch y el valor de `taken`                                                                                                                                        INSERT INTO TABLE branch_taken_analysis2
SELECT branch_type,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count
FROM dataset2
GROUP BY branch_type;

-- Almacenar la proporci  n de registros con `taken` igual a 1 para cada tipo de branch
INSERT INTO TABLE branch_taken_proportion2
SELECT branch_type,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM dataset2
GROUP BY branch_type;

-- 7. Realizar los an  lisis anteriores para distintas tramas de datos de forma aleatoria

-- Crear tabla para almacenar resultados de muestras
CREATE TABLE IF NOT EXISTS sample_analysis2 (
  sample_size INT,
  branch_type STRING,
  frequency BIGINT,
  taken_1_count BIGINT,
  taken_0_count BIGINT,
  total_count BIGINT,
  percentage_taken_1 DOUBLE
);

-- Tomar muestras aleatorias y realizar los an  lisis
-- Por ejemplo, una muestra de tama  o 100
CREATE TEMPORARY TABLE sample_dataset_100 AS
SELECT * FROM dataset2 TABLESAMPLE(100 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 100
INSERT INTO TABLE sample_analysis2
SELECT 100 AS sample_size,
  branch_type,
  COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_100
GROUP BY branch_type;

-- Tomar muestras aleatorias y realizar los an  lisis                                                                                                                                                            -- Por ejemplo, una muestra de tama  o 1000
CREATE TEMPORARY TABLE sample_dataset_1000 AS
SELECT * FROM dataset2 TABLESAMPLE(1000 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 1000
INSERT INTO TABLE sample_analysis2
SELECT 1000 AS sample_size,
  branch_type,
  COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_1000
GROUP BY branch_type;

CREATE TEMPORARY TABLE sample_dataset_10000 AS
SELECT * FROM dataset2 TABLESAMPLE(10000 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 10000
INSERT INTO TABLE sample_analysis2
SELECT 10000 AS sample_size,
  branch_type,                                                                                                                                                                                                     COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_10000
GROUP BY branch_type;

CREATE TEMPORARY TABLE sample_dataset_100000 AS
SELECT * FROM dataset2 TABLESAMPLE(100000 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 100000
INSERT INTO TABLE sample_analysis2
SELECT 100000 AS sample_size,
  branch_type,
  COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_100000
GROUP BY branch_type;

CREATE TEMPORARY TABLE sample_dataset_1000000 AS
SELECT * FROM dataset2 TABLESAMPLE(1000000 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 1000000
INSERT INTO TABLE sample_analysis2
SELECT 1000000 AS sample_size,
  branch_type,
  COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_1000000
GROUP BY branch_type;

CREATE TEMPORARY TABLE sample_dataset_10000000 AS
SELECT * FROM dataset2 TABLESAMPLE(10000000 ROWS);

-- Realizar an  lisis sobre la muestra de tama  o 10000000
INSERT INTO TABLE sample_analysis2
SELECT 10000000 AS sample_size,
  branch_type,
  COUNT(*) AS frequency,
  SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) AS taken_1_count,
  SUM(CASE WHEN taken = 0 THEN 1 ELSE 0 END) AS taken_0_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN taken = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS percentage_taken_1
FROM sample_dataset_10000000
GROUP BY branch_type;

