-- Crear base de datos y usarla
CREATE DATABASE IF NOT EXISTS weather_project;
USE weather_project;

-- Crear tablas
CREATE TABLE cities (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  latitude FLOAT,
  longitude FLOAT
);

CREATE TABLE regions (
  id INT PRIMARY KEY,
  region_name VARCHAR(50)
);

CREATE TABLE city_region (
  city_id INT,
  region_id INT,
  FOREIGN KEY (city_id) REFERENCES cities(id),
  FOREIGN KEY (region_id) REFERENCES regions(id)
);

CREATE TABLE population (
  city_id INT,
  year INT,
  population INT,
  PRIMARY KEY (city_id, year),
  FOREIGN KEY (city_id) REFERENCES cities(id)
);

CREATE TABLE city_consumption (
  id INT PRIMARY KEY,
  city_id INT,
  year INT,
  water_m3 FLOAT,
  electricity_kwh FLOAT,
  FOREIGN KEY (city_id) REFERENCES cities(id)
);

-- Insertar regiones
INSERT INTO regions (id, region_name) VALUES
(1, 'United States'),
(2, 'United Kingdom'),
(3, 'Japan'),
(4, 'France'),
(5, 'Australia'),
(6, 'Germany'),
(7, 'Brazil'),
(8, 'Canada'),
(9, 'South Africa'),
(10, 'India'),
(11, 'Colombia'),
(12, 'Mexico'),
(13, 'Spain'),
(14, 'Russia'),
(15, 'China'),
(16, 'Italy'),
(17, 'Argentina'),
(18, 'Chile'),
(19, 'South Korea'),
(20, 'Egypt'),
(21, 'Peru'),
(22, 'Norway'),
(23, 'Netherlands'),
(24, 'Sweden'),
(25, 'New Zealand'),
(26, 'Thailand'),
(27, 'Indonesia'),
(28, 'Vietnam'),
(29, 'Turkey'),
(30, 'Ukraine'),
(31, 'Poland');

-- Insertar ciudades
INSERT INTO cities (id, name, latitude, longitude) VALUES
(1, 'New York', 40.7128, -74.0060),
(2, 'London', 51.5074, -0.1278),
(3, 'Tokyo', 35.6895, 139.6917),
(4, 'Paris', 48.8566, 2.3522),
(5, 'Sydney', -33.8688, 151.2093),
(6, 'Berlin', 52.5200, 13.4050),
(7, 'São Paulo', -23.5505, -46.6333),
(8, 'Toronto', 43.6510, -79.3470),
(9, 'Cape Town', -33.9249, 18.4241),
(10, 'Mumbai', 19.0760, 72.8777),
(11, 'Medellín', 6.2500, -75.5600),
(12, 'Mexico City', 19.4326, -99.1332),
(13, 'Madrid', 40.4168, -3.7038),
(14, 'Moscow', 55.7558, 37.6176),
(15, 'Beijing', 39.9042, 116.4074),
(16, 'Rome', 41.9028, 12.4964),
(17, 'Buenos Aires', -34.6037, -58.3816),
(18, 'Santiago', -33.4489, -70.6693),
(19, 'Seoul', 37.5665, 126.9780),
(20, 'Cairo', 30.0444, 31.2357),
(21, 'Lima', -12.0464, -77.0428),
(22, 'Oslo', 59.9139, 10.7522),
(23, 'Amsterdam', 52.3676, 4.9041),
(24, 'Stockholm', 59.3293, 18.0686),
(25, 'Auckland', -36.8485, 174.7633),
(26, 'Bangkok', 13.7563, 100.5018),
(27, 'Jakarta', -6.2088, 106.8456),
(28, 'Hanoi', 21.0285, 105.8542),
(29, 'Istanbul', 41.0082, 28.9784),
(30, 'Kyiv', 50.4501, 30.5234),
(31, 'Warsaw', 52.2297, 21.0122);

-- Asociar ciudades a regiones
INSERT INTO city_region (city_id, region_id) VALUES
(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),
(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20),
(21,21),(22,22),(23,23),(24,24),(25,25),(26,26),(27,27),(28,28),(29,29),
(30,30),(31,31);

-- Insertar población
INSERT INTO population (city_id, year, population) VALUES
(1,2025,8097282),(2,2025,9841000),(3,2025,37036000),(4,2025,11347000),(5,2025,5249000),
(6,2025,3580000),(7,2025,22990000),(8,2025,6431000),(9,2025,5064000),(10,2025,22089000),
(11,2025,4173000),(12,2025,9200000),(13,2025,6700000),(14,2025,11920000),(15,2025,21540000),
(16,2025,2873000),(17,2025,15160000),(18,2025,6158000),(19,2025,9765000),(20,2025,10230000),
(21,2025,8847000),(22,2025,1037000),(23,2025,872800),(24,2025,975500),(25,2025,1657000),
(26,2025,10539000),(27,2025,10750000),(28,2025,8054000),(29,2025,15460000),(30,2025,2967000),
(31,2025,1793000);

-- Insertar consumo
INSERT INTO city_consumption (id, city_id, year, water_m3, electricity_kwh) VALUES
(1,1,2025,3200,7400),(2,2,2025,2900,6700),(3,3,2025,4100,8000),(4,4,2025,2700,6200),(5,5,2025,2500,5900),
(6,6,2025,2300,5300),(7,7,2025,3900,7500),(8,8,2025,3000,6800),(9,9,2025,2200,5600),(10,10,2025,4400,8200),
(11,11,2025,2600,6000),(12,12,2025,3100,7100),(13,13,2025,2750,6300),(14,14,2025,3600,7700),(15,15,2025,4150,8800),
(16,16,2025,2400,5200),(17,17,2025,3850,7400),(18,18,2025,2950,6500),(19,19,2025,3500,7800),(20,20,2025,2750,6100),
(21,21,2025,2600,5900),(22,22,2025,2250,5000),(23,23,2025,2350,5100),(24,24,2025,2300,4950),(25,25,2025,2150,4700),
(26,26,2025,3700,7300),(27,27,2025,3450,6900),(28,28,2025,3250,6400),(29,29,2025,3950,7700),(30,30,2025,2450,5300),
(31,31,2025,2500,5400);
