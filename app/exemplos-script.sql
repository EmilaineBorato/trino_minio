SHOW CATALOGS;

SHOW SCHEMAS from minio;
SHOW SCHEMAS FROM minio LIKE 'dev';

SHOW TABLES from minio.dev;



CREATE TABLE IF NOT EXISTS minio.dev.can_div (
iata_code  varchar, 
airport    varchar, 
city       varchar, 
state      varchar, 
country    varchar, 
latitude   double, 
longitude  double
)
WITH (
    format = 'TEXTFILE',
    skip_header_line_count = 1,
    textfile_field_separator = ',',
    external_location = 's3a://test//dev/can_div'
);

DROP TABLE minio.dev.can_div
