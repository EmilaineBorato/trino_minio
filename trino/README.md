# das_trino


Documentação [link](https://trino.io/docs/current/)

Minimal example to run Trino with Minio and the Hive standalone metastore on Docker.

## Status do Serviço

```bash
"curl http://localhost:8080/v1/info/state" 
```


## Trino standalone 

```bash
"docker-compose up -d"
```

## Trino Cluster

Altere 'discovery.uri' com o ip do servidor no arquivo trino-worker/etc/config.properties e em trino-coordinator/etc/config.properties

```bash
"docker-compose -f docker-compose-cluster.yml up --scale trino-worker=3 -d"
```

## Lista de workers
```bash
"select * from system.runtime.nodes;"
```

### Acessando trino via shell

```bash
"docker exec -it container-coordinator-id trino"
```


### Comandos basicos
```bash 
"SHOW catalogs;
SHOW schemas FROM namecatalog;
SHOW tables FROM namecatalog.nameschema;
"
```

```bash
"CREATE SCHEMA IF NOT EXISTS minio.iris
WITH (location = 's3a://iris/');

CREATE TABLE IF NOT EXISTS minio.iris.iris_parquet (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        VARCHAR
)
WITH (
  external_location = 's3a://iris/',
  format = 'PARQUET'
);"
```

Query the newly created table with:

```bash
"
SHOW TABLES IN minio.iris;
SELECT * FROM minio.iris.iris_parquet LIMIT 5;"
```

# License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) for details.
