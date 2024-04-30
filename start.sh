docker-compose -f minio/docker-compose.yml up -d && docker-compose -f trino/docker-compose.yml up -d 

#docker-compose -f trino/docker-compose.yml up --scale trino-worker=3 -d