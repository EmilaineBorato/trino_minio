docker stop $(docker ps | grep trino | awk '{print $1}')
docker rm $(docker ps -a| grep trino | awk '{print $1}')