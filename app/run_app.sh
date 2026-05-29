#!/bin/sh

# Tên image
IMAGE_NAME="esp-app"
CONTAINER_NAME="esp-container"
PORT=8081

echo "Building Docker image: $IMAGE_NAME..."
# Build docker image tu Dockerfile tai thu muc hien tai
docker build -t $IMAGE_NAME .

echo "Stopping and removing existing container if any..."
# Kiem tra neu container dang chay thi stop va xoa
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

echo "Running new container: $CONTAINER_NAME on port $PORT..."
# Chay container moi map port 8081 host vao 8081 container
# Them bien moi truong de ket noi den DB tren host (host.docker.internal)
docker run -d \
  -p $PORT:8081 \
  -e SPRING_DATASOURCE_URL="jdbc:postgresql://host.docker.internal:5432/postgres?currentSchema=esp" \
  --name $CONTAINER_NAME \
  $IMAGE_NAME

echo "Application is starting..."
echo "You can access it at http://localhost:$PORT once it's fully up."
echo "Use 'docker logs -f $CONTAINER_NAME' to follow the logs."
