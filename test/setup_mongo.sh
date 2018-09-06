#!/usr/bin/env bash

docker-compose -f ./test/docker-compose.yaml up -d

function ping_mongo() {
  docker exec mongoutils_mongodb /usr/bin/mongo --eval "print(\"waited for connection\")" > /dev/null
  res=$?
}

echo "Waiting for MongoDB to be ready."

# Wait for MongoDB with 1 minute timeout
max_attempts=60
cur_attempts=0
ping_mongo
while (( res != 0 && ++cur_attempts != max_attempts ))
do
  ping_mongo
  echo Attempt: $cur_attempts of $max_attempts
  sleep 1
done

if (( cur_attempts == max_attempts )); then
  echo "MongoDB Timed Out."
  exit 1
else
  echo "MongoDB Ready!"
fi
