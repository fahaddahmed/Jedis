#!/bin/sh

set -e 

(
  cd "$(dirname "$0")" # Ensures compile steps are run within the repository directory
  mvn -B package -Ddir=/tmp/codecrafters-build-redis-java
)

exec java -jar /tmp/codecrafters-build-redis-java/java_redis.jar "$@"
