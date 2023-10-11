#!/bin/bash

# Define variables
PWD=${PWD}  # Current working directory
DOCKER_NETWORK_NAME='mde'
DOCKER_COMPOSE_FILE='docker-compose.yaml'

# Function to apply spark.conf (currently empty)
function sparkConf() {
  echo "applying spark.conf"
}

# Function to create a Docker network
function createNetwork() {
  cmd="docker network ls | grep ${DOCKER_NETWORK_NAME}"
  eval $cmd
  retVal=$?
  if [ $retVal -ne 0 ]; then
    docker network create -d bridge ${DOCKER_NETWORK_NAME}
  else
    echo "docker network already exists ${DOCKER_NETWORK_NAME}"
  fi
}

# Function to start the environment
function start() {
  # Check if mysqldir directory exists, if not, create it
  if [ ! -d "${PWD}/lakehouse/mysql/data/mysqldir" -d ]; then
    echo "mysqldir doesn't exist. Adding docker/data/mysqldir for mysql database"
    mkdir "${PWD}/data/mysqldir"
  fi
  createNetwork
  docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans mysql
  docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans zeppelin
  echo "Zeppelin will be running on http://127.0.0.1:8080"
}

# Function to stop the environment
function stop() {
  docker-compose -f ${DOCKER_COMPOSE_FILE} down --remove-orphans
}

# Function to stop and then start the environment
function restart() {
  stop && start
}

# Function to initialize the Hive Metastore
function hiveInit() {
  docker cp "${PWD}/lakehouse/hive/install/hive-schema-2.3.0.mysql.sql" "mysql:/"
  docker cp "${PWD}/lakehouse/hive/install/hive-txn-schema-2.3.0.mysql.sql" "mysql:/"
}

# Function to connect to MySQL as root and execute SQL commands
function setup_mysql_root() {
    docker exec -it mysql bash -c "mysql -u root -p'$MYSQL_ROOT_PASSWORD' <<EOF
CREATE DATABASE metastore;
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'dataeng'@'%';
GRANT ALL PRIVILEGES ON \`default\`.* TO 'dataeng'@'%';
GRANT ALL PRIVILEGES ON \`metastore\`.* TO 'dataeng'@'%';
FLUSH PRIVILEGES;
EOF"
}

# Function to connect to MySQL as dataeng and import schema
function setup_mysql_dataeng() {
    docker exec -it mysql bash -c "mysql -u dataeng -p'$MYSQL_PASSWORD' <<EOF
use metastore;
SOURCE /hive-schema-2.3.0.mysql.sql;
EOF"
}

# Main script with command-line options
case "$1" in
  start)
    start
  ;;
  stop)
    stop
  ;;
  restart)
    restart
  ;;
  hiveInit)
    hiveInit
  ;;
  mysqlSetup)
    setup_mysql_root
    setup_mysql_dataeng
  ;;
  *)
    echo $"Usage: $0 {start | stop | restart | initHive | mysqlSetup}"
  ;;
esac