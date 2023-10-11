#!/bin/bash

# Define variables
PWD=${PWD}  # Current working directory

DOCKER_COMPOSE_FILE='docker-compose.yaml'
source .env
jupyter_url=$(docker logs $(docker ps -q --filter "ancestor=jupyter/pyspark-notebook:spark-3.2.0") 2>&1 | grep 'http://127.0.0.1' | tail -1 | sed -n 's/.*http:\/\/127.0.0.1:\([0-9]\{4\}\).*/\1/p')

# Function to apply spark.conf (currently empty)
function sparkConf() {
  echo "applying spark.conf"
}


# Function to start the environment
function start() {
  # Check if mysqldir directory exists, if not, create it
  if [ ! -d "${PWD}/lakehouse/mysql/data/mysqldir" ]; then
    echo "mysqldir doesn't exist. Adding docker/data/mysqldir for mysql database"
    mkdir "${PWD}/lakehouse/mysql/data/mysqldir"
  fi

  createNetwork  # Create Docker network if needed
  docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans mysql
  docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans zeppelin
  docker-compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans jupyter

  echo "Zeppelin will be running on http://127.0.0.1:8080"
  echo "Jupyter will be running on http://127.0.0.1:$jupyter_url"

  # Initialize the MySQL database and create the 'customers' table
  echo "Initializing MySQL and creating the 'customers' table..."
  setup_mysql_root
  setup_mysql_dataeng
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
  docker exec -i mysql mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<EOF
  CREATE DATABASE metastore;
  GRANT ALL PRIVILEGES ON default.* TO 'dataeng'@'%';
  GRANT ALL PRIVILEGES ON metastore.* TO 'dataeng'@'%';
  FLUSH PRIVILEGES;
EOF
}

# Function to connect to MySQL as dataeng and import schema
function setup_mysql_dataeng() {
  docker exec -i mysql mysql -u dataeng -p"$MYSQL_PASSWORD" <<EOF
  USE metastore;
  SOURCE /hive-schema-2.3.0.mysql.sql;
EOF

  # Create the 'customers' table and insert data
  docker exec -i mysql mysql -u dataeng -p"$MYSQL_PASSWORD" <<EOF
  USE default;
  CREATE TABLE IF NOT EXISTS customers (
    id mediumint NOT NULL AUTO_INCREMENT COMMENT 'customer automatic id',
    created timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'customer join date',
    updated timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'last record update',
    first_name varchar(100) NOT NULL,
    last_name varchar(100) NOT NULL,
    email varchar(255) NOT NULL UNIQUE,
    PRIMARY KEY (id)
  );
  INSERT INTO customers (created, first_name, last_name, email)
  VALUES
    ('2021-02-16 00:16:06', 'Scott', 'Haines', 'scott@coffeeco.com'),
    ('2021-02-16 00:16:06', 'John', 'Hamm', 'john.hamm@acme.com'),
    ('2021-02-16 00:16:06', 'Milo', 'Haines', 'mhaines@coffeeco.com'),
    ('2021-02-21 21:00:00', 'Penny', 'Haines', 'penny@coffeeco.com'),
    ('2021-02-21 22:00:00', 'Cloud', 'Fast', 'cloud.fast@acme.com'),
    ('2021-02-21 23:00:00', 'Marshal', 'Haines', 'paws@coffeeco.com'),
    ('2021-02-24 09:00:00', 'Willow', 'Haines', 'willow@coffeeco.com'),
    ('2021-02-24 09:00:00', 'Clover', 'Haines', 'pup@coffeeco.com');
EOF
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