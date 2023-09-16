# Volume 3:  
This chapter is divided into three main sections and exercises:

A crash course on PostgreSQL on Docker.
Connecting the RDBMS world with Spark SQL using JDBC.
Additional exercises.
Let's get started by setting up our development environments and diving into these topics!

---
# Workshop Material

## 1. Setting up PostgreSQL on Docker
### 1.1 File Location:
Navigate to the `vol-03` directory, this is where the `docker-compose.yml` file is located:

- `start` --> `docker` --> `docker-compose.yml`.


## 2. Starting the Docker Container
To start the PostgreSQL container, run the following command from the ./vol-02/start/docker directory:

Start the Docker Container
~~~
cd  volume-two/start/docker/ && ./run.sh start
~~~

OR 

```
docker compose -f docker-compose.yml up -d
```



## 2.1 Creating Datasets with MySQL
MySQL Docker Environment

This chapter’s docker environment spins up MySQL 8.
The user dataeng has been created for working with MySQL data in Spark.
The password is customized in the docker-compose-all.yaml under MYSQL_PASSWORD (dataengineering_user).

To get into the MySQL console, use the following. When prompted use the password dataengineering_user:

```
docker exec -it mysql bash
mysql -u dataeng -p
```

When you are logged in. You should see the following:
  
  ```
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 11
Server version: 8.0.23 MySQL Community Server - GPL

Copyright (c) 2000, 2021, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
  ```

## 2.2 Create a Table and add some Data
1. Reconnect to the Docker mysql container. `docker exec -it mysql bash` and run `mysql -u dataeng -p` and re-enter your password.
2. Create the customers table
3. Add some customers

### 2.3 Create the Customers Table
~~~
CREATE TABLE IF NOT EXISTS customers (
  id VARCHAR(32),
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255)
);

~~~

### 2.4 Add Some Customers to your Table
~~~
INSERT INTO customers (id, first_name, last_name, email)
VALUES
(1, 'Scott', 'Haines', 'scott@coffeeco.com'),
(2, 'John', 'Hamm', 'john.hamm@acme.com'),
(3, 'Milo', 'Haines', 'mhaines@coffeeco.com');
~~~

### 2.5 Query the Customers Table

~~~
mysql> select * from customers;
~~~

Oytput:
~~~
+----+---------------------+---------------------+------------+-----------+-------------------------+
| id | created             | updated             | first_name | last_name | email                   |
+----+---------------------+---------------------+------------+-----------+-------------------------+
| 1  | 2021-04-07 18:00:00 | 2021-04-07 18:00:00 | Scott      | Haines    |
| 2  | 2021-04-07 18:00:00 | 2021-04-07 18:00:00 | John       | Hamm      |
| 3  | 2021-04-07 18:00:00 | 2021-04-07 18:00:00 | Milo       | Haines    |
+----+---------------------+---------------------+------------+-----------+-------------------------+
3 rows in set (0.00 sec)
~~~



### 2.6 Exit the MySQL cli
When you are done in the MySQL cli, just exit with exit command.
~~~
mysql> exit
Bye
~~~



### 3.0 Creating Datasets with Postgres

To start the PostgreSQL container, run the following command from the ./vol-02/start/docker directory:

```
docker exec -it postgres_container psql -U oasis -d oasiscorp
```

Great! You have successfully started the PostgreSQL container.


The customers table is a simple table storing customer information. This table represents a basic registered customer within CoffeeCo.
To create the customers table, run the following command:

```
CREATE TABLE IF NOT EXISTS bettercustomers (
  id SERIAL PRIMARY KEY,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE
);
```

### 3.1. Inserting Data into the Customers Table
The following command will insert data into the customers table:

```
INSERT INTO bettercustomers (first_name, last_name, email)
VALUES ('John', 'Doe', 'johndoe@example.com'),
       ('Jane', 'Smith', 'janesmith@example.com'),
       ('Bob', 'Johnson', 'bobjohnson@example.com'),
       ('Alice', 'Lee', 'alicelee@example.com'),
       ('David', 'Kim', 'davidkim@example.com'),
       ('Linda', 'Nguyen', 'lindanguyen@example.com'),
       ('Mike', 'Garcia', 'mikegarcia@example.com'),
       ('Emily', 'Chen', 'emilychen@example.com'),
       ('Ryan', 'Wong', 'ryanwong@example.com'),
       ('Karen', 'Zhao', 'karenzhao@example.com');
```
Let’s do a quick sanity check to make sure things look alright before we exit the Postgres shell and move over to the Jupyter Lab.

### 3.2. Querying the Customers Table
To query the customers table, run the following command:

```
SELECT * FROM bettercustomers;
```
Great! You have successfully created the customers table and inserted data into it.



### 4. Setting up Jupyter Notebook

Go to http://localhost:8888 to see the jupyter lab homepage.

---
* For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. 
* The URL with the token can be taken from container logs using:
 
```
docker logs $(docker ps -q --filter "ancestor=jupyter/pyspark-notebook:spark-3.2.0") 2>&1 | grep 'http://127.0.0.1' | tail -1
```
---

