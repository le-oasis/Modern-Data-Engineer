CREATE TABLE IF NOT EXISTS customers (
  id INT NOT NULL, 
  created timestamp DEFAULT CURRENT_TIMESTAMP,
  updated timestamp DEFAULT CURRENT_TIMESTAMP,
  first_name varchar(100) NOT NULL,
  last_name varchar(100) NOT NULL,
  email varchar(255) NOT NULL UNIQUE, 
  PRIMARY KEY(id)
);