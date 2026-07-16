CREATE DATABASE IF NOT EXISTS ftgo_consumer;
CREATE DATABASE IF NOT EXISTS ftgo_order;
CREATE DATABASE IF NOT EXISTS ftgo_kitchen;
CREATE DATABASE IF NOT EXISTS ftgo_accounting;
CREATE DATABASE IF NOT EXISTS ftgo_restaurant;
CREATE DATABASE IF NOT EXISTS ftgo_delivery;

GRANT ALL PRIVILEGES ON ftgo_consumer.*   TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_order.*      TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_kitchen.*    TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_accounting.* TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_restaurant.* TO 'ftgo'@'%';
GRANT ALL PRIVILEGES ON ftgo_delivery.*   TO 'ftgo'@'%';
FLUSH PRIVILEGES;

CREATE USER 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
