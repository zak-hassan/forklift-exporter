

CREATE KEYSPACE product
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE KEYSPACE product
CREATE TABLE  customerOrder (
 id uuid PRIMARY KEY,
 productName varchar,
 productPrice double,
 productCategory int,
 productQuantity int  );

 insert into productfulfillment.customerOrder ( id, productName, productPrice, productCategory, productQuantity) VALUES (now(), 'Bluetooth Headphones', 22.2, 1,2);

select * from productfulfillment.customerOrder;

