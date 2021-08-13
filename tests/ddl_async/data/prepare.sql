drop database if exists `ddl_async`;
create database `ddl_async`;
use `ddl_async`;

CREATE TABLE ddl_block1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE ddl_block1 DROP COLUMN col0;
INSERT INTO ddl_block1 (val) VALUES (1);

CREATE TABLE ddl_block2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE ddl_block2 DROP COLUMN col0;
INSERT INTO ddl_block2 (val) VALUES (1);

CREATE TABLE ddl_block3 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE ddl_block3 DROP COLUMN col0;
INSERT INTO ddl_block3 (val) VALUES (1);


CREATE TABLE many_cols1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE many_cols1 DROP COLUMN col0;
INSERT INTO many_cols1 (val) VALUES (1);

CREATE TABLE many_cols2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE many_cols2 DROP COLUMN col0;
INSERT INTO many_cols2 (val) VALUES (1);

CREATE TABLE many_cols3 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE many_cols3 DROP COLUMN col0;
INSERT INTO many_cols3 (val) VALUES (1);

CREATE TABLE many_cols4 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE many_cols4 DROP COLUMN col0;
INSERT INTO many_cols4 (val) VALUES (1);

CREATE TABLE many_cols5 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    val INT DEFAULT 0,
    col0 INT NOT NULL
);
ALTER TABLE many_cols5 DROP COLUMN col0;
INSERT INTO many_cols5 (val) VALUES (1);

CREATE TABLE finish_mark(a int primary key)
