DROP TABLE IF EXISTS t2 CASCADE;
DROP TABLE IF EXISTS t1 CASCADE;
DROP TABLE IF EXISTS t3 CASCADE;

CREATE TABLE T1 (
    k2 int,
    k1 int,
    A int,
    B int,
    PRIMARY KEY (k1)
);

CREATE TABLE T2 (
    k3 int,
    k2 int,
    C int,
    PRIMARY KEY (k2)
);

CREATE TABLE T3 (
    k3 int,
    D int,
    PRIMARY KEY (k3)
);

