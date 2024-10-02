SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
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
ALTER TABLE T1 ADD CONSTRAINT fk_k2_T2 FOREIGN KEY (k2) REFERENCES T2(k2);
ALTER TABLE T2 ADD CONSTRAINT fk_k3_T3 FOREIGN KEY (k3) REFERENCES T3(k3);
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
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
ALTER TABLE T1 ADD CONSTRAINT fk_k2_T2 FOREIGN KEY (k2) REFERENCES T2(k2);
ALTER TABLE T2 ADD CONSTRAINT fk_k3_T3 FOREIGN KEY (k3) REFERENCES T3(k3);
