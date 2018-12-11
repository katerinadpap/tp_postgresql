------------------------- For set operations  -------------------------

drop table K;
drop table L;
drop table M;

CREATE TABLE K (Product int, p numeric,  ts DATE, te DATE);
INSERT INTO K VALUES ('1', 0.3, '2014-08-02', '2014-08-10');
INSERT INTO K VALUES ('2', 0.8, '2014-08-04', '2014-08-07');
INSERT INTO K VALUES ('3', 0.6, '2014-08-01', '2014-08-03');

CREATE TABLE L (Product int, p numeric,  ts DATE, te DATE);
INSERT INTO L VALUES ('1', 0.6, '2014-08-05', '2014-08-09');
INSERT INTO L VALUES ('2', 0.9, '2014-08-03', '2014-08-06');

CREATE TABLE M (Product int, p numeric,  ts DATE, te DATE);
INSERT INTO M VALUES ('1', 0.6, '2014-08-01', '2014-08-04');
INSERT INTO M VALUES ('1', 0.8, '2014-08-06', '2014-08-08');
INSERT INTO M VALUES ('2', 0.8, '2014-08-04', '2014-08-05');
INSERT INTO M VALUES ('2', 0.8, '2014-08-07', '2014-08-09');

---- intro query in the paper "Set Operations in Temporal-Probabilistic Databases"
-- select TP * from m EXCEPT select * from (select TP * from k UNION select * from l) u;

------------------------- For outer join -------------------------

drop table u;
drop table v;

CREATE TABLE u (A1 int, B1 int, ts DATE, te DATE, p numeric);
INSERT INTO u VALUES ('1', '2', '2014-08-02', '2014-08-08', '0.7');
INSERT INTO u VALUES ('3', '4', '2014-08-07', '2014-08-10', '0.8');

CREATE TABLE v (A int, B int, ts DATE, te DATE, p numeric);
INSERT INTO v VALUES ('5', '6', '2014-08-01', '2014-08-04', '0.9');
INSERT INTO v VALUES ('7', '2', '2014-08-05', '2014-08-08', '0.6');
INSERT INTO v VALUES ('8', '2', '2014-08-04', '2014-08-06', '0.7');


--- intro query in the work on outer joins and anti-join
-- select TP * from u left join v on u.b1=v.b;

--- more complex query on outer joins 
-- select TP * from m right join (select TP * from u left join v on u.b1=v.b) u1 on true;



