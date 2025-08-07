drop table test_table1;
drop table test_table_iceberg;
create external table test_table1(
        t int,
        si int,
        s string);

insert into test_table1 select 1, 2, 'test1';

create table test_table_iceberg(
        t int,
        si int) partitioned by (s string)
    stored by iceberg
    stored as orc;

insert into test_table_iceberg select * from test_table1;

drop table test_table1;
drop table test_table_iceberg;
