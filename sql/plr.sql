--
-- first, define the language and functions.  Turn off echoing so that expected file
-- does not depend on contents of plr.sql.
--
\set ECHO none
\i plr.sql
\set ECHO all

CREATE TABLE plr_modules (
  modseq int4,
  modsrc text
);
INSERT INTO plr_modules VALUES (0, 'pg.test.module.load <-function(msg) {print(msg)}');

--
-- plr_modules test
--
create or replace function pg_test_module_load(text) returns text as 'pg.test.module.load(arg1)' language 'plr';
select pg_test_module_load('hello world');

--
-- user defined R function test
--
select install_rcmd('pg.test.install <-function(msg) {print(msg)}');
create or replace function pg_test_install(text) returns text as 'pg.test.install(arg1)' language 'plr';
select pg_test_install('hello world');

--
-- a variety of plr functions
--
create or replace function throw_error(text) returns text as 'pg.throwerror(arg1)' language 'plr';
select throw_error('hello');

create or replace function paste(_text,_text,text) returns text[] as 'paste(arg1,arg2, sep = arg3)' language 'plr';
select paste('{hello, happy}','{world, birthday}',' ');

create or replace function vec(_float8) returns _float8 as 'arg1' language 'plr';
select vec('{1.23, 1.32}'::float8[]);

create or replace function vec(float, float) returns _float8 as 'c(arg1,arg2)' language 'plr';
select vec(1.23, 1.32);

create or replace function echo(text) returns text as 'print(arg1)' language 'plr';
select echo('hello');

create or replace function reval(text) returns text as 'eval(parse(text = arg1))' language 'plr';
select reval('a <- sd(c(1,2,3)); b <- mean(c(1,2,3)); a + b');

create or replace function "commandArgs"() returns text[] as '' language 'plr';
select "commandArgs"();

create or replace function vec(float) returns text as 'c(arg1)' language 'plr';
select vec(1.23);

create or replace function reval(_text) returns text as 'eval(parse(text = arg1))' language 'plr';
select round(reval('{"sd(c(1.12,1.23,1.18,1.34))"}'::text[])::numeric,8);

create or replace function print(text) returns text as '' language 'plr';
select print('hello');

create or replace function cube(int) returns float as 'sq <- function(x) {return(x * x)}; return(arg1 * sq(arg1))' language 'plr';
select cube(3);

create or replace function sd(_float8) returns float as 'sd(arg1)' language 'plr';
select round(sd('{1.23,1.31,1.42,1.27}'::_float8)::numeric,8);

create or replace function sd(_float8) returns float as '' language 'plr';
select round(sd('{1.23,1.31,1.42,1.27}'::_float8)::numeric,8);

create or replace function mean(_float8) returns float as '' language 'plr';
select mean('{1.23,1.31,1.42,1.27}'::_float8);

-- generates appropriate error message
create or replace function elog() returns text as '.C("elog", 18, "err from R")' language 'plr';
select elog() as error;

-- generates appropriate error message
create or replace function sprintf(text,text,text) returns text as 'sprintf(arg1,arg2,arg3)' language 'plr';
select sprintf('%s is %f feet tall', 'Sven', '7') as error;

-- this one works
select sprintf('%s is %s feet tall', 'Sven', '7');

--
-- test aggregates
--
create table foo(f1 text, f2 float8);
insert into foo values('cat1',1.21);
insert into foo values('cat1',1.24);
insert into foo values('cat1',1.18);
insert into foo values('cat1',1.26);
insert into foo values('cat1',1.15);
insert into foo values('cat2',1.15);
insert into foo values('cat2',1.26);
insert into foo values('cat2',1.32);
insert into foo values('cat2',1.30);

create or replace function r_median(_float8) returns float as 'median(arg1)' language 'plr';
select r_median('{1.23,1.31,1.42,1.27}'::_float8);
CREATE AGGREGATE median (sfunc = array_accum, basetype = float8, stype = _float8, finalfunc = r_median);
select f1, median(f2) from foo group by f1 order by f1;

create or replace function r_gamma(_float8) returns float as 'gamma(arg1)' language 'plr';
select round(r_gamma('{1.23,1.31,1.42,1.27}'::_float8)::numeric,8);
CREATE AGGREGATE gamma (sfunc = array_accum, basetype = float8, stype = _float8, finalfunc = r_gamma);
select f1, round(gamma(f2)::numeric,8) from foo group by f1 order by f1;

--
-- test returning vectors, arrays, matricies, and dataframes
-- as scalars, arrays, and records
--
create or replace function test_vt() returns text as 'array(1:10,c(2,5))' language 'plr';
select test_vt();

create or replace function test_vi() returns int as 'array(1:10,c(2,5))' language 'plr';
select test_vi();

create or replace function test_mt() returns text as 'as.matrix(array(1:10,c(2,5)))' language 'plr';
select test_mt();

create or replace function test_mi() returns int as 'as.matrix(array(1:10,c(2,5)))' language 'plr';
select test_mi();

create or replace function test_dt() returns text as 'as.data.frame(array(1:10,c(2,5)))' language 'plr';
select test_dt();

-- generates expected error
create or replace function test_di() returns int as 'as.data.frame(array(1:10,c(2,5)))' language 'plr';
select test_di() as error;

create or replace function test_vta() returns text[] as 'array(1:10,c(2,5))' language 'plr';
select test_vta();

create or replace function test_via() returns int[] as 'array(1:10,c(2,5))' language 'plr';
select test_via();

create or replace function test_mta() returns text[] as 'as.matrix(array(1:10,c(2,5)))' language 'plr';
select test_mta();

create or replace function test_mia() returns int[] as 'as.matrix(array(1:10,c(2,5)))' language 'plr';
select test_mia();

create or replace function test_dia() returns int[] as 'as.data.frame(array(1:10,c(2,5)))' language 'plr';
select test_dia();

create or replace function test_dta() returns text[] as 'as.data.frame(array(1:10,c(2,5)))' language 'plr';
select test_dta();

create or replace function test_dta1() returns text[] as 'as.data.frame(array(letters[1:10], c(2,5)))' language 'plr';
select test_dta1();

create or replace function test_dta2() returns text[] as 'as.data.frame(data.frame(letters[1:10],1:10))' language 'plr';
select test_dta2();

-- generates expected error
create or replace function test_dia1() returns int[] as 'as.data.frame(array(letters[1:10], c(2,5)))' language 'plr';
select test_dia1() as error;

create or replace function test_dtup() returns record as 'data.frame(letters[1:10],1:10)' language 'plr';
select * from test_dtup() as t(f1 text, f2 int);

create or replace function test_mtup() returns record as 'as.matrix(array(1:15,c(5,3)))' language 'plr';
select * from test_mtup() as t(f1 int, f2 int, f3 int);

create or replace function test_vtup() returns record as 'as.vector(array(1:15,c(5,3)))' language 'plr';
select * from test_vtup() as t(f1 int);

--
-- try again with named tuple types
--
CREATE TYPE dtup AS (f1 text, f2 int);
CREATE TYPE mtup AS (f1 int, f2 int, f3 int);
CREATE TYPE vtup AS (f1 int);

create or replace function test_dtup1() returns dtup as 'data.frame(letters[1:10],1:10)' language 'plr';
select * from test_dtup1();

create or replace function test_mtup1() returns mtup as 'as.matrix(array(1:15,c(5,3)))' language 'plr';
select * from test_mtup1();

create or replace function test_vtup1() returns vtup as 'as.vector(array(1:15,c(5,3)))' language 'plr';
select * from test_vtup1();



--
-- test pg R support functions (e.g. SPI_exec)
--
create or replace function pg_quote_ident(text) returns text as 'pg.quoteident(arg1)' language 'plr';
select pg_quote_ident('Hello World');

create or replace function pg_quote_literal(text) returns text as 'pg.quoteliteral(arg1)' language 'plr';
select pg_quote_literal('Hello\'World');

create or replace function test_spi_t(text) returns text as 'pg.spi.exec(arg1)' language 'plr';
select test_spi_t('select oid, typname from pg_type where typname = ''oid'' or typname = ''text''');

create or replace function test_spi_ta(text) returns text[] as 'pg.spi.exec(arg1)' language 'plr';
select test_spi_ta('select oid, typname from pg_type where typname = ''oid'' or typname = ''text''');

create or replace function test_spi_tup(text) returns record as 'pg.spi.exec(arg1)' language 'plr';
select * from test_spi_tup('select oid, typname from pg_type where typname = ''oid'' or typname = ''text''') as t(typeid oid, typename name);

create or replace function fetch_pgoid(text) returns int as 'pg.reval(arg1)' language 'plr';
select fetch_pgoid('BYTEAOID');

create or replace function test_spi_prep(text) returns text as 'sp <<- pg.spi.prepare(arg1, c(NAMEOID, NAMEOID)); print("OK")' language 'plr';
select test_spi_prep('select oid, typname from pg_type where typname = $1 or typname = $2');

create or replace function test_spi_execp(text, text, text) returns record as 'pg.spi.execp(pg.reval(arg1), list(arg2,arg3))' language 'plr';
select * from test_spi_execp('sp','oid','text') as t(typeid oid, typename name);

create or replace function test_spi_lastoid(text) returns text as 'pg.spi.exec(arg1); pg.spi.lastoid()/pg.spi.lastoid()' language 'plr';
select test_spi_lastoid('insert into foo values(''cat3'',3.333)') as "ONE";
