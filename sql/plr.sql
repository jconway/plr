--
-- first, define the language and functions.  Turn off echoing so that expected file
-- does not depend on contents of tablefunc.sql.
--
\set ECHO none
\i plr.sql
\set ECHO all

create or replace function throw_error(text) returns text as 'pg_throw_error(arg1)' language 'plr';
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
select reval('{"sd(c(1.12,1.23,1.18,1.34))"}'::text[]);

create or replace function print(text) returns text as '' language 'plr';
select print('hello');

create or replace function cube(int) returns float as 'sq <- function(x) {return(x * x)}; return(arg1 * sq(arg1))' language 'plr';
select cube(3);

create or replace function sd(_float8) returns float as 'sd(arg1)' language 'plr';
select sd('{1.23,1.31,1.42,1.27}'::_float8);

create or replace function sd(_float8) returns float as '' language 'plr';
select sd('{1.23,1.31,1.42,1.27}'::_float8);

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

--
-- test aggregates
--
create or replace function r_median(_float8) returns float as 'median(arg1)' language 'plr';
select r_median('{1.23,1.31,1.42,1.27}'::_float8);
CREATE AGGREGATE median (sfunc = array_accum, basetype = float8, stype = _float8, finalfunc = r_median);
select f1, median(f2) from foo group by f1 order by f1;

create or replace function r_gamma(_float8) returns float as 'gamma(arg1)' language 'plr';
select r_gamma('{1.23,1.31,1.42,1.27}'::_float8);
CREATE AGGREGATE gamma (sfunc = array_accum, basetype = float8, stype = _float8, finalfunc = r_gamma);
select f1, gamma(f2) from foo group by f1 order by f1;
