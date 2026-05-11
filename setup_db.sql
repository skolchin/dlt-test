-- Source database setup
select 'create database source_db'
where not exists (select from pg_database where datname = 'source_db')\gexec
comment on database source_db is 'Source database';

\c source_db

drop table if exists public.table_data cascade;
drop table if exists public.types cascade;

create table public.types(
    type_id serial not null primary key,
    type_name text not null
);
comment on table public.types is 'Type dictionary table';
comment on column public.types.type_id is 'Type ID';
comment on column public.types.type_name is 'Type name';

create table public.table_data(
    id serial not null primary key,
    type_id int not null references types(type_id),
    comments text not null,
    created timestamp not null default current_timestamp,
    modified timestamp null
);
comment on table public.table_data is 'Data table';
comment on column public.table_data.id is 'ID';
comment on column public.table_data.type_id is 'Type ID';
comment on column public.table_data.comments is 'Comments';
comment on column public.table_data.created is 'Creation timestamp';
comment on column public.table_data.modified is 'Modification timestamp';

insert into public.types(type_id, type_name)
values (1, 'Type 1'), (2, 'Type 2'), (3, 'Type 3');

insert into public.table_data(type_id, comments, created)
select floor(random()*3) + 1, md5(random()::text), current_timestamp - '1 day'::interval
from generate_series(1, 100);

-- Stage database setup
select 'create database stage_db'
where not exists (select from pg_database where datname = 'stage_db')\gexec
comment on database stage_db is 'Stage database';

\c stage_db

-- DLT tables

DROP TABLE if exists _dlt_loads;

CREATE TABLE _dlt_loads (
	load_id varchar(64) NOT NULL,
	schema_name varchar NULL,
	status int8 NOT NULL,
	inserted_at timestamptz NOT NULL,
	schema_version_hash varchar NULL
);

DROP TABLE if exists _dlt_pipeline_state;

CREATE TABLE _dlt_pipeline_state (
	"version" int8 NOT NULL,
	engine_version int8 NOT NULL,
	pipeline_name varchar NOT NULL,
	state varchar NOT NULL,
	created_at timestamptz NOT NULL,
	version_hash varchar NULL,
	_dlt_load_id varchar(64) NOT NULL,
	_dlt_id varchar NOT NULL,
	CONSTRAINT _dlt_pipeline_state__dlt_id_key UNIQUE (_dlt_id)
);

DROP TABLE if exists _dlt_version;

CREATE TABLE _dlt_version (
	"version" int8 NOT NULL,
	engine_version int8 NOT NULL,
	inserted_at timestamptz NOT NULL,
	schema_name varchar NOT NULL,
	version_hash varchar NOT NULL,
	"schema" varchar NOT NULL
);

-- User tables
drop view if exists public.table_data_a;
drop view if exists public.types_a;
drop table if exists public.table_data;
drop table if exists public.types;

create table public.types(
    type_id int not null,
    type_name text not null,
	_dlt_load_id varchar not null,
	_dlt_id varchar not null unique,
	_dlt_valid_from timestamptz,
	_dlt_valid_to timestamptz,    
);
comment on table public.types is 'Type dictionary table';
comment on column public.types.type_id is 'Type ID';
comment on column public.types.type_name is 'Type name';

create table public.table_data(
    id int not null,
    type_id int not null,
    comments text not null,
    created timestamp not null default current_timestamp,
    modified timestamp null,
	_dlt_load_id varchar not null,
	_dlt_id varchar not null unique
);
comment on table public.table_data is 'Data table';
comment on column public.table_data.id is 'ID';
comment on column public.table_data.type_id is 'Type ID';
comment on column public.table_data.comments is 'Comments';
comment on column public.table_data.created is 'Creation timestamp';
comment on column public.table_data.modified is 'Modification timestamp';

create view public.types_a as
    select distinct on (t.type_id) t.*
    from public.types t
    join public._dlt_loads d on t._dlt_load_id = d.load_id and d.status = 0
    order by t.type_id, d.inserted_at desc;

comment on view public.types_a is 'Type dictionary table (actual data)';

create view public.table_data_a as
    select distinct on (t.id) t.*
    from public.table_data t
    join public._dlt_loads d on t._dlt_load_id = d.load_id and d.status = 0
    order by t.id, d.inserted_at desc;

comment on view public.table_data_a is 'Data table (actual data)';
