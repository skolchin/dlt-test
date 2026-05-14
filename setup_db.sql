-- Source database setup
select 'create database source_db'
where not exists (select from pg_database where datname = 'source_db')\gexec
comment on database source_db is 'Source database';

\c source_db

drop table if exists public.table_data;
drop table if exists public.dict1_data;
drop table if exists public.dict2_data;

create table public.dict1_data(
    dict_id serial not null primary key,
    dict_value text not null,
    created timestamp not null default current_timestamp,
    modified timestamp null
);
comment on table public.dict1_data is 'Dictionary table with timestamps';
comment on column public.dict1_data.dict_id is 'Dict ID';
comment on column public.dict1_data.dict_value is 'Value';
comment on column public.dict1_data.created is 'Creation timestamp';
comment on column public.dict1_data.modified is 'Modification timestamp';

create table public.dict2_data(
    dict_id serial not null primary key,
    dict_value text not null
);
comment on table public.dict2_data is 'Dictionary table w/o timestamps';
comment on column public.dict2_data.dict_id is 'Dict ID';
comment on column public.dict2_data.dict_value is 'Value';

create table public.table_data(
    id serial not null primary key,
    dict1_id int not null references dict1_data(dict_id),
    dict2_id int not null references dict2_data(dict_id),
    comments text not null,
    created timestamp not null default current_timestamp,
    modified timestamp null
);
comment on table public.table_data is 'Data table';
comment on column public.table_data.id is 'ID';
comment on column public.table_data.dict1_id is 'Dict ID';
comment on column public.table_data.dict2_id is 'Dict ID';
comment on column public.table_data.comments is 'Comments';
comment on column public.table_data.created is 'Creation timestamp';
comment on column public.table_data.modified is 'Modification timestamp';

insert into public.dict1_data(dict_id, dict_value, created)
select t, 'Value ' || t, current_timestamp - '1 day'::interval
from generate_series(1, 10) as t;

insert into public.dict2_data(dict_id, dict_value)
select t, 'Value ' || t
from generate_series(1, 10) as t;

insert into public.table_data(dict1_id, dict2_id, comments, created)
select floor(random()*10) + 1, floor(random()*10) + 1, md5(random()::text), current_timestamp - '1 day'::interval
from generate_series(1, 100);

-- Stage database setup
select 'create database stage_db'
where not exists (select from pg_database where datname = 'stage_db')\gexec
comment on database stage_db is 'Stage database';

\c stage_db

-- User tables
drop view if exists public.table_data_a;
drop view if exists public.dict1_data_a;
drop view if exists public.dict2_data_a;
drop table if exists public.table_data;
drop table if exists public.dict1_data;
drop table if exists public.dict2_data;

create table public.dict1_data(
    dict_id int not null,
    dict_value text not null,
    created timestamp not null default current_timestamp,
    modified timestamp null
);
comment on table public.dict1_data is 'Dictionary table (w/o timestamps)';
comment on column public.dict1_data.dict_id is 'Dict ID';
comment on column public.dict1_data.dict_value is 'Value';
comment on column public.dict1_data.created is 'Creation timestamp';
comment on column public.dict1_data.modified is 'Modification timestamp';

create table public.dict2_data(
    dict_id int not null,
    dict_value text not null
);
comment on table public.dict2_data is 'Dictionary table (w/o timestamps)';
comment on column public.dict2_data.dict_id is 'Dict ID';
comment on column public.dict2_data.dict_value is 'Value';

create table public.table_data(
    id int not null,
    dict1_id int not null,
    dict2_id int not null,
    comments text not null,
    created timestamp not null,
    modified timestamp null
);
comment on table public.table_data is 'Data table';
comment on column public.table_data.id is 'ID';
comment on column public.table_data.dict1_id is 'Dict ID';
comment on column public.table_data.dict2_id is 'Dict ID';
comment on column public.table_data.comments is 'Comments';
comment on column public.table_data.created is 'Creation timestamp';
comment on column public.table_data.modified is 'Modification timestamp';

-- actual data views are to be generated after initial data loading with dlt
-- see ./setup_views.sql
