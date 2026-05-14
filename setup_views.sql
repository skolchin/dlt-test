-- the script is to be executed on stage database AFTER initial data load with dlt
create view public.dict1_data_a as
    select distinct on (t.dict_id) t.*
    from public.dict1_data t
    join public._dlt_loads d on t._dlt_load_id = d.load_id and d.status = 0
    order by t.dict_id, d.inserted_at desc;
comment on view public.dict1_data_a is 'Actual data view on dictionary table with timestamps';

create view public.dict2_data_a as
    select distinct on (t.dict_id) t.*
    from public.dict2_data t
    join public._dlt_loads d on t._dlt_load_id = d.load_id and d.status = 0
    order by t.dict_id, d.inserted_at desc;
comment on view public.dict2_data_a is 'Actual data view on dictionary table w/o timestamps';

create view public.table_data_a as
    select distinct on (t.id) t.*
    from public.table_data t
    join public._dlt_loads d on t._dlt_load_id = d.load_id and d.status = 0
    order by t.id, d.inserted_at desc;
comment on view public.table_data_a is 'Actual data view on data table';
