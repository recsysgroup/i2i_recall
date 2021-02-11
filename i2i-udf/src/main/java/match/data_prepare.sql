
CREATE TABLE if not exists ml_1m_data_train (user_id STRING, item_id STRING);

CREATE TABLE if not exists ml_1m_data_test (user_id STRING, item_id STRING);

from (
    select cols[0] as user_id
        , cols[1] as item_id
        , row_number() over(partition by cols[0] order by cast(cols[3] as bigint)) as seq
        , count(1) over(partition by cols[0]) as cnt
    from
    (
        select split(text, "::") as cols
        from ml_1m_ratings
    ) t
    where cols[2] >= '4'
) t
insert overwrite table ml_1m_data_train
select user_id, item_id
where seq <= floor(cnt*0.7)

insert overwrite table ml_1m_data_test
select user_id, item_id
where seq > floor(cnt*0.7)
;