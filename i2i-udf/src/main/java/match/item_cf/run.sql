
CREATE TEMPORARY FUNCTION i2i_common_udtf as "match.common.I2ICommonUDTF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";
CREATE TEMPORARY FUNCTION item_cf_udaf as "match.item_cf.ItemCFUDAF" using jar "/opt/hive-0.0.2-SNAPSHOT.jar";


--i2i计算
create table if not exists item_cf_i2i (
    item_id string,
    sim_items string
)
;

insert overwrite table item_cf_i2i

select split(item,':')[0] as item_id, item_cf_udaf(item_neighbors, cast(split(item,':')[1] as bigint))
from
(
    select i2i_common_udtf(items) as (item, item_neighbors)
    from
    (
        select user_id, concat_ws(',', collect_list(concat(item_id,':',uv))) as items
        from
        (
            select *
                , count(distinct user_id) over(partition by item_id) as uv
            from
            (
                select user_id,item_id
                    , row_number() over(partition by user_id order by rand()) as seq
                from (select * from ml_1m_data_train) t
            ) t
            where seq <= 300
        ) t
        group by user_id
    ) t
) t
group by item
;


--计算topk推荐结果
create table if not exists item_cf_topk_rec (
    user_id string,
    item_id string,
    score double,
    seq bigint
)
;

insert overwrite table item_cf_topk_rec
select user_id, sim_item_id, score, seq
from(
    select a.user_id, sim_item_id, score
        , row_number() over(partition by a.user_id order by score desc) as seq
    from (
        select a.user_id, sim_item_id, sum(score) as score
        from ml_1m_data_train a
        join (
            select item_id, split(sim_item,':')[0] as sim_item_id, cast(split(sim_item,':')[1] as double) as score
            from item_cf_i2i
            lateral view explode(split(sim_items,',')) sim_table as sim_item
        ) b
        on a.item_id = b.item_id
        group by a.user_id, sim_item_id
    ) a
    left outer join ml_1m_data_train b
    on a.user_id = b.user_id
        and a.sim_item_id = b.item_id
    where b.user_id is null
) t
where seq <= 100
;

--测评召回率
--hr@20 = 0.07830610568586292
--hr@100 = 0.26738044172675

select count(b.user_id) / count(1) as hr
from ml_1m_data_test a
left outer join (
    select *
    from item_cf_topk_rec
    where seq <= 20
) b
on a.user_id = b.user_id
    and a.item_id = b.item_id
