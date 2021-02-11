select count(b.item_id) / count(1) as hr
from ml_1m_data_test a
left outer join (
    select *
    from (
        select a.*, row_number() over(partition by a.user_id order by a.score desc) as seq
        from (
            select b.user_id, a.item_id, a.score
            from (
                select *, 1 as link
                from (
                    select item_id, count(1) as score
                    from ml_1m_data_train
                    group by item_id
                ) t
                order by score desc
                limit 200
            ) a
            join (
                select distinct user_id , 1 as link
                from ml_1m_data_train
            ) b
            on a.link = b.link
        ) a
        left outer join ml_1m_data_train b
        on a.item_id = b.item_id
            and a.user_id = b.user_id
        where b.item_id is null
    ) t
    where seq <= 100
) b
on a.item_id = b.item_id
    and a.user_id = b.user_id
;

-- hr@20 = 0.06722300812266131
-- hr@100 = 0.2198024094186365
