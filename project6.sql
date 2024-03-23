--Шаг 2. Создать таблицу group_log в Vertica
drop table if exists STV2024021942__STAGING.group_log;

create table STV2024021942__STAGING.group_log
(
	group_id bigint not null,
	user_id int not null,
	user_id_from varchar(20),
	event varchar(20),
	'datetime' datetime
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--Шаг 4. Создать таблицу связи
-- Добавьте в схему *__DWH таблицу связи l_user_group_activity с такими полями:
drop table if exists STV2024021942__DWH.l_user_group_activity;

create table STV2024021942__DWH.l_user_group_activity
(
	hk_l_user_group_activity int primary key,
	hk_user_id int foreign key reference STV2024021942__DWH.h_users,
	hk_group_id int foreign key reference STV2024021942__DWH.h_groups,
	load_dt datetime,
	load_src varchar(20)
	
)

-- Шаг 5. Создать скрипты миграции в таблицу связи
INSERT into STV2024021942__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_srv )
select distinct hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_srv
from STV2024021942__STAGING.group_log as gl
left join STV2024021942__DWH.h_users as us on us.hk_user_id = gl.hk_user_id
left join STV2024021942__DWH.h_groups as gr on gr.hk_group_id = gl.hk_group_id


--Шаг 6. Создать и наполнить сателлит
create table STV2024021942__DWH.s_auth_history
(
	hk_l_user_group_activity int,
	user_id_from int,
	event,
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)	
)

INSERT INTO STV2024021942__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)

select ***Ваш Код здесь***

from STV2024021942__STAGING.group_log as gl
left join STV2024021942__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2024021942__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2024021942__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;


--шаг 7.1
with user_group_messages as (
    select 
        hk_group_id,
        count(distinct user_id) as cnt_users_in_group_with_messages
    from your_table_name
    where message is not null
    group by hk_group_id
)

select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;


--шаг 7.2
with user_group_log as (
    select 
        hk_group_id,
        count(distinct user_id) as cnt_added_users
    from MY__DWH.s_auth_history
    where event = 'add' 
    group by hk_group_id
)

select hk_group_id,
       cnt_added_users
from user_group_log
order by hk_group_id  -- сортируем по хэш-ключу группы
limit 10;



--шаг7.3
with user_group_log as (
    select 
        hk_group_id,
        count(distinct case when event = 'add' then user_id end) as cnt_added_users,
        count(distinct case when event = 'message' then user_id end) as cnt_users_in_group_with_messages
    from MY__DWH.s_auth_history
    where event in ('add', 'message')  
    group by hk_group_id
),

group_activity as (
    select 
        hk_group_id,
        cnt_added_users,
        cnt_users_in_group_with_messages,
        case 
            when cnt_added_users > 0 then round(cnt_users_in_group_with_messages * 100.0 / cnt_added_users, 2)
            else 0
        end as group_conversion
    from user_group_log
)

select hk_group_id,
       cnt_added_users,
       cnt_users_in_group_with_messages,
       group_conversion
from group_activity
order by group_conversion desc
limit 10;

