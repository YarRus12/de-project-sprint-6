-- Создание таблиц саттелитов --

--Таблица с администраторами групп--
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_admins;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Таблица с наименованиями групп--
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_group_name;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_groups(hk_group_id),
group_name varchar(100),
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Таблица с приватным статусом групп--
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_group_private_status;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_group_private_status
(
hk_group_id bigint not null CONSTRAINT fk_s_group_status
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_groups(hk_group_id),
is_private int,
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Таблица с информацией о сообщениях--
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo
(hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_users(hk_user_id),
chat_name varchar (200),
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Таблица с информацией о пользователях--
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_user_socdem;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_user_socdem
(hk_user_id bigint not null CONSTRAINT fk_s_user_socdem
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_users(hk_user_id),
country varchar(200),
age int,
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.s_auth_history
(hk_l_user_group_activity bigint not null CONSTRAINT fk_l_user_group_activity
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity),
user_id_from integer,
event varchar(15),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);