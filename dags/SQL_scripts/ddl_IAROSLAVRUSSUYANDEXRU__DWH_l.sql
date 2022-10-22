-- Создание таблиц связей --
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_user_message;
--Связующая таблица между пользователями и диалогами --
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Связующая таблица между пользователями и группами --
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_admins;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_admin_group_user REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_admin_user_group REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--Связующая таблица между пользователями и группами и диалогами --
--drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs;
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_group_dialog REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_dialog_group REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- Связующая таблица групповой активности --
create table IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_group_dialog REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_dialog_group REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);