CREATE TABLE IAROSLAVRUSSUYANDEXRU__STAGING.users
	(id integer PRIMARY KEY,
	chat_name varchar(200),
	registration_dt datetime,
	country varchar(200),
	age integer)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES

;
--SELECT * FROM IAROSLAVRUSSUYANDEXRU__STAGING.users;
DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.users;

CREATE TABLE IAROSLAVRUSSUYANDEXRU__STAGING.groups
	(id integer PRIMARY KEY,
	admin_id integer,
	group_name varchar(100),
	registration_dt datetime,
	is_private integer)
ORDER BY id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
;

ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.groups ADD CONSTRAINT groups_admin_id_fk
FOREIGN KEY (admin_id) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.users(id);

DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.groups;


CREATE TABLE IAROSLAVRUSSUYANDEXRU__STAGING.dialogs
	(message_id integer NOT NULL,
	message_ts datetime,
	message_from integer,
	message_to integer,
	message varchar (1000),
	massage_group int)
ORDER BY message_id
SEGMENTED BY HASH(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.dialogs ADD CONSTRAINT dialogs_message_from_fk
FOREIGN KEY (message_from) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.users(id);
ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.dialogs ADD CONSTRAINT dialogs_message_to_fk
FOREIGN KEY (message_to) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.users(id);
ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.dialogs ADD CONSTRAINT dialogs_message_group_fk
FOREIGN KEY (massage_group) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.groups(id);


DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.dialogs;




drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.h_users;

create table IAROSLAVRUSSUYANDEXRU__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.h_groups;
create table IAROSLAVRUSSUYANDEXRU__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs;

create table IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    datetime datetime,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from IAROSLAVRUSSUYANDEXRU__STAGING.users
where hash(id) not in (select hk_user_id from IAROSLAVRUSSUYANDEXRU__DWH.h_users);


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from IAROSLAVRUSSUYANDEXRU__STAGING.groups
where hash(id) not in (select hk_group_id from IAROSLAVRUSSUYANDEXRU__DWH.h_groups);

INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs(hk_message_id, message_id,datetime,load_dt,load_src)
select
       hash(message_id) as hk_group_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from IAROSLAVRUSSUYANDEXRU__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs);

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_user_message;

create table IAROSLAVRUSSUYANDEXRU__DWH.l_user_message
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

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_admins;

create table IAROSLAVRUSSUYANDEXRU__DWH.l_admins
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

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs;

create table IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs
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




INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from IAROSLAVRUSSUYANDEXRU__STAGING.groups as g
left join IAROSLAVRUSSUYANDEXRU__DWH.h_users as hu on g.admin_id = hu.user_id
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from IAROSLAVRUSSUYANDEXRU__DWH.l_admins);


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_user_message(
hk_l_user_message,
hk_user_id,
hk_message_id,
load_dt,
load_src)
select
hash(hu.hk_user_id,hd.hk_message_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from IAROSLAVRUSSUYANDEXRU__STAGING.dialogs as g
left join IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs as hd on g.message_id = hd.message_id
left join IAROSLAVRUSSUYANDEXRU__DWH.h_users as hu on g.message_from  = hu.user_id
where hash(hu.hk_user_id,hd.hk_message_id)
not in (select hk_l_user_message from IAROSLAVRUSSUYANDEXRU__DWH.l_user_message);


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs(hk_l_groups_dialogs,
hk_message_id, hk_group_id, load_dt, load_src)
select
hash(hd.hk_message_id,hg.hk_group_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from IAROSLAVRUSSUYANDEXRU__STAGING.dialogs as g
left join IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs as hd on g.message_id = hd.message_id
INNER join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on g.message_group  = hg.group_id
where hash(hd.hk_message_id,hg.hk_group_id)
not in (select hk_l_groups_dialogs from IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs);





drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_admins;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_admins
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


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from IAROSLAVRUSSUYANDEXRU__DWH.l_admins as la
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;



СОЗДАЕМ И ЗАПОЛНЯЕМ САТТЕЛИТЫ

drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_admins;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_admins
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


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from IAROSLAVRUSSUYANDEXRU__DWH.l_admins as la
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


--s_group_name
drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_group_name;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_group_name
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


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_group_name
(hk_group_id, group_name,load_dt,load_src)
SELECT
hg.hk_group_id,
sg.group_name,
now() as load_dt,
's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.groups as sg
on sg.id = hg.group_id;

--
--s_group_private_status
drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_group_private_status;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_group_private_status
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


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_group_private_status
(hk_group_id, is_private, load_dt, load_src)
SELECT
hg.hk_group_id,
sg.is_private,
now() as load_dt,
's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.groups as sg
on sg.id = hg.group_id;

------------------------------

--s_dialog_info
drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_dialog_info;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_dialog_info
(hk_message_id bigint not null CONSTRAINT fk_s_dialog_info
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs(hk_message_id),
message varchar (1000),
message_from int,
message_to int,
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_dialog_info
(hk_message_id, message, message_from,message_to,load_dt,load_src)
SELECT
	hd.hk_message_id,
	std.message,
	std.message_from,
	std.message_to,
	now() as load_dt,
	's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs hd
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.dialogs std
ON hd.message_id = std.message_id;

----

--s_user_socdem
drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_user_socdem;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_user_socdem
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



INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_user_socdem
(hk_user_id,country,age,load_dt,load_src)
SELECT
	hu.hk_user_id,
	stu.country,
	stu.age,
	now() as load_dt,
	's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_users hu
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.users stu
ON hu.user_id = stu.id;


--s_user_chatinfo
drop table if exists IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo;

create table IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo
(hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo
REFERENCES IAROSLAVRUSSUYANDEXRU__DWH.h_users(hk_user_id),
chat_name varchar (200),
load_dt datetime,
load_src varchar(20)
)order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
SELECT
	hu.hk_user_id,
	stu.chat_name,
	now() as load_dt,
	's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_users hu
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.users stu
ON hu.user_id = stu.id;
