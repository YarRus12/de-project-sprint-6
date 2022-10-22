--СОЗДАНИЕ СТАЙДЖА--
-- Создаем таблицу с пользователями --
--DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.users;
CREATE TABLE IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.users
	(id integer PRIMARY KEY,
	chat_name varchar(200),
	registration_dt datetime,
	country varchar(200),
	age integer)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;

--таблица с группами пользователей
--DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.groups;
CREATE TABLE IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.groups
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

--Таблица с диалогами --
--DROP TABLE IF EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.dialogs;
CREATE TABLE IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.dialogs
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


CREATE TABLE IF NOT EXISTS IAROSLAVRUSSUYANDEXRU__STAGING.group_log
(
    group_id integer NOT NULL,
    user_id integer NOT NULL,
    user_id_from integer,
    event varchar(10),
    datetime datetime
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);
ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.group_log ADD CONSTRAINT log_group_fk
FOREIGN KEY (group_id) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.groups(group_id);
ALTER TABLE IAROSLAVRUSSUYANDEXRU__STAGING.group_log ADD CONSTRAINT log_user_fk
FOREIGN KEY (user_id) REFERENCES IAROSLAVRUSSUYANDEXRU__STAGING.users(user_id);