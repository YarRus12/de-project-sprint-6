CREATE TABLE members
(
    id int NOT NULL,
    age int,
    gender char,
    email varchar(50),
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
);


COPY members ( id, age, gender, email ENFORCELENGTH)
FROM LOCAL 'C:\Users\OMEN\Dropbox\Warlock\Data_engineering\yandex_practicum\lesson6\s6-lessons\Тема 2. Аналитические СУБД. Vertica\5. Запись данных в Vertica\Задание 2\members.csv'
DELIMITER ';'
REJECTED DATA AS TABLE members_rej
;

create table orders
(
id  varchar(2000) PRIMARY KEY,
registration_ts timestamp(6),
user_id varchar(2000),
is_confirmed int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;


DROP TABLE IAROSLAVRUSSUYANDEXRU.USERS  Cascade;

CREATE table IAROSLAVRUSSUYANDEXRU.USERS (
    id int,
    change_ts timestamp(6),
    chat_name varchar(128),
    gender varchar(1),
    age_cohort varchar(4),
    PRIMARY KEY (id)
);

DROP TABLE IAROSLAVRUSSUYANDEXRU.dialogs  Cascade;


create table IAROSLAVRUSSUYANDEXRU.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES IAROSLAVRUSSUYANDEXRU.users(id),
    message_to int REFERENCES IAROSLAVRUSSUYANDEXRU.users(id),
    message varchar(1000),
    message_type varchar(100)
)
SEGMENTED BY hash(message_id) all nodes
;

DROP TABLE IAROSLAVRUSSUYANDEXRU.USERS  Cascade;

CREATE table IAROSLAVRUSSUYANDEXRU.USERS (
    id int,
    change_ts timestamp(6),
    chat_name varchar(128),
    gender varchar(1),
    age_cohort varchar(4),
    PRIMARY KEY (id)
);

DROP TABLE IAROSLAVRUSSUYANDEXRU.dialogs  Cascade;


create table IAROSLAVRUSSUYANDEXRU.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES IAROSLAVRUSSUYANDEXRU.users(id),
    message_to int REFERENCES IAROSLAVRUSSUYANDEXRU.users(id),
    message varchar(1000),
    message_type varchar(100)
)
SEGMENTED BY hash(message_id) all nodes
;


DROP TABLE IAROSLAVRUSSUYANDEXRU.dialogs  Cascade;
 create table IAROSLAVRUSSUYANDEXRU.dialogs
 (
     message_id   int PRIMARY KEY,
     message_ts   timestamp(6),
     message_from int REFERENCES members(id),
     message_to int REFERENCES members(id),
     message varchar(1000),
     message_type varchar(100)
 )
 order by message_id, message_ts
 SEGMENTED BY hash(message_id) all nodes
 PARTITION BY message_ts::date
 ;


MERGE INTO members tgt
USING /* Запрос, формирующий входящие данные */
    (SELECT id, age, gender, email /*список колонок*/
    FROM members_inc)src /*таблица из которой нужно взять данные*/
ON  /* ключи MERGE */
    tgt.id = src.id
WHEN MATCHED and (tgt.gender <> src.gender/*сравнение записей*/
                                    or tgt.email <> src.email
                                    or tgt.age <> src.age )
    THEN UPDATE SET gender = src.gender, email = src.email, age= src.age
WHEN NOT MATCHED
    THEN INSERT (id, age, gender, email)
    VALUES (src.id,src.age, src.gender, src.email);


DROP TABLE dialogs;
create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int,
    message_to int,
    message varchar(1000),
    message_type varchar(100)
)
order by message_id, message_ts
SEGMENTED BY hash(message_id) all nodes
--PARTITION BY TRUNC(message_ts at timezone 'UTC-6', 'MM');
PARTITION BY message_ts::date;


DROP TABLE dialogs;
create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int,
    message_to int,
    message varchar(1000),
    message_type varchar(100)
)
order by message_id, message_ts
SEGMENTED BY hash(message_id) all nodes
--PARTITION BY TRUNC(message_ts at timezone 'UTC-6', 'MM');
PARTITION BY message_ts::date
GROUP BY CALENDAR_HIERARCHY_DAY(message_ts::DATE, 3, 2);
;