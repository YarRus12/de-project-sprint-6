
-- Заполнение таблиц саттелитов --
--Заполнение таблиц админов--
INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_admins
(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
	True as is_admin,
	hg.registration_dt,
	now() as load_dt,
	's3' as load_src
from IAROSLAVRUSSUYANDEXRU__DWH.l_admins as la
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

--Заполнение таблиц наименования групп--
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

--Заполнение таблиц с приватным статусом групп--
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

--Заполнение таблиц с информацией о сообщениях--
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

--Заполнение таблиц с информацией о пользователях--
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

--Заполнение таблиц с информацией о чатах--
INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
SELECT
	hu.hk_user_id,
	stu.chat_name,
	now() as load_dt,
	's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__DWH.h_users hu
LEFT JOIN IAROSLAVRUSSUYANDEXRU__STAGING.users stu
ON hu.user_id = stu.id;

--Заполнение таблиц с информацией о действиях пользователя в группе--
INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.s_auth_history
(hk_l_user_group_activity, user_id_from,
event, event_dt, load_dt,load_src)
SELECT
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime_ts,
	now() as load_dt,
	's3' as load_src
FROM IAROSLAVRUSSUYANDEXRU__STAGING.group_log gl
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
left join IAROSLAVRUSSUYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
left join IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
--LIMIT 100000 --при тестировании получал timeout поэтому сокращал число записей
;