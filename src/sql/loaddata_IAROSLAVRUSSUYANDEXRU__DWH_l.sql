
-- Заполнение таблиц с линками --
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

INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_user_message
(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
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

INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs
(hk_l_groups_dialogs,hk_message_id, hk_group_id, load_dt, load_src)
select
    hash(hd.hk_message_id,hg.hk_group_id),
    hd.hk_message_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from IAROSLAVRUSSUYANDEXRU__STAGING.dialogs as g
left join IAROSLAVRUSSUYANDEXRU__DWH.h_dialogs as hd on g.message_id = hd.message_id
INNER join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on g.message_group  = hg.group_id --Сделаю left - получу ошибку
where hash(hd.hk_message_id,hg.hk_group_id)
not in (select hk_l_groups_dialogs from IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs);

INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity
(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select
    hash(hu.hk_user_id,hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from IAROSLAVRUSSUYANDEXRU__STAGING.group_log as g
left join IAROSLAVRUSSUYANDEXRU__DWH.h_users as hu on g.user_id = hu.user_id
left join IAROSLAVRUSSUYANDEXRU__DWH.h_groups as hg on g.group_id  = hg.group_id
where hash(hu.hk_user_id,hg.hk_group_id)
not in (select hk_l_user_group_activity from IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity);
