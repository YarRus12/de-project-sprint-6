;with user_group_messages as (
    SELECT lgd.hk_group_id as hk_group_id, count(DISTINCT sdi.message_from) as cnt_users_in_group_with_messages
FROM IAROSLAVRUSSUYANDEXRU__DWH.s_dialog_info sdi
INNER JOIN IAROSLAVRUSSUYANDEXRU__DWH.l_groups_dialogs lgd
ON sdi.hk_message_id = lgd.hk_message_id
GROUP BY lgd.hk_group_id
),
user_group_log
as (select ugm.hk_group_id, count(distinct luga.hk_user_id) as cnt_added_users
FROM IAROSLAVRUSSUYANDEXRU__DWH.l_user_group_activity luga
INNER JOIN user_group_messages ugm
ON luga.hk_group_id = ugm.hk_group_id
WHERE luga.hk_l_user_group_activity in (
SELECT hk_l_user_group_activity FROM IAROSLAVRUSSUYANDEXRU__DWH.s_auth_history
WHERE event = 'add')
GROUP BY ugm.hk_group_id)
SELECT
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users as group_conversion
FROM user_group_log as ugl
INNER JOIN user_group_messages as ugm
ON ugm.hk_group_id=ugl.hk_group_id
WHERE ugl.hk_group_id in (select hk_group_id
                    from IAROSLAVRUSSUYANDEXRU__DWH.h_groups
                    order by registration_dt limit 10)
ORDER BY ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users DESC;