-- Создание таблицы group_log в схеме STAGING
DROP TABLE IF EXISTS VT260317D7FA03__STAGING.group_log;

CREATE TABLE VT260317D7FA03__STAGING.group_log
(
    group_id INT NOT NULL,
    user_id INT NOT NULL,
    user_id_from INT,
    event VARCHAR(10),
    datetime TIMESTAMP,
    load_dt DATETIME DEFAULT NOW(),
    load_src VARCHAR(20) DEFAULT 's3'
)
ORDER BY datetime
SEGMENTED BY HASH(group_id, user_id) ALL NODES
PARTITION BY datetime::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(datetime::DATE, 3, 2);

-- Добавление комментариев к таблице и колонкам
COMMENT ON TABLE VT260317D7FA03__STAGING.group_log IS 'Лог работы групп';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.group_id IS 'Уникальный идентификатор группы';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.user_id IS 'Уникальный идентификатор пользователя';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.user_id_from IS 'Идентификатор пользователя, который добавил user_id в группу (NULL если сам вступил)';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.event IS 'Действие: create - создание группы, add - вступление/добавление, leave - выход из группы';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.datetime IS 'Время совершения события';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.load_dt IS 'Дата загрузки в хранилище';
COMMENT ON COLUMN VT260317D7FA03__STAGING.group_log.load_src IS 'Источник данных';

-- Часть 4 
-- Создание таблицы связи l_user_group_activity в схеме DWH
DROP TABLE IF EXISTS VT260317D7FA03__DWH.l_user_group_activity CASCADE;

CREATE TABLE VT260317D7FA03__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_user 
    	REFERENCES VT260317D7FA03__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_group 
    	REFERENCES VT260317D7FA03__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);

-- Добавление комментариев
COMMENT ON TABLE VT260317D7FA03__DWH.l_user_group_activity IS 'Линк связи пользователей и групп (активность пользователей в группах)';
COMMENT ON COLUMN VT260317D7FA03__DWH.l_user_group_activity.hk_l_user_group_activity IS 'Хэш-ключ связи пользователь-группа';
COMMENT ON COLUMN VT260317D7FA03__DWH.l_user_group_activity.hk_user_id IS 'Хэш-ключ пользователя (ссылка на h_users)';
COMMENT ON COLUMN VT260317D7FA03__DWH.l_user_group_activity.hk_group_id IS 'Хэш-ключ группы (ссылка на h_groups)';
COMMENT ON COLUMN VT260317D7FA03__DWH.l_user_group_activity.load_dt IS 'Дата загрузки связи';
COMMENT ON COLUMN VT260317D7FA03__DWH.l_user_group_activity.load_src IS 'Источник данных связи';

-- мигрирование данных по этапу 5
INSERT INTO VT260317D7FA03__DWH.l_user_group_activity(
	hk_l_user_group_activity, 
	hk_user_id, 
	hk_group_id, 
	load_dt,
	load_src)

SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    NOW() as load_dt,
    's3' as load_src
FROM VT260317D7FA03__STAGING.group_log as gl
LEFT JOIN VT260317D7FA03__DWH.h_users as hu ON gl.user_id = hu.user_id
LEFT JOIN VT260317D7FA03__DWH.h_groups as hg ON gl.group_id = hg.group_id
WHERE gl.event IN ('add', 'create')  -- Берем только события, когда пользователь вступил или создал группу
  AND hu.hk_user_id IS NOT NULL      -- Исключаем записи без соответствия в хабе пользователей
  AND hg.hk_group_id IS NOT NULL     -- Исключаем записи без соответствия в хабе групп
  AND HASH(hu.hk_user_id, hg.hk_group_id) NOT IN (
      SELECT hk_l_user_group_activity 
      FROM VT260317D7FA03__DWH.l_user_group_activity
  );  -- Предотвращаем дублирование при повторной загрузке

  -- этап 6 
  -- Создание сателлита s_auth_history
DROP TABLE IF EXISTS VT260317D7FA03__DWH.s_auth_history CASCADE;

CREATE TABLE VT260317D7FA03__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_history_link REFERENCES VT260317D7FA03__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INT,
    event VARCHAR(10),
    event_dt DATETIME,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);

-- Добавление комментариев
COMMENT ON TABLE VT260317D7FA03__DWH.s_auth_history IS 'Сателлит истории авторизации/активности пользователей в группах';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.hk_l_user_group_activity IS 'Хэш-ключ связи пользователь-группа (ссылка на l_user_group_activity)';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.user_id_from IS 'Идентификатор пользователя, который добавил другого в группу (NULL если сам вступил)';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.event IS 'Событие: create - создание группы, add - вступление/добавление, leave - выход из группы';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.event_dt IS 'Дата и время совершения события';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.load_dt IS 'Дата загрузки записи в хранилище';
COMMENT ON COLUMN VT260317D7FA03__DWH.s_auth_history.load_src IS 'Источник данных';


-- Наполнение сателлита s_auth_history
INSERT INTO VT260317D7FA03__DWH.s_auth_history(
	hk_l_user_group_activity, 
	user_id_from, 
	event, 
	event_dt, 
	load_dt,
	load_src)
SELECT 
    luga.hk_l_user_group_activity,
    gl.user_id_from,  -- может быть NULL, если пользователь вступил сам
    gl.event,
    gl.datetime as event_dt,
    NOW() as load_dt,
    's3' as load_src
FROM VT260317D7FA03__STAGING.group_log as gl
LEFT JOIN VT260317D7FA03__DWH.h_groups as hg ON gl.group_id = hg.group_id
LEFT JOIN VT260317D7FA03__DWH.h_users as hu ON gl.user_id = hu.user_id
LEFT JOIN VT260317D7FA03__DWH.l_user_group_activity as luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
WHERE luga.hk_l_user_group_activity IS NOT NULL  -- Только существующие связи
  AND NOT EXISTS (  -- Предотвращаем дублирование
      SELECT 1 
      FROM VT260317D7FA03__DWH.s_auth_history sah
      WHERE sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
        AND sah.event = gl.event
        AND sah.event_dt = gl.datetime
  );

-- этап 7 
-- CTE для подсчета пользователей, которые написали хотя бы одно сообщение в группе
with user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from VT260317D7FA03__DWH.l_groups_dialogs as lgd
    inner join VT260317D7FA03__DWH.l_user_message as lum 
        on lgd.hk_message_id = lum.hk_message_id
    group by lgd.hk_group_id
)

select 
    hk_group_id,
    cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages desc
limit 10;

-- CTE для подсчета пользователей, которые вступили в группы (только 10 самых старых групп)
with 
oldest_groups as (
    select hk_group_id
    from VT260317D7FA03__DWH.h_groups
    order by registration_dt
    limit 10
),
user_group_log as (
    select 
        og.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from oldest_groups og
    left join VT260317D7FA03__DWH.l_user_group_activity luga 
        on og.hk_group_id = luga.hk_group_id
    left join VT260317D7FA03__DWH.s_auth_history sah 
        on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        and sah.event = 'add'
    group by og.hk_group_id
)
select 
    hk_group_id,
    cnt_added_users
from user_group_log
order by cnt_added_users desc
limit 10;

-- Полный запрос для анализа доли активных пользователей в 10 самых старых группах
with 
oldest_groups as (
    select hk_group_id
    from VT260317D7FA03__DWH.h_groups
    order by registration_dt
    limit 10
),
user_group_log as (
    select 
        og.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from oldest_groups og
    left join VT260317D7FA03__DWH.l_user_group_activity luga 
        on og.hk_group_id = luga.hk_group_id
    left join VT260317D7FA03__DWH.s_auth_history sah 
        on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        and sah.event = 'add'
    group by og.hk_group_id
),
user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from VT260317D7FA03__DWH.l_groups_dialogs lgd
    inner join VT260317D7FA03__DWH.l_user_message lum 
        on lgd.hk_message_id = lum.hk_message_id
    where lgd.hk_group_id in (select hk_group_id from oldest_groups)
    group by lgd.hk_group_id
)
select 
    ugl.hk_group_id,
    coalesce(ugl.cnt_added_users, 0) as cnt_added_users,
    coalesce(ugm.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages,
    case 
        when coalesce(ugl.cnt_added_users, 0) > 0 
        then round(100.0 * coalesce(ugm.cnt_users_in_group_with_messages, 0) / ugl.cnt_added_users, 2)
        else 0
    end as group_conversion
from user_group_log ugl
left join user_group_messages ugm on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;

