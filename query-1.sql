--일별 Active User 데이터 추출
SELECT server_datetime AS '일자',
	COUNT(DISTINCT identity_adid) AS 'active User'
FROM dfncodetestdb.event_data_table
WHERE server_datetime > '2018-05-01'
	and server_datetime < '2018-05-16'
GROUP BY 1
ORDER BY 1;



--일별 구매 유저 및 구매 금액 추출
SELECT server_datetime AS '일자', 
       COUNT(DISTINCT identity_adid) AS 'active User',
       SUM(price) AS '구매 금액'
FROM dfncodetestdb.event_data_table
WHERE server_datetime > '2018-05-01' 
    AND server_datetime < '2018-05-16'
GROUP BY 1
ORDER BY 1;



--이벤트별 User 데이터 추출
SELECT event_name AS 'event', 
       COUNT(DISTINCT identity_adid) AS 'active User'
FROM event_data_table
WHERE server_datetime > '2018-05-01' 
    and server_datetime < '2018-05-16'
GROUP BY 1
ORDER BY 1;



--캠페인별 User 데이터 추출
SELECT campaign AS 'campaign_id', 
       COUNT(DISTINCT identity_adid) AS 'active User'
FROM attribution_data_table
WHERE server_datetime > '2018-05-01' 
    AND server_datetime < '2018-05-16'
GROUP BY 1
ORDER BY 1;



--캠페인별 구매 금액 추출
SELECT campaign AS 'campaign_id', 
       COUNT(DISTINCT a.identity_adid) AS 'active User',
       SUM(price) AS '구매 금액'
FROM attribution_data_table a
JOIN event_data_table e ON a.identity_adid = e.identity_adid
WHERE a.server_datetime > '2018-05-01' 
    and a.server_datetime < '2018-05-16'
GROUP BY 1
ORDER BY 1;



--국가, 캠페인별 구매 금액 추출
SELECT country,
       campaign AS 'campaign_id', 
       COUNT(DISTINCT a.identity_adid) AS 'active User',
       SUM(price) AS '구매 금액'
FROM attribution_data_table a
JOIN event_data_table e ON a.identity_adid = e.identity_adid
WHERE a.server_datetime > '2018-05-01' 
    and a.server_datetime < '2018-05-16'
GROUP BY 1, 2
ORDER BY 1, 2;


--아래는 실제로 athena로 실행했을 때 timeout이 되어 결과는 못 보았지만 시도해봤던 쿼리만 기재하였습니다.

--Funnel 데이터 추출
WITH
total_firstopen AS (
    SELECT DISTINCT e.server_datetime, 
            COUNT(e.identity_adid) OVER (
                                        PARTITION BY e.server_datetime, e.event_name
                                        ) AS fo 
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = e.server_datetime
    WHERE e.event_name = 'abx:firstopen 수행 유저' 
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
    ),
total_login AS ( 
    SELECT DISTINCT e.server_datetime, 
            COUNT(e.identity_adid) OVER (
                                        PARTITION BY e.server_datetime, e.event_name
                                        ) AS lo 
    FROM event_data_table e 
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE e.event_name = 'abx:login 수행 유저'
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
    ), 
total_purchase AS( 
    SELECT DISTINCT e.server_datetime, 
                COUNT(e.identity_adid) OVER (
                                            PARTITION BY e.server_datetime,e.event_name
                                            ) AS pur 
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE e.event_name = 'abx:purchase 수행유저'
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
    ) 
SELECT a.server_datetime , 
        a.fo AS 'abx:firstopen 수행 유저',
        b.lo AS 'abx_login 수행 유저', 
        c.pur AS 'abx_purchase 수행 유저' 
FROM total_firstopen a 
    JOIN total_login b on a.server_datetime = b.server_datetime 
    JOIN total_purchase c on b.server_datetime = c.server_datetime;



--파트너별 new_install 유저의 funnel 데이터 추출
WITH
new_install AS (
    SELECT DISTINCT a.partner,
            COUNT(e.identity_adid) OVER (
                                        PARTITION BY e.partner, e.event_name
                                        ) AS ni
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE a.attribution_type = 0
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
),
total_firstopen AS (
    SELECT DISTINCT a.partner, 
                COUNT(e.identity_adid) OVER (
                                            PARTITION BY e.partner, e.event_name
                                            ) AS fo
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE e.event_name = 'abx:firstopen 수행유저'
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
),
total_login AS (
    SELECT DISTINCT a.partner, 
                COUNT(e.identity_adid) OVER (
                                            PARTITION BY e.partner, e.event_name
                                            ) AS lo
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE e.event_name = 'abx:login 수행유저'
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
),
total_purchase AS (
    SELECT DISTINCT a.partner, 
                COUNT(e.identity_adid) OVER (
                                            PARTITION BY e.partner, e.event_name
                                            ) AS pur
    FROM event_data_table e
    LEFT JOIN attribution_data_table a
    ON e.server_datetime = a.server_datetime
    WHERE e.event_name = 'abx:purchase 수행유저'
        AND e.server_datetime > '2018-05-01' 
        AND e.server_datetime < '2018-05-16'
)
SELECT a.partner , 
        a.ni AS 'new install user'
        b.fo AS 'abx:firstopen 수행 유저',
        c.lo AS 'abx_login 수행 유저', 
        d.pur AS 'abx_purchase 수행 유저' 
FROM new_install a
    JOIN total_firstopen b ON a.partner = b.partner
    JOIN total_login c ON a.server_datetime = b.server_datetime 
    JOIN total_purchase d ON b.server_datetime = c.server_datetime;