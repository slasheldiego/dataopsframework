SELECT date_trunc('minute', ts) as minute, step, status, count(*) AS events
FROM ops_db.ops_event_log
GROUP BY 1,2,3
ORDER BY 1 DESC;
