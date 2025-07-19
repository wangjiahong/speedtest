-- Top 10 locations by average download speed (last 24h)
SELECT location, AVG(avg_download_speed) AS avg_dl
FROM iceberg.gold.speedtest_agg
WHERE date >= current_date - INTERVAL '1' DAY
GROUP BY location
ORDER BY avg_dl DESC
LIMIT 10;

-- Hourly download/upload speed trends for a location
SELECT date, hour, AVG(avg_download_speed) AS avg_dl, AVG(avg_upload_speed) AS avg_ul
FROM iceberg.gold.speedtest_agg
WHERE location = 'New York'
GROUP BY date, hour
ORDER BY date, hour;

-- Average latency by location
SELECT location, AVG(avg_latency) AS avg_latency
FROM iceberg.gold.speedtest_agg
GROUP BY location
ORDER BY avg_latency; 