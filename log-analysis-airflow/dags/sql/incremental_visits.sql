-- Upsert incremental visit count to aggr_visits table
INSERT INTO aggr_visits (cs_uri_stem, date, number_of_visits)
SELECT
	cs_uri_stem,
	date, 
	count(*)as number_of_visits
FROM cdn_logs
WHERE date = '{{ ds }}'
GROUP BY date, cs_uri_stem
ORDER BY date asc, number_of_visits desc
ON CONFLICT (cs_uri_stem, date) DO UPDATE SET 
    number_of_visits = EXCLUDED.number_of_visits;