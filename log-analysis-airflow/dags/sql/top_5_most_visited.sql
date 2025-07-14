-- Top 5 most visited pages daily

WITH RankedDailyVisits as(
	SELECT  
		date, 
		cs_uri_stem,
		visits,
		ROW_NUMBER() OVER(PARTITION BY date ORDER BY visits DESC) as rank_per_day
	FROM aggr_visits
)
SELECT 
	date, 
	cs_uri_stem, 
	visits
FROM RankedDailyVisits
WHERE rank_per_day <= 5
ORDER BY date ASC, visits DESC;