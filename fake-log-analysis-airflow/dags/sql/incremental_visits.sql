DELETE FROM aggr_visits WHERE date = '{{ ds }}';

INSERT INTO aggr_visits (date, cs_uri_stem, visits)
SELECT
    DATE(timestamp) as date,
    cs_uri_stem,
    COUNT(*) as visits
FROM
    cdn_logs
WHERE
    sc_status = '200' AND
    DATE(timestamp) = '{{ ds }}'
GROUP BY
    DATE(timestamp),
    cs_uri_stem;