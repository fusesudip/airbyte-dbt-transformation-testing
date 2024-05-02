WITH merged_view AS (
    SELECT
        f._ab_source_file_url,
        f.date,
        f.permanentLink,
        f.value,
        f.srcid, 
        f.configuration_id AS f_configuration_id, 
        bcm.configuration_id AS bcm_configuration_id
    FROM {{ref("stg__facebook__acq")}} f
    INNER JOIN  {{source('public', 'brands_configuration_merged')}} bcm
    ON CAST(f.configuration_id AS INTEGER) = bcm.configuration_id
)
SELECT *
FROM merged_view
WHERE srcid IN (
    SELECT srcid
    FROM merged_view
    GROUP BY srcid
    HAVING COUNT('idKey') > 1
)
order by srcid