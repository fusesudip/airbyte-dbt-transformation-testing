with filtered_data as (
    SELECT 
        _ab_source_file_url,
        CAST(substring(_ab_source_file_url from 'facebook/(\d+)/originals') AS INTEGER) AS configuration_id,
        (json_data->>'date')::timestamp AS date,
        json_data->>'srcId' AS srcId,
        (json_data->'misc'->>'permanentLink') AS permanentLink,
        json_data->>'value' AS value
    FROM (
        SELECT 
            _ab_source_file_url,
            jsonb_array_elements(entries) AS json_data
        FROM {{ source('public', 'facebook') }}
        WHERE _ab_source_file_url LIKE '%originals%'
    ) AS data
)

select * from filtered_data