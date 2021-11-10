{{ config(materialized='view') }}

WITH partition_for_deduplicate AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY ma_don_hang, ma_san_pham ORDER BY Ngay_Xuat DESC) AS index_by_ma_ncc,
        *
    FROM
        {{ ref('raw_data_nivea_tiki') }}
)
SELECT
    *
    EXCEPT(
        index_by_ma_ncc
    )
FROM
    partition_for_deduplicate
WHERE
    index_by_ma_ncc = 1