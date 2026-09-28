-- One row per order_id — the newest copy by updated_at.
--
-- The upstream feed re-ingests the same order_id 2-3 times (sometimes with a
-- changed status); keep only the latest and hand a clean, unique grain to
-- everything downstream. A dbt model is just a SELECT — no CREATE/DROP.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as rn
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    sku,
    quantity,
    price,
    status,
    order_date,
    updated_at
from ranked
where rn = 1
