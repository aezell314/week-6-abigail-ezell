-- One row per customer_id — the highest record_version wins.
--
-- Customers are re-ingested too (~5% appear twice with a higher
-- record_version). Dedup here, standalone, so anything downstream can
-- ref('customers_deduped') without the order counts fanning out on the join.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by record_version desc
        ) as rn
    from {{ source('raw', 'customers') }}
)

select
    customer_id,
    name,
    email,
    signup_date,
    record_version,
    address,
    tags
from ranked
where rn = 1
