-- Typed, normalized orders, built on the deduped grain.
--
-- Reads from the model built upstream (ref, not the raw table) and does the
-- Week 2 cleaning: money text -> numeric, three date formats -> DATE, status
-- normalized to a small set, unusable rows dropped, line_total added.
with typed as (
    select
        order_id,
        customer_id,
        sku,

        -- quantity is blank on a sprinkling of rows -> becomes NULL, dropped below
        try_cast(quantity as integer) as quantity,

        -- price arrives as text: "$1,209.50", "  44.00 ", "" -> strip $ , spaces, cast
        try_cast(
            replace(replace(trim(price), '$', ''), ',', '') as double
        ) as price,

        -- status: mixed casing + stray spaces + some blank -> lowercase, blank -> 'unknown'
        coalesce(nullif(lower(trim(status)), ''), 'unknown') as status,

        -- order_date is one of three formats, chosen at random per row
        coalesce(
            try_strptime(order_date, '%d-%b-%Y'),
            try_strptime(order_date, '%Y-%m-%d'),
            try_strptime(order_date, '%m/%d/%Y')
        )::date as order_date,

        updated_at
    from {{ ref('orders_deduped') }}
)

select
    *,
    quantity * price as line_total
from typed
-- drop rows without a usable quantity or price — can't measure revenue on them
where quantity is not null
  and price is not null
