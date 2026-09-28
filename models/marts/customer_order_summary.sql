-- One row per customer: order_count and total_revenue.
--
-- The dedup CTEs are gone (they're their own models now), so this mart is just
-- the join + aggregate. min_orders comes from a project variable, overridable
-- per-run:  uv run dbt run --select customer_order_summary --vars 'min_orders: 5'
--
-- The Week 2 NULL trap, on purpose: orders with a NULL customer_id drop out of
-- this inner join — revenue is attributed only to known customers.
select
    c.customer_id,
    c.name,
    count(*) as order_count,
    sum(o.line_total) as total_revenue
from {{ ref('clean_orders') }} as o
join {{ ref('customers_deduped') }} as c
    on o.customer_id = c.customer_id
group by c.customer_id, c.name
having count(*) >= {{ var('min_orders') }}
