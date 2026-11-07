import json
from datetime import datetime, timezone

from src.analytics.db import q
from src.utils.config import PROJECT_ROOT

OUT_PATH = PROJECT_ROOT / "dashboard" / "data.js"
DELIVERED = "is_valid_order and delivery_days is not null"

SEGMENT_NAMES = {
    "champions": "Чемпионы",
    "loyal": "Лояльные",
    "at_risk": "Под угрозой ухода",
    "new_high_value": "Новые крупные",
    "new": "Новые",
    "needs_attention": "Требуют внимания",
    "hibernating_high_value": "Спящие крупные",
    "hibernating": "Спящие",
}

QUERIES = {
    "kpi": f"""
        with v as (select * from fact_orders where is_valid_order),
        c as (select * from gold_customer_metrics),
        yoy as (
            select sum(case when d.year = 2018 then f.items_value end)
                   / sum(case when d.year = 2017 then f.items_value end) - 1 as g
            from fact_orders f join dim_date d on d.date_key = f.purchase_date_key
            where f.is_valid_order and d.month between 1 and 8
        ),
        clv as (
            with o as (select customer_key, purchase_ts, items_value from v),
            f as (select customer_key, min(purchase_ts) as first_ts from o group by 1),
            e as (select max(purchase_ts) as end_ts from o)
            select sum(o.items_value) / count(distinct f.customer_key) as clv
            from f cross join e
            join o on o.customer_key = f.customer_key and o.purchase_ts < f.first_ts + interval 365 day
            where f.first_ts + interval 365 day <= e.end_ts
        ),
        dl as (
            select avg(delivery_days) as delivery_days,
                   median(delivery_days) as median_delivery_days,
                   avg(is_late::int) as late_rate,
                   avg(case when is_late then review_score end) as late_score,
                   avg(case when not is_late then review_score end) as ontime_score
            from fact_orders where {DELIVERED}
        )
        select (select sum(items_value) from v) as revenue,
               (select sum(freight_value) from v) as freight,
               (select count(*) from v) as orders,
               (select sum(items_value) / count(*) from v) as aov,
               (select median(items_value) from v where has_items) as median_order,
               (select count(*) from c) as customers,
               (select avg(is_repeat_customer::int) from c) as rpr,
               (select sum(case when is_repeat_customer then revenue end) / sum(revenue) from c) as repeat_revenue_share,
               (select clv from clv) as clv_365,
               (select g from yoy) as yoy_growth,
               (select avg(review_score) from v) as avg_score,
               dl.*
        from dl
    """,
    "monthly": """
        select d.year_month || '-01' as month,
               sum(f.items_value) as revenue,
               count(*) as orders,
               sum(f.items_value) / count(*) as aov,
               avg(f.delivery_days) as delivery_days,
               avg(f.is_late::int) filter (where f.delivery_days is not null) as late_rate,
               avg(f.review_score) as avg_score
        from fact_orders f join dim_date d on d.date_key = f.purchase_date_key
        where f.is_valid_order and d.is_in_analysis_window
        group by 1 order by 1
    """,
    "categories": """
        with total as (
            select p.category_en as category, sum(i.price) as revenue, count(distinct i.order_id) as orders
            from fact_order_items i join dim_product p using (product_key)
            where i.is_valid_order group by 1
        ),
        yr as (
            select p.category_en as category, d.year, sum(i.price) as revenue
            from fact_order_items i join dim_product p using (product_key)
            join dim_date d on d.date_key = i.purchase_date_key
            where i.is_valid_order and d.month between 1 and 8 and d.year in (2017, 2018)
            group by 1, 2
        ),
        sh as (
            select category,
                   sum(case when year = 2017 then revenue end) / (select sum(revenue) from yr where year = 2017) as share_2017,
                   sum(case when year = 2018 then revenue end) / (select sum(revenue) from yr where year = 2018) as share_2018
            from yr group by 1
        )
        select t.category, t.revenue, t.orders, t.revenue / t.orders as avg_check,
               t.revenue / (select sum(revenue) from total) as share,
               coalesce(sh.share_2017, 0) as share_2017, coalesce(sh.share_2018, 0) as share_2018
        from total t left join sh using (category)
        order by t.revenue desc
        limit 15
    """,
    "week_hour": """
        select d.day_of_week as dow, hour(f.purchase_ts) as hour, count(*) as orders
        from fact_orders f join dim_date d on d.date_key = f.purchase_date_key
        where f.is_valid_order and d.is_in_analysis_window
        group by 1, 2 order by 1, 2
    """,
    "states": f"""
        with s as (
            select delivery_state as state, delivery_region as region, count(*) as orders,
                   sum(items_value) as revenue, sum(items_value) / count(*) as aov, avg(freight_value) as avg_freight
            from fact_orders where is_valid_order and has_items group by 1, 2
        ),
        l as (
            select delivery_state as state, avg(delivery_days) as delivery_days,
                   avg(is_late::int) as late_rate, avg(review_score) as avg_score
            from fact_orders where {DELIVERED} group by 1
        )
        select s.*, s.revenue / (select sum(revenue) from s) as share, l.delivery_days, l.late_rate, l.avg_score
        from s join l using (state)
        order by revenue desc
    """,
    "segments": """
        select segment, count(*) as customers, sum(monetary) as revenue, avg(monetary) as avg_revenue,
               count(*) / sum(count(*)) over () as customer_share,
               sum(monetary) / sum(sum(monetary)) over () as revenue_share
        from gold_rfm_segments group by 1 order by revenue desc
    """,
    "cohorts": """
        with p as (
            select customer_key, date_trunc('month', purchase_ts) as month
            from fact_orders where is_valid_order group by 1, 2
        ),
        f as (select customer_key, min(month) as cohort from p group by 1),
        a as (
            select f.cohort, datediff('month', f.cohort, p.month) as month_number,
                   count(distinct p.customer_key) as customers
            from p join f using (customer_key) group by 1, 2
        )
        select strftime(a.cohort, '%Y-%m') as cohort, s.customers as cohort_size, a.month_number, a.customers,
               a.customers / s.customers as retention
        from a join a s on s.cohort = a.cohort and s.month_number = 0
        where a.cohort between date '2017-01-01' and date '2018-07-01'
          and a.month_number between 1 and 12
          and a.cohort + to_months(a.month_number) <= date '2018-08-01'
        order by 1, 3
    """,
    "estimate": f"""
        select least(greatest(delivery_vs_estimate_days, -20), 30) as days_before_estimate,
               count(*) as orders, avg(review_score) as avg_score
        from fact_orders where {DELIVERED} and review_score is not null
        group by 1 order by 1
    """,
}


def build() -> dict:
    data = {}
    for name, sql in QUERIES.items():
        df = q(sql)
        if name == "segments":
            df.insert(1, "name", df.segment.map(SEGMENT_NAMES))
        data[name] = json.loads(df.to_json(orient="records", date_format="iso", double_precision=6))
    return data


def main() -> None:
    data = build()
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload = json.dumps({"generated_at": generated, **data}, ensure_ascii=False, separators=(",", ":"))
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(f"window.DASH_DATA = {payload};\n", encoding="utf-8")
    print(f"{OUT_PATH} ({OUT_PATH.stat().st_size / 1024:.1f} KB): " + ", ".join(f"{k}={len(v)}" for k, v in data.items()))


if __name__ == "__main__":
    main()
