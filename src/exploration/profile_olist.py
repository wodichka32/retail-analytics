from __future__ import annotations

import pandas as pd

from src.utils.config import (
    DOCS_DIR,
    OLIST_DATETIME_COLUMNS,
    OLIST_STRING_COLUMNS,
    OLIST_TABLES,
    RAW_DIR,
)

SOURCE_DIR = RAW_DIR / "olist"
REPORT_PATH = DOCS_DIR / "data_dictionary" / "olist_profile.md"


def load_tables() -> dict[str, pd.DataFrame]:
    tables = {}
    for name, meta in OLIST_TABLES.items():
        path = SOURCE_DIR / meta["file"]
        header = pd.read_csv(path, nrows=0).columns
        dtype = {c: "string" for c in header if c in OLIST_STRING_COLUMNS}
        dates = [c for c in header if c in OLIST_DATETIME_COLUMNS]
        tables[name] = pd.read_csv(path, dtype=dtype, parse_dates=dates)
    return tables


def n(x, decimals: int = 0) -> str:
    return f"{x:,.{decimals}f}".replace(",", " ").replace(".", ",")


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        cells = [str(v).replace("|", "\\|").replace("\n", " ") for v in row]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def column_profile(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        s = df[col]
        non_null = s.dropna()
        sample = non_null.iloc[0] if len(non_null) else ""
        if isinstance(sample, str) and len(sample) > 40:
            sample = sample[:37] + "..."
        rows.append({
            "колонка": col,
            "тип": str(s.dtype),
            "пропуски": int(s.isna().sum()),
            "пропуски, %": n(s.isna().mean() * 100, 2),
            "уникальных": int(s.nunique()),
            "пример": sample,
        })
    return pd.DataFrame(rows)


def pk_check(df: pd.DataFrame, pk: list[str]) -> str:
    if not pk:
        return "первичный ключ не задан"
    dup = int(df.duplicated(subset=pk).sum())
    nulls = int(df[pk].isna().any(axis=1).sum())
    status = "OK" if dup == 0 and nulls == 0 else "НАРУШЕН"
    return f"`{', '.join(pk)}` -> {status} (дубликатов ключа: {dup}, пустых ключей: {nulls})"


def fk_checks(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for child, meta in OLIST_TABLES.items():
        for col, (parent, parent_col) in meta["fk"].items():
            c = tables[child][col].dropna()
            p = tables[parent][parent_col]
            orphans = ~c.isin(p)
            children_per_parent = c.value_counts()
            parents_without_child = int((~p.isin(c)).sum())
            rows.append({
                "дочерняя колонка": f"{child}.{col}",
                "родительская колонка": f"{parent}.{parent_col}",
                "пустых ссылок": int(tables[child][col].isna().sum()),
                "висячих строк": int(orphans.sum()),
                "висячих ключей": int(c[orphans].nunique()),
                "родителей без потомков": parents_without_child,
                "макс. потомков на родителя": int(children_per_parent.max()),
                "в среднем потомков": n(children_per_parent.mean(), 3),
            })
    return pd.DataFrame(rows)


def date_ranges(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, df in tables.items():
        for col in df.select_dtypes(include="datetime").columns:
            rows.append({
                "таблица.колонка": f"{name}.{col}",
                "минимум": df[col].min(),
                "максимум": df[col].max(),
                "пропуски": int(df[col].isna().sum()),
            })
    return pd.DataFrame(rows)


def numeric_summary(df: pd.DataFrame) -> pd.DataFrame | None:
    num = df.select_dtypes(include="number")
    if num.empty:
        return None
    desc = num.describe(percentiles=[0.01, 0.5, 0.99]).T
    out = pd.DataFrame({
        "колонка": desc.index,
        "значений": desc["count"].map(lambda v: n(v)).values,
        "среднее": desc["mean"].map(lambda v: n(v, 2)).values,
        "ст. отклонение": desc["std"].map(lambda v: n(v, 2)).values,
        "минимум": desc["min"].map(lambda v: n(v, 2)).values,
        "1%": desc["1%"].map(lambda v: n(v, 2)).values,
        "медиана": desc["50%"].map(lambda v: n(v, 2)).values,
        "99%": desc["99%"].map(lambda v: n(v, 2)).values,
        "максимум": desc["max"].map(lambda v: n(v, 2)).values,
        "нулей": (num == 0).sum().values,
        "отрицательных": (num < 0).sum().values,
    })
    return out


def business_checks(t: dict[str, pd.DataFrame]) -> list[str]:
    out = []
    orders, items, pays, revs = t["orders"], t["order_items"], t["payments"], t["reviews"]
    customers, products = t["customers"], t["products"]

    status = orders["order_status"].value_counts()
    out.append("**Статусы заказов**\n\n" + md_table(
        status.rename_axis("статус").reset_index(name="заказов")))

    per_unique = customers.groupby("customer_unique_id")["customer_id"].nunique()
    out.append(
        "**Идентификация покупателя.** "
        f"`customer_id`: {n(customers['customer_id'].nunique())} значений; "
        f"`customer_unique_id`: {n(customers['customer_unique_id'].nunique())} значений; "
        f"людей с несколькими `customer_id`: {n((per_unique > 1).sum())}. "
        "`customer_id` создаётся для каждого заказа, поэтому повторные покупки нужно считать по `customer_unique_id`."
    )

    no_items = ~orders["order_id"].isin(items["order_id"])
    no_pay = ~orders["order_id"].isin(pays["order_id"])
    no_rev = ~orders["order_id"].isin(revs["order_id"])
    out.append(
        "**Полнота заказов.** "
        f"заказов без товаров: {n(no_items.sum())} "
        f"(статусы: {orders.loc[no_items, 'order_status'].value_counts().to_dict()}); "
        f"заказов без платежей: {n(no_pay.sum())}; "
        f"заказов без отзывов: {n(no_rev.sum())}."
    )

    item_total = items.groupby("order_id")[["price", "freight_value"]].sum().sum(axis=1)
    pay_total = pays.groupby("order_id")["payment_value"].sum()
    both = pd.concat([item_total.rename("items"), pay_total.rename("payments")], axis=1).dropna()
    diff = (both["items"] - both["payments"]).abs()
    out.append(
        "**Товары и платежи.** Для заказов, которые есть в обеих таблицах, "
        "`sum(price + freight_value)` и `sum(payment_value)` "
        f"совпадают с точностью 0,01 у {n((diff <= 0.01).sum())} из {n(len(both))} заказов; "
        f"расходятся больше чем на 1,00 у {n((diff > 1).sum())}. "
        f"Способы оплаты: {pays['payment_type'].value_counts().to_dict()}."
    )

    rev_per_order = revs.groupby("order_id").size()
    orders_per_review = revs.groupby("review_id")["order_id"].nunique()
    full_dup_reviews = int(revs.duplicated().sum())
    out.append(
        "**Отзывы.** "
        f"заказов с несколькими отзывами: {n((rev_per_order > 1).sum())}; "
        f"review_id, связанных с несколькими заказами: {n((orders_per_review > 1).sum())}; "
        f"полностью одинаковых строк: {n(full_dup_reviews)}. "
        "Значит, гранулярность `reviews` — (review_id, order_id), а не review_id."
    )

    d = orders
    bad = {
        "подтверждение раньше покупки": (d["order_approved_at"] < d["order_purchase_timestamp"]).sum(),
        "передача перевозчику раньше покупки": (d["order_delivered_carrier_date"] < d["order_purchase_timestamp"]).sum(),
        "доставка раньше передачи перевозчику": (d["order_delivered_customer_date"] < d["order_delivered_carrier_date"]).sum(),
        "доставка раньше покупки": (d["order_delivered_customer_date"] < d["order_purchase_timestamp"]).sum(),
        "статус delivered, но нет даты доставки": (
            (d["order_status"] == "delivered") & d["order_delivered_customer_date"].isna()).sum(),
        "есть дата доставки, но статус не delivered": (
            (d["order_status"] != "delivered") & d["order_delivered_customer_date"].notna()).sum(),
    }
    out.append("**Согласованность дат (orders)**\n\n" + md_table(
        pd.DataFrame({"проверка": bad.keys(), "строк": [int(v) for v in bad.values()]})))

    delivered = d.dropna(subset=["order_delivered_customer_date"])
    days = (delivered["order_delivered_customer_date"] - delivered["order_purchase_timestamp"]).dt.days
    late = delivered["order_delivered_customer_date"].dt.normalize() > delivered["order_estimated_delivery_date"]
    out.append(
        "**Доставка.** "
        f"доставленных заказов: {n(len(delivered))}; срок доставки: медиана {n(days.median())} дн., "
        f"среднее {n(days.mean(), 1)} дн., 99-й перцентиль {n(days.quantile(0.99))} дн.; "
        f"опоздали относительно обещанной даты: {n(late.mean() * 100, 1)}%."
    )

    cats = set(t["category_translation"]["product_category_name"])
    missing_cat = products["product_category_name"].dropna()
    missing_cat = sorted(set(missing_cat[~missing_cat.isin(cats)]))
    out.append(
        "**Категории.** "
        f"товаров без категории: {n(products['product_category_name'].isna().sum())}; "
        f"категории без перевода: {missing_cat}."
    )

    geo = t["geolocation"]
    geo_zip = set(geo["geolocation_zip_code_prefix"])
    cust_cov = customers["customer_zip_code_prefix"].isin(geo_zip).mean() * 100
    sell_cov = t["sellers"]["seller_zip_code_prefix"].isin(geo_zip).mean() * 100
    out_of_brazil = ~geo["geolocation_lat"].between(-34, 6) | ~geo["geolocation_lng"].between(-74, -34)
    out.append(
        "**Геолокация.** "
        f"строк: {n(len(geo))}; уникальных индексов: {n(len(geo_zip))} "
        f"(~{n(len(geo) / len(geo_zip))} строк на индекс); "
        f"полностью одинаковых строк: {n(int(geo.duplicated().sum()))}; "
        f"точек за пределами Бразилии: {n(int(out_of_brazil.sum()))}; "
        f"покрытие индексов покупателей: {n(cust_cov, 2)}%; продавцов: {n(sell_cov, 2)}%. "
        "Перед соединением таблицу нужно свести к одной строке на индекс."
    )

    pm = orders.set_index("order_purchase_timestamp").resample("MS").size()
    out.append("**Заказы по месяцам** (крайние месяцы неполные)\n\n" + md_table(
        pm.rename_axis("месяц").reset_index(name="заказов").assign(
            месяц=lambda x: x["месяц"].dt.strftime("%Y-%m"))))
    return out


def build_report(tables: dict[str, pd.DataFrame]) -> str:
    parts = [
        "# Olist — профиль исходных данных",
        "",
        "_Создаётся командой `python -m src.exploration.profile_olist`. Не редактировать вручную._",
        "",
        "## Обзор",
        "",
        md_table(pd.DataFrame([{
            "таблица": name,
            "файл": OLIST_TABLES[name]["file"],
            "строк": n(len(df)),
            "колонок": df.shape[1],
            "полных дубликатов": int(df.duplicated().sum()),
            "первичный ключ": pk_check(df, OLIST_TABLES[name]["pk"]),
        } for name, df in tables.items()])),
        "",
        "## Внешние ключи и кардинальность",
        "",
        md_table(fk_checks(tables)),
        "",
        "## Диапазоны дат",
        "",
        md_table(date_ranges(tables)),
        "",
        "## Бизнес-проверки",
        "",
    ]
    for block in business_checks(tables):
        parts += [block, ""]

    parts += ["## Таблицы", ""]
    for name, df in tables.items():
        parts += [f"### {name}", "", md_table(column_profile(df)), ""]
        num = numeric_summary(df)
        if num is not None:
            parts += ["Распределение числовых колонок:", "", md_table(num), ""]
    return "\n".join(parts)


def main() -> None:
    tables = load_tables()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(build_report(tables), encoding="utf-8")
    print(f"отчёт записан в {REPORT_PATH}")


if __name__ == "__main__":
    main()
