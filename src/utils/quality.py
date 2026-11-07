from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def boolean_flag_counts(df: DataFrame) -> dict[str, int]:
    flags = [f.name for f in df.schema.fields if isinstance(f.dataType, T.BooleanType)]
    if not flags:
        return {}
    row = df.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in flags]).first().asDict()
    return {c: int(v or 0) for c, v in row.items()}


def duplicate_keys(df: DataFrame, keys: list[str]) -> int:
    return df.groupBy(*keys).count().filter("count > 1").count()


def null_keys(df: DataFrame, keys: list[str]) -> int:
    cond = F.lit(False)
    for k in keys:
        cond = cond | F.col(k).isNull()
    return df.filter(cond).count()


def orphans(child: DataFrame, col: str, parent: DataFrame, parent_col: str) -> int:
    keys = parent.select(F.col(parent_col).alias(col)).distinct()
    return child.filter(F.col(col).isNotNull()).join(keys, col, "left_anti").count()


def column_sum(df: DataFrame, col: str):
    return df.agg(F.sum(col)).first()[0]
