"""The same medallion pipeline on Spark + Delta Lake (local ``delta-spark`` or a Databricks cluster).

Semantics match the reference engine rule for rule; ``tests/spark/test_spark_parity.py`` runs both on
the same landing data and compares silver, quarantine, DQ results and gold.

Three entry points, one per layer, so a Databricks job can run them as separate tasks:

* :func:`ingest_bronze`  landing files → ``bronze.<entity>`` (append) + ingest manifest
* :func:`process_silver` DQ gates → ``silver.<entity>`` via Delta ``MERGE`` (SCD2 / upsert) + audit tables
* :func:`build_gold`     marts with the PII policy applied, then a leak scan before anything is written
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .. import bronze as py_bronze
from ..config import Entity, Settings
from ..gold import DIM_CUSTOMER_COLUMNS, PAYMENT_STATUSES, REVENUE_COLUMNS
from ..masking import raw_pii_values, scan_for_leaks
from ..pipeline import DQ_COLUMNS, DQ_RESULTS, QUARANTINE, QUARANTINE_COLUMNS, PiiLeakError
from ..silver import SCD2_COLUMNS, silver_columns

log = logging.getLogger("lakehouse.spark")

SPARK_TYPES = {"string": "string", "date": "date", "timestamp": "timestamp", "decimal": "decimal(18,2)"}


def get_spark(app: str = "medallion-lakehouse") -> SparkSession:
    """The active session on Databricks; a local Delta-enabled session everywhere else."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    from delta import configure_spark_with_delta_pip

    builder = (SparkSession.builder.appName(app).master("local[2]")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class Tables:
    """Delta tables addressed by path (local, CI) or by Unity Catalog name (``catalog.layer.table``)."""

    def __init__(self, spark: SparkSession, root: str | None = None, catalog: str | None = None):
        if not (root or catalog):
            raise ValueError("need a root path or a catalog")
        self.spark, self.root, self.catalog = spark, root, catalog

    def ident(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.`{name}`" if self.catalog else f"{self.root}/{layer}/{name}"

    def exists(self, layer: str, name: str) -> bool:
        if self.catalog:
            return self.spark.catalog.tableExists(f"{self.catalog}.{layer}.{name}")
        return DeltaTable.isDeltaTable(self.spark, self.ident(layer, name))

    def read(self, layer: str, name: str) -> DataFrame:
        ident = self.ident(layer, name)
        return self.spark.table(ident) if self.catalog else self.spark.read.format("delta").load(ident)

    def delta(self, layer: str, name: str) -> DeltaTable:
        ident = self.ident(layer, name)
        return DeltaTable.forName(self.spark, ident) if self.catalog else DeltaTable.forPath(self.spark, ident)

    def write(self, df: DataFrame, layer: str, name: str, mode: str, **options: str) -> None:
        w = df.write.format("delta").mode(mode)
        for k, v in options.items():
            w = w.option(k, v)
        ident = self.ident(layer, name)
        w.saveAsTable(ident) if self.catalog else w.save(ident)


# --------------------------------------------------------------------------------------------- bronze
def _bronze_schema(entity: Entity) -> T.StructType:
    fields = [T.StructField(c, T.StringType()) for c in entity.columns]
    fields += [T.StructField("_rescued_data", T.StringType()), T.StructField("_corrupt_record", T.StringType()),
               T.StructField("_source_file", T.StringType()), T.StructField("_line_no", T.LongType()),
               T.StructField("_batch_id", T.StringType()), T.StructField("_ingested_at", T.StringType())]
    return T.StructType(fields)


_MANIFEST_SCHEMA = T.StructType([
    T.StructField("source_file", T.StringType()), T.StructField("sha256", T.StringType()),
    T.StructField("entity", T.StringType()), T.StructField("batch_id", T.StringType()),
    T.StructField("rows", T.LongType()), T.StructField("missing_columns", T.StringType()),
    T.StructField("extra_columns", T.StringType()), T.StructField("corrupt_rows", T.LongType()),
    T.StructField("ingested_at", T.StringType()),
])


def ingest_bronze(cfg: Settings, t: Tables, batch_id: str) -> dict[str, dict]:
    """Parse landing files with the shared parser (works on local paths and /Volumes) and append to Delta."""
    known = set()
    if t.exists("bronze", py_bronze.MANIFEST):
        known = {(r.source_file, r.sha256) for r in
                 t.read("bronze", py_bronze.MANIFEST).select("source_file", "sha256").collect()}
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest, results = [], {}
    for name in cfg.order:
        entity = cfg.entities[name]
        rows, files, skipped = [], 0, 0
        for path in py_bronze.landing_files(cfg, name, batch_id):
            rel = str(path.relative_to(cfg.path("landing")))
            digest = py_bronze._sha256(path)
            if (rel, digest) in known:
                skipped += 1
                continue
            parsed, info = py_bronze.parse_file(path, entity)
            rows += [{**r, "_source_file": rel, "_batch_id": batch_id, "_ingested_at": now} for r in parsed]
            files += 1
            manifest.append({"source_file": rel, "sha256": digest, "entity": name, "batch_id": batch_id,
                             "rows": len(parsed), **info, "ingested_at": now})
        if rows:
            schema = _bronze_schema(entity)
            df = t.spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)
            t.write(df, "bronze", name, "append")
        results[name] = {"files": files, "files_skipped": skipped, "rows": len(rows)}
    if manifest:
        rows = [tuple(m[f.name] for f in _MANIFEST_SCHEMA.fields) for m in manifest]
        t.write(t.spark.createDataFrame(rows, _MANIFEST_SCHEMA), "bronze", py_bronze.MANIFEST, "append")
    return results


# --------------------------------------------------------------------------------------------- silver
def _cast(col: str, kind: str):
    c = F.col(f"`{col}`")
    if kind == "string":
        return F.when(F.trim(c) == "", F.lit(None).cast("string")).otherwise(F.trim(c))
    return F.expr(f"try_cast(`{col}` AS {SPARK_TYPES[kind]})")


def evaluate(cfg: Settings, t: Tables, entity: Entity, batch: DataFrame, batch_id: str):
    """Spark version of ``quality.evaluate``. Returns (valid, quarantine, results, checked, dups, rate, blocked)."""
    cols = entity.columns
    total_in = batch.count()
    dedupe = Window.partitionBy(*[F.col(f"`{c}`") for c in cols + ["_corrupt_record"]]) \
        .orderBy("_source_file", "_line_no")
    raw = batch.withColumn("__rn", F.row_number().over(dedupe)).where("__rn = 1").drop("__rn")

    manifest = t.read("bronze", py_bronze.MANIFEST).select(
        F.col("source_file").alias("_source_file"),
        F.coalesce(F.col("missing_columns"), F.lit("")).alias("__missing")).dropDuplicates(["_source_file"])
    df = raw.join(F.broadcast(manifest), "_source_file", "left") \
        .withColumn("__missing", F.coalesce("__missing", F.lit("")))
    drift = F.col("__missing") != ""
    corrupt = F.col("_corrupt_record").isNotNull()
    parseable = ~corrupt & ~drift

    for c, kind in entity.schema.items():
        df = df.withColumn(f"__t_{c}", _cast(c, kind))
    tc = {c: F.col(f"__t_{c}") for c in cols}

    reason_cols, counters = [], []    # counters: (rule, type, column, fail_expr, checked_expr)
    reason_cols.append(F.when(drift, F.concat(F.lit("schema_drift:"), F.col("__missing"))))
    counters.append(("schema_contract", "schema", "*", drift, F.lit(True)))
    reason_cols.append(F.when(corrupt, F.lit("malformed_record")))
    counters.append(("well_formed", "parse", "*", corrupt, F.lit(True)))
    cast_any = F.lit(False)
    for c, kind in entity.schema.items():
        if kind == "string":
            continue
        bad = F.col(f"`{c}`").isNotNull() & tc[c].isNull() & ~corrupt
        reason_cols.append(F.when(bad, F.lit(f"type_cast:{c}")))
        cast_any = cast_any | bad
    cast_count = sum(  # number of failed casts, not rows, to match the reference engine
        [F.when(F.col(f"`{c}`").isNotNull() & tc[c].isNull() & ~corrupt, 1).otherwise(0)
         for c, k in entity.schema.items() if k != "string"], F.lit(0))
    for rule in entity.rules:
        c = tc[rule.column]
        if rule.type == "not_null":
            fail = c.isNull()
        elif rule.type == "regex":
            fail = c.isNotNull() & ~c.rlike(rule.pattern)
        elif rule.type == "accepted_values":
            fail = c.isNotNull() & ~c.isin(list(rule.values))
        elif rule.type == "range":
            cond = F.lit(False)
            if rule.min is not None:
                cond = cond | (c < F.lit(rule.min))
            if rule.max is not None:
                cond = cond | (c > F.lit(rule.max))
            fail = c.isNotNull() & cond
        elif rule.type == "foreign_key":
            flag = f"__fk_{rule.name}"
            if t.exists("silver", rule.ref_entity):
                ref = t.read("silver", rule.ref_entity).select(F.col(rule.ref_column).alias(f"__key_{rule.name}")) \
                    .distinct().withColumn(flag, F.lit(True))
                df = df.join(ref, tc[rule.column] == F.col(f"__key_{rule.name}"), "left") \
                    .drop(f"__key_{rule.name}")
            else:
                df = df.withColumn(flag, F.lit(None).cast("boolean"))
            fail = c.isNotNull() & F.col(flag).isNull()
        else:
            raise ValueError(rule.type)
        fail = fail & parseable
        reason_cols.append(F.when(fail, F.lit(rule.name)))
        counters.append((rule.name, rule.type, rule.column, fail, parseable))

    df = df.withColumn("__reasons", F.concat_ws(";", *reason_cols)).cache()
    aggs = [F.count(F.lit(1)).alias("n"), F.sum(F.when(F.col("__reasons") != "", 1).otherwise(0)).alias("bad"),
            F.sum(cast_count).alias("cast_failed")]
    for i, (_, _, _, fail, checked) in enumerate(counters):
        aggs += [F.sum(F.when(fail, 1).otherwise(0)).alias(f"f{i}"),
                 F.sum(F.when(checked, 1).otherwise(0)).alias(f"c{i}")]
    row = df.agg(*aggs).collect()[0]
    n, bad = row["n"], row["bad"] or 0
    rate = bad / n if n else 0.0
    blocked = rate > entity.max_failure_rate
    status = "BLOCKED" if blocked else "PASSED"
    results = []
    for i, (rule, rtype, column, _, _) in enumerate(counters):
        results.append((batch_id, entity.name, rule, rtype, column, int(row[f"c{i}"] or 0), int(row[f"f{i}"] or 0),
                        status))
        if rule == "well_formed":   # keep the reference engine's order: schema, parse, cast, rules
            results.append((batch_id, entity.name, "types_castable", "cast", "*", n, int(row["cast_failed"] or 0),
                            status))
    results.append((batch_id, entity.name, "batch_gate", "gate", f"<= {entity.max_failure_rate:.0%}", n, bad, status))

    valid = df.where(F.col("__reasons") == "").select(
        *[tc[c].alias(c) for c in cols], "_source_file", "_line_no", "_batch_id", "_rescued_data")
    record_fields = sorted(cols + ["_rescued_data", "_corrupt_record"])
    quarantine = df.where(F.col("__reasons") != "").select(
        F.lit(batch_id).alias("batch_id"), F.lit(entity.name).alias("entity"),
        F.col("_source_file").alias("source_file"), F.col("_line_no").cast("long").alias("line_no"),
        F.col("__reasons").alias("reasons"),
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in record_fields])).alias("record"))
    return valid, quarantine, results, n, total_in - n, rate, blocked


def _replace_batch(t: Tables, table: str, df: DataFrame, batch_id: str, entity: str) -> None:
    if t.exists("silver", table):
        t.write(df, "silver", table, "overwrite",
                replaceWhere=f"batch_id = '{batch_id}' AND entity = '{entity}'")
    else:
        t.write(df, "silver", table, "append")


def _conform(cfg: Settings, entity: Entity, df: DataFrame) -> DataFrame:
    if entity.name != "transactions":
        return df
    fx = F.create_map(*[x for k, v in cfg.fx_to_inr.items() for x in (F.lit(k), F.lit(v))])
    return df.withColumn("amount_inr", F.round(F.col("amount").cast("double") * fx[F.col("currency")], 2)
                         .cast("decimal(18,2)"))


def _merge_upsert(t: Tables, entity: Entity, src: DataFrame) -> dict:
    key, ts = entity.primary_key, entity.order_by
    if not t.exists("silver", entity.name):
        t.write(src, "silver", entity.name, "overwrite")
        return {"inserted": src.count(), "updated": 0, "unchanged": 0, "stale": 0}
    tgt = t.read("silver", entity.name).select(key, F.col(ts).alias("__tgt_ts"))
    j = src.join(tgt, key, "left")
    m = j.agg(F.sum(F.when(F.col("__tgt_ts").isNull(), 1).otherwise(0)).alias("ins"),
              F.sum(F.when(F.col(ts) >= F.col("__tgt_ts"), 1).otherwise(0)).alias("upd"),
              F.sum(F.when(F.col(ts) < F.col("__tgt_ts"), 1).otherwise(0)).alias("stale")).collect()[0]
    (t.delta("silver", entity.name).alias("t")
     .merge(src.alias("s"), f"t.{key} = s.{key}")
     .whenMatchedUpdateAll(condition=f"s.{ts} >= t.{ts}")
     .whenNotMatchedInsertAll()
     .execute())
    return {"inserted": int(m["ins"] or 0), "updated": int(m["upd"] or 0), "unchanged": 0,
            "stale": int(m["stale"] or 0)}


def _merge_scd2(t: Tables, entity: Entity, src: DataFrame) -> dict:
    key, ts = entity.primary_key, entity.order_by
    src_cols = src.columns
    if not t.exists("silver", entity.name):
        out = (src.withColumn("valid_from", F.col(ts)).withColumn("valid_to", F.lit(None).cast("timestamp"))
               .withColumn("is_current", F.lit(True)))
        t.write(out, "silver", entity.name, "overwrite")
        return {"inserted": src.count(), "updated": 0, "unchanged": 0, "stale": 0}
    differs = " OR ".join(f"NOT (s.{c} <=> t.{c})" for c in entity.tracked_columns)
    cur = t.read("silver", entity.name).where("is_current")
    j = src.alias("s").join(cur.alias("t"), F.col(f"s.{key}") == F.col(f"t.{key}"), "left")
    matched = F.col(f"t.{key}").isNotNull()
    stale = matched & (F.col(f"s.{ts}") <= F.col("t.valid_from"))
    changed = matched & ~stale & F.expr(differs)
    m = j.agg(F.sum(F.when(~matched, 1).otherwise(0)).alias("ins"), F.sum(F.when(changed, 1).otherwise(0)).alias("upd"),
              F.sum(F.when(stale, 1).otherwise(0)).alias("stale"),
              F.sum(F.when(matched & ~stale & ~F.expr(differs), 1).otherwise(0)).alias("same")).collect()[0]
    # Classic SCD2 MERGE: changed rows appear twice, once to close the current version (matched on key)
    # and once with a NULL merge key so they are inserted as the new current version.
    new_versions = j.where(changed).select("s.*").withColumn("__merge_key", F.lit(None).cast("string"))
    staged = new_versions.unionByName(src.withColumn("__merge_key", F.col(key)))
    insert_values = {c: f"s.{c}" for c in src_cols}
    insert_values.update({"valid_from": f"s.{ts}", "valid_to": "CAST(NULL AS TIMESTAMP)", "is_current": "true"})
    (t.delta("silver", entity.name).alias("t")
     .merge(staged.alias("s"), f"t.{key} = s.__merge_key AND t.is_current = true")
     .whenMatchedUpdate(condition=f"s.{ts} > t.valid_from AND ({differs})",
                        set={"is_current": "false", "valid_to": f"s.{ts}"})
     .whenNotMatchedInsert(values=insert_values)
     .execute())
    return {"inserted": int(m["ins"] or 0), "updated": int(m["upd"] or 0), "unchanged": int(m["same"] or 0),
            "stale": int(m["stale"] or 0)}


def process_silver(cfg: Settings, t: Tables, batch_id: str) -> dict[str, dict]:
    out = {}
    for name in cfg.order:
        entity = cfg.entities[name]
        if not t.exists("bronze", name):
            continue
        batch = t.read("bronze", name).where(F.col("_batch_id") == batch_id)
        valid, quarantine, results, checked, dups, rate, blocked = evaluate(cfg, t, entity, batch, batch_id)
        q_count = quarantine.count()
        _replace_batch(t, QUARANTINE, quarantine.select(*QUARANTINE_COLUMNS), batch_id, name)
        res_schema = T.StructType([T.StructField(c, T.LongType() if c in ("checked", "failed") else T.StringType())
                                   for c in DQ_COLUMNS])
        _replace_batch(t, DQ_RESULTS, t.spark.createDataFrame(results, res_schema).select(*DQ_COLUMNS), batch_id, name)
        s = {"checked": checked, "duplicates_dropped": dups, "quarantined": q_count, "failure_rate": round(rate, 4),
             "blocked": blocked}
        if blocked:
            log.warning("%s %s: quality gate BLOCKED the batch (%.1f%% failed)", batch_id, name, 100 * rate)
            s.update(inserted=0, updated=0, unchanged=0, stale=0)
        else:
            w = Window.partitionBy(entity.primary_key).orderBy(F.col(entity.order_by).desc(), F.col("_line_no").desc())
            latest = valid.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")
            latest = _conform(cfg, entity, latest)
            cols = [c for c in silver_columns(cfg, entity) if c not in SCD2_COLUMNS]
            src = latest.select(*cols)
            merge = _merge_scd2 if entity.write_mode == "scd2" else _merge_upsert
            s.update(merge(t, entity, src))
        out[name] = s
        log.info("%s %-12s %s", batch_id, name, s)
    return out


# ----------------------------------------------------------------------------------------------- gold
def _gold_tables(cfg: Settings, t: Tables) -> dict[str, DataFrame]:
    tx = t.read("silver", "transactions")
    merchants = t.read("silver", "merchants").select("merchant_id", "category")
    customers = t.read("silver", "customers")
    paid = F.col("status").isin(list(PAYMENT_STATUSES))
    refunded = F.col("status") == "refunded"
    amt = F.col("amount_inr").cast("double")

    rev = (tx.join(merchants, "merchant_id", "left")
           .withColumn("txn_date", F.date_format("txn_ts", "yyyy-MM-dd"))
           .withColumn("category", F.coalesce("category", F.lit("unknown")))
           .groupBy("txn_date", "category")
           .agg(F.sum(F.when(paid, 1).otherwise(0)).cast("long").alias("payments"),
                F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).cast("long").alias("failed"),
                F.sum(F.when(refunded, 1).otherwise(0)).cast("long").alias("refunds"),
                F.round(F.sum(F.when(paid, amt).otherwise(0.0)), 2).alias("gross_inr"),
                F.round(F.sum(F.when(refunded, amt).otherwise(0.0)), 2).alias("refunded_inr"),
                F.countDistinct(F.when(paid, F.col("customer_id"))).cast("long").alias("paying_customers"))
           .withColumn("net_inr", F.round(F.col("gross_inr") - F.col("refunded_inr"), 2))
           .select(*REVENUE_COLUMNS))

    stats = (tx.where(paid).withColumn("txn_date", F.date_format("txn_ts", "yyyy-MM-dd"))
             .groupBy("customer_id")
             .agg(F.count("txn_id").cast("long").alias("payments"),
                  F.round(F.sum(F.when(F.col("status") == "captured", amt).otherwise(0.0)), 2).alias("net_spend_inr"),
                  F.min("txn_date").alias("first_txn_date"), F.max("txn_date").alias("last_txn_date")))
    versions = customers.groupBy("customer_id").agg(F.count(F.lit(1)).cast("long").alias("versions"))
    salt = cfg.pii.salt()
    masks = {
        "redact": lambda c: F.when(F.col(c).isNotNull(), F.lit("***")),
        "hash": lambda c: F.sha2(F.concat(F.lit(salt), F.lower(F.trim(F.col(c)))), 256),
        "mask_last4": lambda c: F.concat(F.expr(f"repeat('*', greatest(length({c}) - 4, 0))"),
                                         F.expr(f"right({c}, 4)")),
    }
    dim = customers.where("is_current").join(stats, "customer_id", "left").join(versions, "customer_id", "left")
    dim = (dim.withColumn("payments", F.coalesce("payments", F.lit(0)).cast("long"))
           .withColumn("net_spend_inr", F.coalesce("net_spend_inr", F.lit(0.0)))
           .withColumn("signup_date", F.date_format("signup_date", "yyyy-MM-dd"))
           .withColumnRenamed("valid_from", "current_since"))
    for col, method in cfg.pii.for_entity("customers").items():
        if method == "year_only":
            dim = dim.withColumn(f"{col}_year", F.year(col).cast("long")).drop(col)
        else:
            dim = dim.withColumn(col, masks[method](col))
    dim = dim.select(*DIM_CUSTOMER_COLUMNS)

    dq = t.read("silver", DQ_RESULTS)
    score = dq.withColumn("pass_rate", F.round(F.when(F.col("checked") > 0, 1 - F.col("failed") / F.col("checked"))
                                               .otherwise(F.lit(1.0)), 4)) \
        .select("batch_id", "entity", "rule", "rule_type", "column", "checked", "failed", "pass_rate", "gate_status")
    return {"daily_merchant_revenue": rev, "dim_customer": dim, "dq_scorecard": score}


def build_gold(cfg: Settings, t: Tables) -> dict[str, int]:
    tables = _gold_tables(cfg, t)
    # Leak scan before publishing. Gold is small (aggregates and one row per customer), so it is
    # scanned on the driver with the same function the reference engine uses.
    customers = t.read("silver", "customers").select(*cfg.pii.for_entity("customers")).toPandas()
    customers["date_of_birth"] = customers["date_of_birth"].astype(str)
    findings = scan_for_leaks({k: v.toPandas() for k, v in tables.items()}, raw_pii_values(customers, cfg.pii))
    if findings:
        raise PiiLeakError(f"raw PII found in gold, nothing was published: {findings}")
    counts = {}
    for name, df in tables.items():
        t.write(df, "gold", name, "overwrite", overwriteSchema="true")
        counts[name] = t.read("gold", name).count()
    return counts


def run_batch(cfg: Settings, t: Tables, batch_id: str) -> dict:
    bronze = ingest_bronze(cfg, t, batch_id)
    silver = process_silver(cfg, t, batch_id)
    gold = build_gold(cfg, t)
    return {"batch_id": batch_id, "bronze": bronze, "silver": silver, "gold": gold}
