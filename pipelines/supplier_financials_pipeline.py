"""
Supplier financials ingestion — Lakeflow Declarative Pipeline (DLT) with Auto Loader
====================================================================================

Flow
  S3 landing (Textract JSON + supplier CSV templates)
    -> bronze_textract_extracts / bronze_template_uploads   (Auto Loader, streaming)
    -> silver_line_items                                      (normalise + map to chart of accounts)
    -> silver_unmapped_lines                                  (quarantine for analyst review)
    -> silver.financial_statements                            (one row per ABN + financial year,
                                                               feeds ratio_engine.sql)

Assumptions (adjust to your environment)
  * Supplier PDFs are uploaded to S3 and processed by Textract (Lambda / Step Functions).
    The extractor writes one JSON file per submission to LANDING_TEXTRACT, shaped like:
      {
        "submission_id": "SUB-123", "abn": "11 111 111 111", "company_name": "Acme Civil Pty Ltd",
        "fy_end": "2024-06-30", "source": "audited", "units": "thousands",
        "lines": [ {"statement": "balance_sheet", "label": "Total current assets", "amount": "9,800"}, ... ]
      }
  * Suppliers can alternatively upload CSVs using the CreditSource template:
      submission_id,abn,company_name,fy_end,source,units,statement,label,amount
  * ref.coa_mapping is a reference Delta table maintained by analysts:
      label_pattern STRING (regex, lower-case), canonical_field STRING, sign INT, priority INT
  * Pipeline settings: target catalog = creditsource, default schema = silver;
    configuration keys below can be overridden in the pipeline's Configuration panel.

Uses the classic `import dlt` API, which remains supported. On newer runtimes the
equivalent module is `from pyspark import pipelines as dp` (dp.table, dp.expect_or_drop, ...).
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LANDING_TEXTRACT = spark.conf.get(
    "creditsource.landing.textract", "s3://creditsource-landing/supplier-financials/textract/"
)
LANDING_TEMPLATES = spark.conf.get(
    "creditsource.landing.templates", "s3://creditsource-landing/supplier-financials/templates/"
)
COA_MAPPING_TABLE = spark.conf.get("creditsource.ref.coa_mapping", "creditsource.ref.coa_mapping")

# Canonical fields expected by silver.financial_statements / ratio_engine.sql
CANONICAL_FIELDS = [
    "revenue", "gross_profit", "ebitda_normalised", "depreciation_amortisation",
    "interest_expense", "npbt", "npat",
    "cash", "trade_receivables", "inventory", "total_current_assets",
    "total_non_current_assets", "total_assets", "trade_payables",
    "total_current_liabilities", "total_non_current_liabilities", "total_liabilities",
    "interest_bearing_debt", "related_party_loans", "share_capital", "total_equity",
    "operating_cash_flow",
]

UNIT_MULTIPLIER = (
    F.when(F.lower("units").isin("thousands", "000", "$000", "k"), F.lit(1_000))
     .when(F.lower("units").isin("millions", "m", "$m"), F.lit(1_000_000))
     .otherwise(F.lit(1))
)


def clean_amount(col):
    """'(1,234)' -> -1234, '1,234' -> 1234, '-' -> 0, blank -> null."""
    s = F.trim(col.cast("string"))
    negative = s.startswith("(") & s.endswith(")") | s.startswith("-") & (F.length(s) > 1)
    digits = F.regexp_replace(s, r"[^0-9.]", "")
    value = F.when(s == "-", F.lit(0)).when(digits == "", F.lit(None)).otherwise(digits.cast("decimal(18,2)"))
    return F.when(negative, -value).otherwise(value)


# ---------------------------------------------------------------------------
# BRONZE — raw ingestion with Auto Loader
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_textract_extracts",
    comment="Raw Textract JSON extracts of supplier financial statements (Auto Loader).",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_textract_extracts():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")      # unexpected fields -> _rescued_data
        .option("cloudFiles.useNotifications", "true")           # S3 event notifications, scales better than listing
        .option("multiLine", "true")
        .load(LANDING_TEXTRACT)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("textract").alias("_channel"),
        )
    )


@dlt.table(
    name="bronze_template_uploads",
    comment="Raw supplier CSV template uploads (Auto Loader).",
    table_properties={"quality": "bronze"},
)
def bronze_template_uploads():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints",
                "submission_id STRING, abn STRING, fy_end STRING, amount STRING")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(LANDING_TEMPLATES)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("template").alias("_channel"),
        )
    )


# ---------------------------------------------------------------------------
# SILVER — line items: flatten, clean, validate, map to chart of accounts
# ---------------------------------------------------------------------------
@dlt.view(name="v_raw_lines")
def v_raw_lines():
    textract = (
        dlt.read_stream("bronze_textract_extracts")
        .select(
            "submission_id", "abn", "company_name", "fy_end", "source", "units",
            "_source_file", "_ingested_at", "_channel",
            F.explode_outer("lines").alias("line"),
        )
        .select(
            "*", F.col("line.statement").alias("statement"),
            F.col("line.label").alias("label"), F.col("line.amount").cast("string").alias("amount"),
        )
        .drop("line")
    )
    template = dlt.read_stream("bronze_template_uploads").select(
        "submission_id", "abn", "company_name", "fy_end", "source", "units",
        "_source_file", "_ingested_at", "_channel", "statement", "label", "amount",
    )
    return textract.unionByName(template)


@dlt.view(name="v_normalised_lines")
@dlt.expect_or_drop("valid_abn", "abn_clean RLIKE '^[0-9]{11}$'")
@dlt.expect_or_drop("has_fy_end", "fy_end_date IS NOT NULL")
@dlt.expect_or_drop("has_label", "label_norm IS NOT NULL AND label_norm <> ''")
@dlt.expect("amount_parsed", "amount_aud IS NOT NULL")   # warn only: blank cells are common
def v_normalised_lines():
    return (
        dlt.read_stream("v_raw_lines")
        .withColumn("abn_clean", F.regexp_replace("abn", r"[^0-9]", ""))
        .withColumn("fy_end_date", F.coalesce(
            F.to_date("fy_end", "yyyy-MM-dd"), F.to_date("fy_end", "dd/MM/yyyy")))
        .withColumn("label_norm", F.lower(F.trim(F.regexp_replace("label", r"[^A-Za-z0-9 &/\-]", " "))))
        .withColumn("label_norm", F.regexp_replace("label_norm", r"\s+", " "))
        .withColumn("amount_aud", clean_amount(F.col("amount")) * UNIT_MULTIPLIER)
    )


@dlt.table(
    name="silver_line_items",
    comment="Supplier statement lines mapped to the canonical chart of accounts.",
    table_properties={"quality": "silver"},
)
def silver_line_items():
    lines = dlt.read_stream("v_normalised_lines")
    mapping = spark.read.table(COA_MAPPING_TABLE)          # static reference join (stream-static)

    matched = lines.join(
        F.broadcast(mapping), F.expr("label_norm RLIKE label_pattern"), "left"
    )
    return matched.select(
        "submission_id",
        F.col("abn_clean").alias("abn"),
        "company_name",
        F.col("fy_end_date").alias("fy_end"),
        "source", "statement", "label", "label_norm",
        "canonical_field", "priority",
        (F.col("amount_aud") * F.coalesce(F.col("sign"), F.lit(1))).alias("amount_aud"),
        "_channel", "_source_file", "_ingested_at",
    )


@dlt.table(
    name="silver_unmapped_lines",
    comment="Lines with no chart-of-accounts match — analyst review queue.",
)
def silver_unmapped_lines():
    return (
        dlt.read_stream("silver_line_items")
        .where("canonical_field IS NULL AND amount_aud IS NOT NULL")
        .select("submission_id", "abn", "company_name", "fy_end", "statement",
                "label", "amount_aud", "_source_file", "_ingested_at")
    )


# ---------------------------------------------------------------------------
# SILVER — one row per ABN + financial year (input to ratio_engine.sql)
# ---------------------------------------------------------------------------
@dlt.table(
    name="financial_statements",
    comment="Normalised supplier financial statements, one row per ABN and financial year.",
    table_properties={"quality": "silver"},
)
@dlt.expect("balance_sheet_balances",
            "abs(total_assets - (total_liabilities + total_equity)) <= 0.01 * greatest(abs(total_assets), 1)")
@dlt.expect("has_revenue", "revenue IS NOT NULL")
@dlt.expect_or_fail("no_duplicate_years", "row_cnt = 1")
def financial_statements():
    lines = dlt.read("silver_line_items").where("canonical_field IS NOT NULL")

    # Latest submission wins for each ABN + year (a supplier may re-upload corrected accounts)
    latest_sub = Window.partitionBy("abn", "fy_end").orderBy(F.col("_ingested_at").desc())
    latest = (
        lines.withColumn("sub_rank", F.dense_rank().over(latest_sub))
             .where("sub_rank = 1")
    )

    # Where several labels map to one field (e.g. 'sales' and 'revenue'), keep the highest-priority match
    best = Window.partitionBy("abn", "fy_end", "canonical_field").orderBy(
        F.col("priority").asc_nulls_last(), F.col("amount_aud").desc_nulls_last())
    deduped = latest.withColumn("rn", F.row_number().over(best)).where("rn = 1")

    wide = (
        deduped.groupBy("abn", "fy_end")
        .pivot("canonical_field", CANONICAL_FIELDS)
        .agg(F.first("amount_aud"))
    )
    header = deduped.groupBy("abn", "fy_end").agg(
        F.first("company_name").alias("company_name"),
        F.first("source").alias("source"),
        F.first("submission_id").alias("submission_id"),
        F.max("_ingested_at").alias("_ingested_at"),
    )

    return (
        header.join(wide, ["abn", "fy_end"])
        # anzsic_division is enriched later from ABR data; placeholder keeps the ratio engine schema
        .withColumn("anzsic_division", F.lit(None).cast("string"))
        # derive totals when the supplier only gave components
        .withColumn("total_assets", F.coalesce(
            "total_assets", F.col("total_current_assets") + F.col("total_non_current_assets")))
        .withColumn("total_liabilities", F.coalesce(
            "total_liabilities", F.col("total_current_liabilities") + F.col("total_non_current_liabilities")))
        .withColumn("total_equity", F.coalesce(
            "total_equity", F.col("total_assets") - F.col("total_liabilities")))
        .withColumn("row_cnt", F.count("*").over(Window.partitionBy("abn", "fy_end")))
    )
