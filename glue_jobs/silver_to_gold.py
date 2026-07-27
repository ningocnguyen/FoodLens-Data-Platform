import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Job parameters
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_database",
        "source_table",
        "gold_root",
        "processing_date",
        "run_id",
    ],
)

SOURCE_DATABASE = args["source_database"]
SOURCE_TABLE = args["source_table"]
GOLD_ROOT = args["gold_root"]
PROCESSING_DATE = args["processing_date"]
RUN_ID = args["run_id"]
GOLD_GENERATED_AT = datetime.now(timezone.utc)

# ---------------------------------------------------------
# Initialize Spark and Glue
# ---------------------------------------------------------

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Read Silver from the Glue Data Catalog
# ---------------------------------------------------------

silver_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    push_down_predicate=(
        f"processing_date='{PROCESSING_DATE}' "
        f"and run_id='{RUN_ID}'"
    ),
)

silver_df = silver_dynamic_frame.toDF()

source_count = silver_df.count()

print(f"Silver source count: {source_count}")


# ---------------------------------------------------------
# Normalize dimensions
# ---------------------------------------------------------

fact_products_df = (
    silver_df
    .select(
        "barcode",
        "product_name",

        F.when(
            F.col("brand").isNull()
            | (F.trim(F.col("brand")) == ""),
            F.lit("Unknown"),
        )
        .otherwise(
            F.initcap(
                F.lower(
                    F.trim(F.col("brand"))
                )
            )
        )
        .alias("brand"),

        F.when(
            F.col("categories").isNull()
            | (F.trim(F.col("categories")) == ""),
            F.lit("Unknown")
        )
        .otherwise(
            F.trim(
                F.split(
                    F.col("categories"),
                    ","
                )[0]
            )
        )
        .alias("category"),

        F.when(
            F.col("countries").isNull()
            | (F.trim(F.col("countries")) == ""),
            F.lit("Unknown")
        )
        .otherwise(
            F.trim(
                F.split(
                    F.col("countries"),
                    ","
                )[0]
            )
        )
        .alias("country"),

        F.when(
            F.col("nutrition_grade").isNull()
            | (F.trim(F.col("nutrition_grade")) == ""),
            F.lit("Unknown")
        )
        .otherwise(
            F.upper(
                F.trim(
                    F.col("nutrition_grade")
                )
            )
        )
        .alias("nutrition_grade"),

        "energy_kcal_100g",
        "fat_100g",
        "proteins_100g",
        "salt_100g",
        "sugars_100g",

        "completeness_score",

        "processed_at",
        "processing_date",
        "run_id",
    )
)
fact_products_df.cache()
fact_product_count = fact_products_df.count()

# ---------------------------------------------------------
# Build Gold brand summary
# ---------------------------------------------------------

brand_summary_df = (
    fact_products_df
    .groupBy("brand")
    .agg(
        F.count("*").alias("product_count"),
        F.round(
            F.avg("energy_kcal_100g"),
            2,
        ).alias("avg_energy_kcal_100g"),
        F.round(
            F.avg("proteins_100g"),
            2,
        ).alias("avg_proteins_100g"),
        F.round(
            F.avg("fat_100g"),
            2,
        ).alias("avg_fat_100g"),
        F.round(
            F.avg("sugars_100g"),
            2,
        ).alias("avg_sugars_100g"),
        F.round(
            F.avg("completeness_score"),
            4,
        ).alias("avg_completeness_score"),
    )
    .withColumn(
        "processing_date",
        F.lit(PROCESSING_DATE),
    )
    .withColumn(
        "run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "gold_generated_at",
        F.lit(GOLD_GENERATED_AT),
    )
)

category_summary_df = (
    fact_products_df
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),

        F.round(
            F.avg("energy_kcal_100g"),
            2,
        ).alias("avg_energy_kcal_100g"),

        F.round(
            F.avg("proteins_100g"),
            2,
        ).alias("avg_proteins_100g"),

        F.round(
            F.avg("fat_100g"),
            2,
        ).alias("avg_fat_100g"),

        F.round(
            F.avg("sugars_100g"),
            2,
        ).alias("avg_sugars_100g"),

        F.round(
            F.avg("completeness_score"),
            4,
        ).alias("avg_completeness_score"),

        F.sum(
            F.when(
                F.col("nutrition_grade") == "A",
                1
            ).otherwise(0)
        ).alias("grade_a_count"),

        F.sum(
            F.when(
                F.col("nutrition_grade") == "B",
                1
            ).otherwise(0)
        ).alias("grade_b_count"),

        F.sum(
            F.when(
                F.col("nutrition_grade") == "C",
                1
            ).otherwise(0)
        ).alias("grade_c_count"),

        F.sum(
            F.when(
                F.col("nutrition_grade") == "D",
                1
            ).otherwise(0)
        ).alias("grade_d_count"),

        F.sum(
            F.when(
                F.col("nutrition_grade") == "E",
                1
            ).otherwise(0)
        ).alias("grade_e_count"),
    )
    .withColumn(
        "processing_date",
        F.lit(PROCESSING_DATE),
    )
    .withColumn(
        "run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "gold_generated_at",
        F.lit(GOLD_GENERATED_AT),
    )
)
# ---------------------------------------------------------
# Write Gold Parquet
# ---------------------------------------------------------

fact_products_path = (
    f"{GOLD_ROOT}/fact_products/"
    f"processing_date={PROCESSING_DATE}/"
    f"run_id={RUN_ID}"
)

(
    fact_products_df
    .drop("processing_date", "run_id")
    .write
    .mode("overwrite")
    .parquet(fact_products_path)
)

brand_summary_path = (
    f"{GOLD_ROOT}/brand_summary/"
    f"processing_date={PROCESSING_DATE}/"
    f"run_id={RUN_ID}"
)

(
    brand_summary_df
    .drop("processing_date", "run_id")
    .write
    .mode("overwrite")
    .parquet(brand_summary_path)
)

category_summary_path = (
    f"{GOLD_ROOT}/category_summary/"
    f"processing_date={PROCESSING_DATE}/"
    f"run_id={RUN_ID}"
)

(
    category_summary_df
    .drop("processing_date", "run_id")
    .write
    .mode("overwrite")
    .parquet(category_summary_path)
)

# ---------------------------------------------------------
# Operational logging
# ---------------------------------------------------------

brand_count = brand_summary_df.count()
category_count = category_summary_df.count()

print("Gold transformation completed")
print(f"Source Silver count: {source_count}")

print(f"Fact products rows: {fact_product_count}")
print(f"Fact products path: {fact_products_path}")

print(f"Brand summary rows: {brand_count}")
print(f"Brand summary path: {brand_summary_path}")

print(f"Category summary rows: {category_count}")
print(f"Category summary path: {category_summary_path}")

brand_summary_df.orderBy(
    F.desc("product_count")
).show(
    20,
    truncate=False,
)

category_summary_df.orderBy(
    F.desc("product_count")
).show(
    20,
    truncate=False,
)

fact_products_df.show(
    20,
    truncate=False,
)

fact_products_df.unpersist()

job.commit()