from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import json
import re

type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType()
}
def load_schema_from_json(json_path):
    with open(json_path, "r") as f:
        schema_json = json.load(f)
    return StructType([
        StructField(f["name"], type_mapping[f["type"]], f["nullable"])
        for f in schema_json
    ])

def to_snake_case(df):
    new_columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
    return df.toDF(*new_columns)

def main():

    spark = SparkSession.builder \
        .appName("EcommerceDataIngestion") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-latest") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    
    print("Spark Session Criada Com Sucesso")

    datasets_info = {
        "customers": {
            "csv": "../../data_source/olist_customers_dataset.csv",
            "schema": "../../config/schemas/customers_schema.json"
        },
        "geolocation": {
            "csv": "../../data_source/olist_geolocation_dataset.csv",
            "schema": "../../config/schemas/geolocation_schema.json"
        },
        "order_items": {
            "csv": "../../data_source/olist_order_items_dataset.csv",
            "schema": "../../config/schemas/order_items_schema.json"
        },
        "order_payments": {
            "csv": "../../data_source/olist_order_payments_dataset.csv",
            "schema": "../../config/schemas/order_payments_schema.json"
        },
          "order_reviews": {
            "csv": "../../data_source/olist_order_reviews_dataset.csv",
            "schema": "../../config/schemas/order_reviews_schema.json"
        },
        "orders": {
            "csv": "../../data_source/olist_orders_dataset.csv",
            "schema": "../../config/schemas/orders_schema.json"
        },
        "product_category_name_translation": {
            "csv": "../../data_source/product_category_name_translation.csv",
            "schema": "../../config/schemas/product_category_name_translation_schema.json"
        },
        "products": {
            "csv": "../../data_source/olist_products_dataset.csv",
            "schema": "../../config/schemas/products_schema.json"
        },
        "sellers": {
            "csv": "../../data_source/olist_sellers_dataset.csv",
            "schema": "../../config/schemas/sellers_schema.json"
        },
    }

    gcs_bronze_bucket = "gs://gcs-ecommerce-bucket-bronze"

    for name, info in datasets_info.items():
        print(f"Processando dataset: {name}")

        try:

            schema = load_schema_from_json(info["schema"])
            df = spark.read.schema(schema).option("header", True).csv(info["csv"])

            df_clean = to_snake_case(df)

            print(f"Schema para {name}:")
            df.printSchema()

            gcs_path_target = f"{gcs_bronze_bucket}/ecommerce/{name}"
            df_clean.write.mode("overwrite").parquet(gcs_path_target)

            print(f"Dataset '{name}' gravado em {gcs_path_target}")

        except Exception as e:
            print(f"Erro ao processar {name}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()