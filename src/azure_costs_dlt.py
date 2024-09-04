# Databricks notebook source

bronze_schema = """
  GeneratedKey STRING COMMENT 'Synthetically generated key used for avoiding duplicates getting inserted',
  InvoiceSectionName STRING COMMENT 'Name of the invoice section',
  AccountName STRING COMMENT 'Name of the account',
  AccountOwnerId STRING COMMENT 'ID of the account owner',
  SubscriptionId STRING COMMENT 'ID of the subscription',
  SubscriptionName STRING COMMENT 'Name of the subscription',
  ResourceGroup STRING COMMENT 'Resource group',
  ResourceLocation STRING COMMENT 'Location of the resource',
  Date TIMESTAMP COMMENT 'Date of the record',
  ProductName STRING COMMENT 'Name of the product',
  MeterCategory STRING COMMENT 'Category of the meter',
  MeterSubCategory STRING COMMENT 'Sub-category of the meter',
  MeterId STRING COMMENT 'ID of the meter',
  MeterName STRING COMMENT 'Name of the meter',
  MeterRegion STRING COMMENT 'Region of the meter',
  UnitOfMeasure STRING COMMENT 'Unit of measure',
  Quantity DECIMAL(38,18) COMMENT 'Quantity used',
  EffectivePrice DECIMAL(38,18) COMMENT 'Effective price',
  CostInBillingCurrency DECIMAL(38,18) COMMENT 'Cost in billing currency',
  CostCenter STRING COMMENT 'Cost center',
  ConsumedService STRING COMMENT 'Consumed service',
  ResourceId STRING COMMENT 'ID of the resource',
  Tags STRING COMMENT 'Tags associated with the resource',
  OfferId STRING COMMENT 'ID of the offer',
  AdditionalInfo STRING COMMENT 'Additional information',
  ServiceInfo1 STRING COMMENT 'Service information 1',
  ServiceInfo2 STRING COMMENT 'Service information 2',
  ResourceName STRING COMMENT 'Name of the resource',
  ReservationId STRING COMMENT 'ID of the reservation',
  ReservationName STRING COMMENT 'Name of the reservation',
  UnitPrice DECIMAL(38,18) COMMENT 'Unit price',
  ProductOrderId STRING COMMENT 'ID of the product order',
  ProductOrderName STRING COMMENT 'Name of the product order',
  Term STRING COMMENT 'Term',
  PublisherType STRING COMMENT 'Type of the publisher',
  PublisherName STRING COMMENT 'Name of the publisher',
  ChargeType STRING COMMENT 'Type of charge',
  Frequency STRING COMMENT 'Frequency',
  PricingModel STRING COMMENT 'Pricing model',
  AvailabilityZone STRING COMMENT 'Availability zone',
  BillingAccountId STRING COMMENT 'ID of the billing account',
  BillingAccountName STRING COMMENT 'Name of the billing account',
  BillingCurrencyCode STRING COMMENT 'Billing currency code',
  BillingPeriodStartDate TIMESTAMP COMMENT 'Start date of the billing period',
  BillingPeriodEndDate TIMESTAMP COMMENT 'End date of the billing period',
  BillingProfileId STRING COMMENT 'ID of the billing profile',
  BillingProfileName STRING COMMENT 'Name of the billing profile',
  InvoiceSectionId STRING COMMENT 'ID of the invoice section',
  IsAzureCreditEligible BOOLEAN COMMENT 'Is Azure credit eligible',
  PartNumber STRING COMMENT 'Part number',
  PayGPrice DECIMAL(38,18) COMMENT 'Pay-as-you-go price',
  PlanName STRING COMMENT 'Name of the plan',
  ServiceFamily STRING COMMENT 'Service family',
  CostAllocationRuleName STRING COMMENT 'Name of the cost allocation rule',
  benefitId STRING COMMENT 'ID of the benefit',
  benefitName STRING COMMENT 'Name of the benefit',
  _rescued_data STRING COMMENT 'Rescued data'
"""

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, md5, concat_ws

output_table_name_prefix = spark.conf.get("output_table_name_prefix", "azure_costs")

bronze_table_name = f"{output_table_name_prefix}_bronze"

@dlt.view
def azure_costs_new_data():
    # Read new data from ADLS
    source_path = spark.conf.get(
        "source_path",
        "/Volumes/irina_catalog/jg_schema_1/jg_volume_1/azure_costs/jg_test_cost_export-actual-cost/"
    )
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "*.parquet")
        .load(source_path)
    )

    # Concatenate all columns into a single string with a separator (e.g., '|')
    concatenated_columns = concat_ws("|", *[col(c).cast("string") for c in df.columns])

    # Create and apply generated key using md5 hash
    return df.withColumn("GeneratedKey", md5(concatenated_columns))

    
dlt.create_streaming_live_table(bronze_table_name, schema=bronze_schema)
dlt.apply_changes(
    target=bronze_table_name,
    source="azure_costs_new_data",
    keys=["GeneratedKey"],
    sequence_by=col("Date"),
    except_column_list=["_rescued_data"],
    stored_as_scd_type=1
)

# COMMAND ----------

from pyspark.sql.functions import from_json, concat, lit, col

silver_table_name = f"{output_table_name_prefix}_silver"

@dlt.table(
    name=f"{output_table_name_prefix}_silver",
    comment="Silver level data. Adds a TagsArray column by parsing the Tags column."
)
def azure_costs_silver():
    bronze_df = dlt.read_stream(bronze_table_name)
    return bronze_df.withColumn(
        "TagsMap",
        from_json(concat(lit("{"), col("Tags"), lit("}")), "MAP<STRING, STRING>")
    )

# COMMAND ----------

from pyspark.sql.functions import sum

@dlt.table(
    name=f"{output_table_name_prefix}_gold",
    comment="Gold level data. Aggregates Azure cost data.",
)
def azure_costs_gold():
    silver_df = dlt.read(silver_table_name)
    return (
        silver_df.groupBy("MeterCategory", "MeterSubCategory", "MeterName", "Date")
        .agg(sum(col("UnitPrice").cast("DECIMAL(38,18)")).alias("sum_unit_price"))
        .select(
            col("MeterCategory").alias("meter_category"),
            col("MeterSubCategory").alias("meter_subcategory"),
            col("MeterName").alias("meter_name"),
            col("Date").alias("date"),
            col("sum_unit_price"),
        )
    )