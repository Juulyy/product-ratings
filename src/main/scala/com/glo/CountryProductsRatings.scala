package com.glo

import com.glo.utils.JobParameters
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}

object CountryProductsRatings {

  def calculateProductsCountryRatings(sparkSession: SparkSession, jobParameters: JobParameters): Unit = {

    import sparkSession.implicits._

    val productsDF = sparkSession.read
      .option("header", "true")
      .option("charset", "UTF8")
      .schema(getProductsSchema())
      .csv(jobParameters.inputPath)
      .filter('origin_country.isNotNull)
      .filter('price.isNotNull)
      .filter('rating_five_count.isNotNull)
      .filter('rating_count.isNotNull)
      .repartition(jobParameters.partitions, 'product_id)
      .select('origin_country.alias("Country of Origins"), 'price, 'rating_count, 'rating_five_count)
      .groupBy("Country of Origins")
      .agg(
        round(avg("price"), 2).alias("Average price of product"),
        round(sum("rating_five_count") / sum("rating_count") * 100, 2).alias("Share of five-star products")
      )
      .orderBy("Country of Origins")

    productsDF.show(false)

    productsDF
      .coalesce(jobParameters.writePartitions)
      .write
      .option("header", "true")
      .option("charset", "UTF8")
      .mode("overwrite")
      .csv(jobParameters.outputPath)
  }

  def getProductsSchema(): StructType = {
    StructType(Array(
      StructField("title", StringType),
      StructField("title_orig", StringType),
      StructField("price", DataTypes.createDecimalType(15, 2), nullable = false),
      StructField("retail_price", DataTypes.createDecimalType(15, 2), nullable = false),
      StructField("currency_buyer", StringType),
      StructField("units_sold", StringType),
      StructField("uses_ad_boosts", StringType),
      StructField("rating", StringType),
      StructField("rating_count", IntegerType, nullable = false),
      StructField("rating_five_count", IntegerType, nullable = false),
      StructField("rating_four_count", StringType),
      StructField("rating_three_count", StringType),
      StructField("rating_two_count", StringType),
      StructField("rating_one_count", StringType),
      StructField("badges_count", StringType),
      StructField("badge_local_product", StringType),
      StructField("badge_product_quality", StringType),
      StructField("badge_fast_shipping", StringType),
      StructField("tags", StringType),
      StructField("product_color", StringType),
      StructField("product_variation_size_id", StringType),
      StructField("product_variation_inventory", StringType),
      StructField("shipping_option_name", StringType),
      StructField("shipping_option_price", StringType),
      StructField("shipping_is_express", StringType),
      StructField("countries_shipped_to", StringType),
      StructField("inventory_total", StringType),
      StructField("has_urgency_banner", StringType),
      StructField("urgency_text", StringType),
      StructField("origin_country", StringType, nullable = false),
      StructField("merchant_title", StringType),
      StructField("merchant_name", StringType),
      StructField("merchant_info_subtitle", StringType),
      StructField("merchant_rating_count", StringType),
      StructField("merchant_rating", StringType),
      StructField("merchant_id", StringType),
      StructField("merchant_has_profile_picture", StringType),
      StructField("merchant_profile_picture", StringType),
      StructField("product_url", StringType),
      StructField("product_picture", StringType),
      StructField("product_id", StringType),
      StructField("theme", StringType),
      StructField("crawl_month", StringType)
    ))
  }

}
