package com.glo

import com.glo.utils.JobParameters
import org.apache.spark.sql.SparkSession

object JobRunnerLocal {

  def main(args: Array[String]): Unit = {

    val sparkSession = createSparkSession()
    val path = System.getProperty("user.dir")

    CountryProductsRatings.calculateProductsCountryRatings(sparkSession, getJobParams(path))

    sparkSession.close()
  }

  private def getJobParams(path: String) = {
    JobParameters(
      inputPath = "src/test/resources/test-task_dataset_summer_products.csv",
      partitions = 8,
      writePartitions = 1,
      outputPath = s"$path/processing_result/products_ratings.csv"
    )
  }

  private def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("processing")
      .master("local[*]")
      .getOrCreate()
  }

}