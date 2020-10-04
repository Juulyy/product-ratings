package com.glo

import com.glo.utils.JobParameters
import org.apache.spark.sql.SparkSession

object JobRunner {

  def main(args: Array[String]): Unit = {

    val config = parseArgs(args).getOrElse(throw new IllegalArgumentException("Provided arguments are incorrect."))

    CountryProductsRatings.calculateProductsCountryRatings(buildSparkSession("Products ratings"), config)
  }

  def buildSparkSession(name: String): SparkSession = {
    SparkSession.builder
      .appName(s"$name processing")
      .getOrCreate()
  }

  private def parseArgs(args: Array[String]): Option[JobParameters] = {
    //    CommandLineParser
    val parser = new scopt.OptionParser[JobParameters]("scopt") {
      head("scopt", "3.x")

      opt[String]("input-path") required() action { (x, c) =>
        c.copy(inputPath = x)
      }
      opt[String]("output-path") required() action { (x, c) =>
        c.copy(outputPath = x)
      }
      opt[String]("partitions") required() action { (x, c) =>
        c.copy(partitions = x.toInt)
      }
      opt[String]("write-partitions") required() action { (x, c) =>
        c.copy(writePartitions = x.toInt)
      }
    }

    parser.parse(args, JobParameters())
  }
}
