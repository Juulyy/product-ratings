package com.glo.utils

case class JobParameters(inputPath: String = null,
                         partitions: Int = 1,
                         writePartitions: Int = 1,
                         outputPath: String = null)
