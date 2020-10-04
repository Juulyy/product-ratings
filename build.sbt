name := "product-ratings"

version := "0.1"

scalaVersion := "2.12.12"

assemblyJarName in assembly := "products-ratings.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % Provided
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test