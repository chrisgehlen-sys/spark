/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.benchmark

import java.io.File

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf


/**
 * Benchmark to measure data source read performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DataSourceReadBenchmark-results.txt".
 * }}}
 */
object ParquetDataSourceReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("DataSourceReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "4g")
      .setIfMissing("spark.executor.memory", "4g")
      .setIfMissing(UI_ENABLED, false)

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    sparkSession
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def saveAsParquetTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetTable")
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "parquetTable") {
        import spark.implicits._

        spark.range(values)
          .map(_ => (Random.nextLong, Random.nextDouble, Random.nextInt,
            Random.nextLong, Random.nextDouble, Random.nextInt,
            Random.nextLong, Random.nextDouble, Random.nextInt,
            Random.nextLong, Random.nextDouble, Random.nextInt,
            Random.nextLong, Random.nextDouble, Random.nextInt))
          .toDF("value", "value1", "value2", "value3", "value4", "value5", "value6", "value7",
            "value8", "value9", "value10", "value11", "value12", "value13", "value14")
          .createOrReplaceTempView("t1")

        val testDf = spark.sql("SELECT 1 p0, 2 AS p1, 3 AS p2, value " +
          "AS id, value1 AS v1, value2 AS v2, value3 AS v3, value4 AS v4, value5 AS v5, value6 AS" +
          " v6, value7 AS v7, value8 AS v8 , value9 AS v9, value10 AS v10, value11 AS v11, " +
          "value12 AS v12, value13 AS v13, value14 AS v14 " +
          "FROM t1").write.partitionBy("p0", "p1", "p2")

        saveAsParquetTable(testDf, dir.getCanonicalPath + "/parquet")

        benchmark.addCase("18 columns sum - Parquet MR") { _ =>
          spark.sql("select sum(id), sum(v1), sum(v2), sum(v3), sum(v4), sum(v5), sum(v6)" +
            ", sum(v7), sum(v8), sum(v9), sum(v10), sum(v11), sum(v12), sum(v13), sum(v14), " +
            "sum(p0), sum(p1), sum(p2)" +
            "from parquetTable").collect()
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Partitioned Table Scan") {
      partitionTableScanBenchmark(1024 * 1024 * 32)
    }
  }
}
