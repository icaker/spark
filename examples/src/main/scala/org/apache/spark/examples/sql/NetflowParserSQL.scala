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

package org.apache.spark.examples.sql

import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext

/**
 * Parse the Netflow format file created by nprobe. Print time, in_rate, out_rate in bps
 * Usage: NetflowParserSQL <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.sql.NetflowParserSQL localdir outputdir
 *
 */

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
//NetflowRecord(SRC_VLAN: String, FIRST_SWITCHED: Double, IPV4_SRC_ADDR: String, IPV4_DST_ADDR: String, IN_BYTES: Double, OUT_BYTES: Double, IN_PKTS: Double, OUT_PKTS: Double, PROTOCOL: Int, L4_SRC_PORT: Int, L4_DST_PORT: Int, FLOWS: Int, DIRECTION: Int)

case class NetflowRecord(FIRST_SWITCHED: Double, IPV4_SRC_ADDR: String, IPV4_DST_ADDR: String, IN_BYTES: Double, OUT_BYTES: Double, IN_PKTS: Double, OUT_PKTS: Double)

object NetflowParserSQL {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetflowParserSQL <directory> <outputdir>")
      System.exit(1)
    }

//    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("NetflowParserSQL")

    // Create the context
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val sqlContext = new SQLContext(sc)

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val files = ssc.textFileStreamRecur(args(0))

    val results = files.transform(parse(_, sqlContext))

    results.saveAsTextFiles(args(1))

    ssc.start()
    ssc.awaitTermination()
  }

  def parse(input: RDD[String], sqlContext: SQLContext):RDD[String] ={

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._



    val first_line = input.first()

    val parsed_input = input.filter(line => line.compareTo(first_line) != 0)

    val tables = parsed_input.map(_.split(",")).map(p => NetflowRecord(p(1).toDouble, p(2), p(3), p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble))

    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    tables.registerAsTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("sum(in_bytes):")
    val results = sql("SELECT MIN(FIRST_SWITCHED), SUM(IN_BYTES) * 8 / 60, SUM(OUT_BYTES) * 8 / 60 FROM records")

    results.map(line => line.toString)
  }
}
