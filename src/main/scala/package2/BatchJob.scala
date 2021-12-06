/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package package2

import org.apache.flink.api.scala._
//import org.apache.flink.connector.file.sink.FileSink
//import org.apache.avro.Schema
//import org.apache.flink.formats.parquet.avro.ParquetAvroWriters

//import org.apache.flink.api.scala._
//import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._


/**
 * Skeleton for a Flink Batch Job.
 *
 * For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
object BatchJob {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

//    ################# word counts
//    val text = benv.fromElements(
//      "Who's there?",
//      "I think I hear them. Stand, ho! Who's there?")
//    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
//      .map { (_, 1) }
//      .groupBy(0)
//      .sum(1)
//    counts.print()

//    ################# Line counts
    val text = env.readCsvFile[(String,String,String,String,String,String,String,String,String,String,String,String,String)]("./BowlingAvg.csv")
    //    Int,Double,Int,Int,Int,String,Double,Double,Double,Int,Int

    printf("manish row count = " + text.count())
    println(text.count())
    text.print()


//    ################# Table API
//    val settings = EnvironmentSettings.newInstance.inStreamingMode.build

//    val tEnv = TableEnvironment.create(settings)
//    val tableEnv = TableEnvironment.getTableEnvironment(env)

//    tableEnv.registerDataSet("text",tbl)

//    val batchtable = tEnv.from("text")
//
//    val result = batchtable.print()


    env.execute("Flink Batch Scala API Skeleton")
  }
}
