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

package hpdcj.examples.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.logging.Logger
import java.util.logging.Level
import org.apache.spark.storage.StorageLevel


/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf().setAppName("PageRankExample")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.INFO)
    sc.setLogLevel("INFO")

    // Arguments
    val iterations = args(0).toInt
    // Edge file
    val edgeFile = args(1)
    // Edge array type (triplet/int)
    val edgeArrayType = args(2)
    // Partitions
    val partitions = if (args.length > 3) args(3).toInt else 0


    // Load the edges as a graph
    //val graph = GraphLoader.edgeListFile(sc, edgeFile)
    val graph = if (edgeArrayType == "i") GraphLoader.myEdgeListFile(sc, edgeFile, numEdgePartitions = partitions)
      else GraphLoader.edgeListFile(sc, edgeFile, numEdgePartitions = partitions)
    if (edgeArrayType == "g") graph.persist(StorageLevel.OFF_HEAP)
    if (edgeArrayType == "h") graph.persist(StorageLevel.MEMORY_ONLY_SER);

    // Checkpoint
    // graph.checkpoint();
    // Run PageRank
    val ranks = graph.staticPageRank(iterations).vertices
    if (edgeArrayType == "g") ranks.persist(StorageLevel.OFF_HEAP)
    if (edgeArrayType == "h") ranks.persist(StorageLevel.MEMORY_ONLY_SER);
    // if (nodeFile != null) {
    //   // Join the ranks with the usernames
    //   val users = sc.textFile(nodeFile).map { line =>
    //     val fields = line.split(",")
    //     (fields(0).toLong, fields(1))
    //   }
    //   val ranksByUsername = users.join(ranks).map {
    //     case (id, (username, rank)) => (username, rank)
    //   }
    //   // Print the result
    //   logger.info("Results: \n" + ranksByUsername.collect().mkString("\n"))
    // } else {
      // Print the result
      logger.info("Results: \n" + ranks.collect())
    // }
  }
}
