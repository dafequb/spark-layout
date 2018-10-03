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

// import org.apache.spark.graphx.{ GraphLoader, VertexId }
import org.apache.spark.{ SparkConf, SparkContext }

// import org.apache.spark.graphx.impl.{MyEdgePartitionBuilder, GraphImpl}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

// import hpdcj.examples.graphx.impl._

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, SortDataFormat, Sorter}
import java.util.logging.Logger
import java.util.logging.Level




/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object BFS {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf().setAppName("PageRankExample")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.INFO)
    sc.setLogLevel("INFO")

    // Edge file
    val edgeFile = args(0)

    // Int array
    val edgeArrayType = args(2)
    // Partitions
    val partitions = if (args.length > 3) args(3).toInt else 0

    // BFS implementation
    // val graph = GraphLoader.EdgeListFile(sc, edgeFile)
    val graph = if (edgeArrayType == "i") GraphLoader.myEdgeListFile(sc, edgeFile, numEdgePartitions = partitions)
      else GraphLoader.edgeListFile(sc, edgeFile, numEdgePartitions = partitions)
    if (edgeArrayType == "h") graph.persist(StorageLevel.MEMORY_ONLY_SER);
    if (edgeArrayType == "g") graph.persist(StorageLevel.OFF_HEAP)
    val root: VertexId = args(1).toInt
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
    if (edgeArrayType == "h") initialGraph.persist(StorageLevel.MEMORY_ONLY_SER);
    if (edgeArrayType == "g") initialGraph.persist(StorageLevel.OFF_HEAP)

    val bfs = initialGraph.pregel(Double.PositiveInfinity)(
      (id, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b));
    if (edgeArrayType == "g") bfs.persist(StorageLevel.OFF_HEAP)
    if (edgeArrayType == "h") bfs.persist(StorageLevel.MEMORY_ONLY_SER);
    logger.info("Results: \n" + bfs.vertices.collect.mkString("\n"));

  }

}

