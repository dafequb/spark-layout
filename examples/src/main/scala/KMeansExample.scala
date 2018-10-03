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

 // scalastyle:off println
 package hpdcj.examples.mllib

 import org.apache.spark.{SparkConf, SparkContext}
 // $example on$
 import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
 import org.apache.spark.mllib.linalg.Vectors
import java.util.logging.Logger
import java.util.logging.Level
import org.apache.spark.mllib.clustering.MyKMeans
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.{Vector => SVector}
import org.apache.spark.Dependency
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.OneToOneDependency
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.clustering.PKMeans
import org.apache.spark.mllib.clustering.KMeansPartition
import org.apache.spark.mllib.clustering.KMeansRDD
import org.apache.spark.mllib.clustering.KMeansDirectPartition
import org.apache.spark.mllib.clustering.KMeansDirectRDD

 // $example off$

 object KMeansExample {

    val logger: Logger = Logger.getLogger(getClass.getName)

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("KMeansExample")
        val sc = new SparkContext(conf)
        Logger.getLogger("org").setLevel(Level.INFO)
        sc.setLogLevel("INFO")

        // $example on$
        // Load and parse the data
        val method = args(4)
        val splitChar = args(3)
        // Cluster the data into two classes using KMeans
        val numClusters = args(1).toInt
        val numIterations = args(2).toInt
        val count = args(5).toInt
        val dimensions = args(6).toInt
        val partitions = if (args.length > 7) args(7).toInt else 0
        val unions = if (args.length > 8) args(8).toInt else 0
        // Arrays read and transposed
        if (method == "i") {
          // Data
          val data = sc.textFile(args(0))
          logger.info("Dimensions: " + dimensions + ", Count: " + count)
          val indexData = data.zipWithIndex

          val byColumnAndRow = data.zipWithIndex.flatMap {
            case (s:String, row:Long) => {
              s.split(splitChar).zipWithIndex.map {
                case (d:String, column:Int) => column -> (row, d.toDouble)
              }
            }
          }
          val byColumn = byColumnAndRow.groupByKey.sortByKey().values
          val transposed = byColumn.map {
            indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2).toArray
          }
          val dataArray = transposed.collect();


          val parsedIndexes = indexData.map(id => id match {
            case (s:String, i:Long) => {
              val index = i.toInt;
              s.split(splitChar).zipWithIndex.foreach{ case(x, d) =>
                dataArray(d)(index) = x.toDouble
              }
              index
            }
          }).cache()
          val clusters = MyKMeans.train(parsedIndexes, dataArray, numClusters, numIterations)
          // Evaluate clustering by computing Within Set Sum of Squared Errors
          val WSSSE = clusters.computeCost(parsedIndexes, dataArray)
          logger.info("Within Set Sum of Squared Errors = " + WSSSE)

        // Arrays read from driver
        } else if (method == "j") {
          // Data
          val data = sc.textFile(args(0))
          val dataArray = data.map({ l => {
            val a = new Array[Double](count)
            var k = 0
            var i = 0
            var j = 0
            for (c <- l) {
              if (c != ' ') {
                if (i > j) {
                  j = i
                }
                j += 1
              } else if (i >= j) {
                i += 1
              } else {
                a(k) = l.substring(i, j).toDouble
                k += 1
                j += 1
                i = j
              }
            }
            a
          }}).collect
          data.unpersist(true)
          val x = (0 to count - 1).toArray
          val indexes = sc.parallelize(x).cache()

//          var lineArray = sc.textFile(args(0)).map(_.split(splitChar).map(_.toDouble)).zipWithIndex.cache()
//          val dataArray = new Array[Array[Double]](dimensions)
//          for (i <- 0 to dimensions - 1) {
//            dataArray(i) = lineArray.map(_._1(i)).collect
//          }
//          val indexes = lineArray.map(_._2.toInt).cache()
//          lineArray.unpersist()
//          lineArray = null
          time {
            val clusters = MyKMeans.train(indexes, dataArray, numClusters, numIterations, 0, MyKMeans.K_MEANS_PARALLEL, 0) // 5&6

            // Evaluate clustering by computing Within Set Sum of Squared Errors
            val WSSSE = clusters.computeCost(indexes, dataArray) // 7
            logger.info("Within Set Sum of Squared Errors = " + WSSSE)
          }
        }
        else if (method == "k") {
          val data = sc.textFile(args(0))
          val byColumnAndRow = data.zipWithIndex.flatMap {
            case (s:String, row:Long) => {
              s.split("\\s+").zipWithIndex.map {
                case (d:String, column:Int) => column -> (row, d.toDouble)
              }
            }
          }
          val byColumn = byColumnAndRow.groupByKey.sortByKey().values
          val transposed = byColumn.map {
            indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2).toArray
          }
          val dataArray = transposed.collect();
          val indexes = sc.parallelize(List.range(0, count))
          val clusters = MyKMeans.train(indexes, dataArray, numClusters, numIterations) // 5&6
          // Evaluate clustering by computing Within Set Sum of Squared Errors
          val WSSSE = clusters.computeCost(indexes, dataArray) // 7

        } else if (method == "g") {
          val data = sc.textFile(args(0))
          data.persist(StorageLevel.OFF_HEAP)
          val pData = data.map(s => Vectors.dense(s.split(splitChar).map(_.toDouble))).collect
          data.unpersist(true)
          val parsedData = sc.parallelize(pData)
          parsedData.persist(StorageLevel.OFF_HEAP)

          time {
            val clusters = KMeans.train(parsedData, numClusters, numIterations, 0, KMeans.K_MEANS_PARALLEL, 0)
            // Evaluate clustering by computing Within Set Sum of Squared Errors
            val WSSSE = clusters.computeCost(parsedData)
            logger.info("Within Set Sum of Squared Errors = " + WSSSE)
          }
        } else if (method == "h") {
          val data = sc.textFile(args(0))
          if (partitions > 0) data.repartition(partitions)
          val partData = data.mapPartitionsWithIndex { (pid, iter) =>
            val vPoints = Array.fill[ArrayBuffer[Double]](dimensions)(new ArrayBuffer[Double]())
            iter.foreach { line =>
              val strArray = line.split(splitChar);
              var i = 0;
              while (i < strArray.length) {
                vPoints(i) += strArray(i).toDouble
                i += 1
              }
            }
            val points = vPoints.map( pv => pv.toArray )
            Iterator((pid, new KMeansPartition(points)))
          }
          val pData = new KMeansRDD(partData)
//          time {
            val clusters = PKMeans.train(pData, numClusters, numIterations, 0, KMeans.K_MEANS_PARALLEL, 0)
            val WSSSE = clusters.computeCost(pData)
            logger.info("Within Set Sum of Squared Errors = " + WSSSE)
//          }
        } else if (method == "m") {
          val data = if (partitions > 0) sc.textFile(args(0), partitions) else sc.textFile(args(0))
          val partData = data.mapPartitionsWithIndex { (pid, iter) =>
            val vPoints = new ArrayBuffer[Double]()
            iter.foreach { line =>
              val strArray = line.split(splitChar);
              var i = 0;
              while (i < strArray.length) {
                vPoints += strArray(i).toDouble
                i += 1
              }
            }
            Iterator((pid, new KMeansDirectPartition(vPoints.toArray, dimensions)))
          }
          val pData = new KMeansDirectRDD(partData)
          data.unpersist(false);
          // Keep data in mem
          pData.cache()
          val clusters = PKMeans.train(pData, numClusters, numIterations, true, 0, KMeans.K_MEANS_PARALLEL, 0)
          val WSSSE = clusters.computeCost(pData)
          logger.info("Within Set Sum of Squared Errors = " + WSSSE)
        } else if (method == "l") {
          val data = if (partitions > 0) sc.textFile(args(0), partitions) else sc.textFile(args(0))
          val pData = data.map(s => Vectors.dense(s.split(splitChar).map(_.toDouble)))
          data.unpersist(false);
          // Keep data in mem
          pData.cache()
          val clusters = KMeans.train(pData, numClusters, numIterations, true, 0, KMeans.K_MEANS_PARALLEL, 0)
          val WSSSE = clusters.computeCost(pData)
          logger.info("Within Set Sum of Squared Errors = " + WSSSE)
        } else if (method == "n") {
          val data = sc.textFile(args(0))
          data.persist(StorageLevel.MEMORY_ONLY_SER)
          val pData = data.map(s => Vectors.dense(s.split(splitChar).map(_.toDouble))).collect
          data.unpersist(true)
          val parsedData = sc.parallelize(pData)
          parsedData.persist(StorageLevel.MEMORY_ONLY_SER)

          time {
            val clusters = KMeans.train(parsedData, numClusters, numIterations, 0, KMeans.K_MEANS_PARALLEL, 0)
            // Evaluate clustering by computing Within Set Sum of Squared Errors
            val WSSSE = clusters.computeCost(parsedData)
            logger.info("Within Set Sum of Squared Errors = " + WSSSE)
          }
        } else {
          val data = sc.textFile(args(0))
          val pData = data.map(s => Vectors.dense(s.split(splitChar).map(_.toDouble)))
          val cData = pData.collect
          data.unpersist(true)
//          val parsedData = sc.parallelize(pData).cache()
          time {
//            val clusters = KMeans.train(parsedData, numClusters, numIterations, 0, KMeans.K_MEANS_PARALLEL, 0)
            val clusters = KMeans.train(pData, numClusters, numIterations, false, 0, KMeans.K_MEANS_PARALLEL, 0)
            // Evaluate clustering by computing Within Set Sum of Squared Errors
//            val WSSSE = clusters.computeCost(parsedData)
            val WSSSE = clusters.computeCost(pData)
            logger.info("Within Set Sum of Squared Errors = " + WSSSE)
          }
        }

        sc.stop()
    }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}

// scalastyle:on println
