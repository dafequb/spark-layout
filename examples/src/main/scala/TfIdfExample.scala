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

// scalastyle:off logger.info
package asap.examples.mllib

// $example on$
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.mllib.feature.wrapByteBuffer
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.reflect._
import java.nio.ByteBuffer
import asap.examples.mllib.utils._
import java.util.ArrayList
import java.lang.CharSequence
import java.util.regex.Pattern
import scala.collection.JavaConversions._

// class TextIndexIterator extends Iterator[Seq[(Int, Int)]] {
      
//       def next() = {
//         val tweet = retrieveNextTweet(lastTweetReceived)
//         lastTweetReceived = tweet.id
//         tweet
//       }
// }


object TfIdfExample {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def splitFn(el: (String, String)): Seq[String] = {
    el._2.trim.split("\\s+").toSeq
  }

  def splitInternFn(el: (String, String)): Seq[String] = {
    val splitArray = el._2.trim.split("\\s+")
    splitArray.foreach(_.intern());
    splitArray.toSeq
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRankExample")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.INFO)
    sc.setLogLevel("INFO")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val inputDir = args(0)
    val inputMethod = args(1)
    // val partitions = args(1)

    // *****************************************
    // Use 2 arrays of indexes with custom splitter
    // *****************************************
    if (inputMethod == "b") {
      logger.info("Processing 2 arrays of indexes (with custom iterables)" + inputMethod)

      // Read files
      val wtRdd = sc.wholeTextFiles(inputDir)
      // Compute token index pairs
      def startIndexesFn (p: (String, String)) = {
        new WordStartEndIterable(p._2).toArray
      }
      val wordIndexRdd = wtRdd.map(p => {
        val startEndIndexes = startIndexesFn(p);
        new WordDoubleIndexIterable(startEndIndexes, p._2);
      })

      val hashingTF = new HashingTF()
      val tf: RDD[Vector] = hashingTF.transform(wordIndexRdd)
      val idf = new IDF().fit(tf)
      val tfidf: RDD[Vector] = idf.transform(tf)

      logger.info("Count is is " + tfidf.count)

    // *****************************************
    // Use array of strings with custom splitter
    // *****************************************
    } else if (inputMethod == "c") {
      logger.info("Processing whole text files with custom split " + inputMethod)
      val regex = "\\w+".r;
      val wtRdd = sc.wholeTextFiles(inputDir).map(p => new WordIterable(p._2).toSeq)
      val hashingTF = new HashingTF()
      val tf: RDD[Vector] = hashingTF.transform(wtRdd)
      val idf = new IDF().fit(tf)
      val tfidf: RDD[Vector] = idf.transform(tf)
      logger.info("Count is is " + tfidf.count)

    // **************************************
    // Iterative
    // **************************************
    } else if (inputMethod == "i") {

      logger.info("Processing iterables " + inputMethod)

      val docsI: RDD[DirectoryIterator.ASAPWordsIterable] = sc.parallelize(new DirectoryIterator.ASAPFileIterable(inputDir).toList)

      val hashingTF = new HashingTF()
      val tf: RDD[Vector] = hashingTF.transform(docsI)
      val idf = new IDF().fit(tf)
      val tfidf: RDD[Vector] = idf.transform(tf)

      logger.info("Count is is " + tfidf.count)
    }

  }
}

class WordIterator(val text: String) extends Iterator[String] with Serializable {

  var i: Int = 0;
  var len = text.length;

  def next: String = {
    // var startIndex = i
    var c:Char = ' ';
    do {
      c = text.charAt(i);
      i += 1;
    } while ((i < len) && ((c == ' ') || (c == '\t') || (c == '\r') || (c == '\n') || (c == '\f')));
    i -= 1;

    var endIndex = i
    do {
      c = text.charAt(endIndex);
      endIndex += 1;
    } while ((endIndex < len) && (c != ' ') && (c != '\t') && (c != '\r') && (c != '\n') && (c != '\f'))
    val currentWord = text.substring(i, endIndex);
    i = endIndex;
    currentWord;
  }

  def hasNext = {
    i < len
  }
}

class WordIterable(val text: String) extends Iterable[String] with Serializable {
  def iterator = new WordIterator(text)
}

class WordStartEndIterator(val text: String) extends Iterator[Int] with Serializable {

  var startIndex: Int = 0;
  var endIndex: Int = 0;
  var len = text.length;

  def next: Int = {
    var c:Char = ' ';
    if (endIndex == 0) {
      do {
        c = text.charAt(startIndex);
        startIndex += 1;
      } while ((startIndex < len) && ((c == ' ') || (c == '\t') || (c == '\r') || (c == '\n') || (c == '\f')));
      endIndex = startIndex;
      startIndex -= 1;
    } else {
      if (endIndex < len) {
        do {
          c = text.charAt(endIndex);
          endIndex += 1;
        } while ((endIndex < len) && (c != ' ') && (c != '\t') && (c != '\r') && (c != '\n') && (c != '\f'))
      }
      startIndex = endIndex;
      endIndex = 0
    }
    startIndex
  }

  def hasNext = {
    startIndex < len
  }
}

class WordStartEndIterable(val text: String) extends Iterable[Int] with Serializable {
  def iterator = new WordStartEndIterator(text)
}

class WordDoubleIndexIterator(val startEndIndexArray: Array[Int], val text: String) extends Iterator[String] with Serializable {

  var i: Int = 0;

  def next: String = {
    val currentWord = text.substring(startEndIndexArray(i), startEndIndexArray(i + 1));
    i += 2;
    currentWord;
  }

  def hasNext = {
    i < startEndIndexArray.length
  }
}

class WordDoubleIndexIterable(val startEndIndexArray: Array[Int], val text: String) extends Iterable[String] with Serializable {
  def iterator = new WordDoubleIndexIterator(startEndIndexArray, text)
}

