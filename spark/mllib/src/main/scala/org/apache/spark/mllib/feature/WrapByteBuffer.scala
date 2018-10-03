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

package org.apache.spark.mllib.feature

import java.lang.{Iterable => JavaIterable}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


class wrapByteBuffer(buff: ByteBuffer)  {

    val bbuff = buff

    def canEqual(that: Any): Boolean = that.isInstanceOf[wrapByteBuffer]

    override def equals(that: Any): Boolean = {
      that match {
        case that: wrapByteBuffer => (this.hashCode() == that.hashCode())
        case _ => false
      }
    }

    var h: Int = 0

    override def hashCode(): Int = {

      h = 0
      while (buff.position() < buff.limit) {
        val b: Byte = buff.get()
        h = 31 * h + b.toChar;
      }
      h
    }

    def getBytes(): Array[Byte] = {
      val bytes: Array[Byte] = new Array[Byte](bbuff.remaining());
      bbuff.get(bytes);
      return bytes;
    }

    def printAsString() {
        var bytes: Array[Byte] = Array.fill[Byte](buff.remaining())(0)
        buff.mark()
        buff.get(bytes, 0, bytes.length)
        val str = new String(bytes, "UTF-8")
        // scalastyle:off println
        println("TERM, " + str)
        // scalastyle:on println
        buff.reset()
      }

  }

