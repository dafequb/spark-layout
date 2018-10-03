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

package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, SortDataFormat, Sorter}

private[graphx]
class MyEdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
  size: Int = 64) {
  private[this] val edgeSrc = new PrimitiveVector[VertexId](size)
  private[this] val edgeDst = new PrimitiveVector[VertexId](size)
  private[this] val edgeD = new PrimitiveVector[ED](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    edgeSrc += src
    edgeDst += dst
    edgeD += d
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    // val edgeSrcArray = edgeSrc.trim().array
    // val edgeDstArray = edgeDst.trim().array
    // val edgeDArray = edgeD.trim().array
    // new Sorter(Edge.edgeArraySortDataFormat[ED])
    // .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeSrc.length)
    val localDstIds = new Array[Int](edgeDst.length)
    val data = new Array[ED](edgeD.length)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index. Also populate a map from vertex id to a sequential local offset.
    if (edgeSrc.length > 0) {
      index.update(edgeSrc.apply(0), 0)
      var currSrcId: VertexId = edgeSrc.apply(0)
      var currLocalId = -1
      var i = 0
      while (i < edgeSrc.length) {
        val srcId = edgeSrc.apply(i)
        val dstId = edgeDst.apply(i)
        localSrcIds(i) = global2local.changeValue(srcId,
          { currLocalId += 1; local2global += srcId; currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstId,
          { currLocalId += 1; local2global += dstId; currLocalId }, identity)
        data(i) = edgeD.apply(i)
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }

        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs,
      None)
  }
}
