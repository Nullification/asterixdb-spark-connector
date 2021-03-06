/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.connector.rdd

import org.apache.asterix.connector.result.{AsterixResultIterator, AsterixResultReader, AsterixClient}
import org.apache.asterix.connector.{Configuration, Handle, ResultLocations, AsterixHttpAPI}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import org.json.JSONObject

import scala.util.Try

/**
 * AsterixRDD is the bridge between AsterixDB and Spark.
 *
 * @param sc SparkContext
 * @param aql Currently not used.
 * @param api AsterixDB HTTP API client.
 * @param locations Result Locations.
 * @param handle Result handle from the HTTP API for the submitted job.
 */
class AsterixRDD(@transient sc: SparkContext,
                 @transient aql:String,
                 @transient api: AsterixHttpAPI,
                 @transient locations: ResultLocations,
                 @transient handle: Handle,
                 val configuration: Configuration)
  extends RDD[String](sc, Seq.empty){


  override def getPreferredLocations(split:Partition) : Seq[String] = {
    val location = split.asInstanceOf[AsterixPartition].location.address
    Seq(location)
  }



  override def getPartitions : Array[Partition] = {
    val resultLocations = locations
    val distinctLocations = resultLocations.locations.zipWithIndex

    val part = distinctLocations.map(x=> AsterixPartition(x._2,resultLocations.handle,x._1))
    part.toArray
  }


  @transient
  def getSchema : JSONObject = {
    api.getResultSchema(handle)
  }

  @transient
  def repartitionAsterix(numPartitions: Int): RDD[String] ={
    val count = getPartitions.length
    super.repartition(numPartitions)
  }

  override def compute(split:Partition, context:TaskContext): Iterator[String] = {
    val partition = split.asInstanceOf[AsterixPartition]

    val resultReader = new AsterixResultReader(partition.location, partition.index,
      partition.handle, configuration)

    val startTime = System.nanoTime()

    context.addTaskCompletionListener{(context) =>
      val endTime = System.nanoTime()
      logInfo("Finish from running partition:" + partition.index
        + " in " + ((endTime-startTime)/1000000000d) + "s")
    }


    new AsterixResultIterator(resultReader)
  }

}
