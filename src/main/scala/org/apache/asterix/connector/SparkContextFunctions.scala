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
package org.apache.asterix.connector

import org.apache.asterix.connector.QueryType.QueryType
import org.apache.asterix.connector.rdd.AsterixRDD
import org.apache.asterix.connector.result.AsterixClient
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord.Status
import org.apache.spark.{Logging, SparkContext}

import scala.util.{Failure, Success, Try}

/**
 * This class extends SparkContext (implicitly) to query AsterixDB.
 * @param sc SparkContext
 */
class SparkContextFunctions(@transient sc: SparkContext) extends Serializable with Logging{

  private val WaitTime = 100;
  private val configuration: Configuration = {
    val sparkConf = sc.getConf

    //Non-optional configurations
    val host: String = sparkConf.get(Configuration.AsterixDBHost)
    val port: String = sparkConf.get(Configuration.AsterixDBPort)
    val frameSize: String = sparkConf.get(Configuration.AsterixDBFrameSize)

    //Optional configurations
    val nFrame: Int = Try(sparkConf.get(Configuration.AsterixDBFrameNumber)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.NUM_FRAMES
    }

    val nReader: Int = Try(sparkConf.get(Configuration.AsterixDBNumberOfReaders)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.NUM_READERS
    }

    val prefetchThreshold: Int = Try(sparkConf.get(Configuration.AsterixDBPrefetchThreshold)) match {
      case Success(n) => n.toInt
      case Failure(e) => AsterixClient.PREFETCH_THRESHOLD
    }

    logInfo(Configuration.AsterixDBHost + " " + host)
    logInfo(Configuration.AsterixDBPort + " " + port)
    logInfo(Configuration.AsterixDBFrameSize + " " + frameSize)
    logInfo(Configuration.AsterixDBFrameNumber + " " + nFrame)
    logInfo(Configuration.AsterixDBNumberOfReaders + " " + nReader)
    logInfo(Configuration.AsterixDBPrefetchThreshold + " " + prefetchThreshold)

    new Configuration(
      host,
      port,
      frameSize.toInt,
      nFrame,
      nReader,
      prefetchThreshold
    )
  }

  private val api = new AsterixHttpAPI(configuration)

  /**
   * AQL Query should work after https://asterix-gerrit.ics.uci.edu/#/c/1653/
   * AQL might get deprecated soon.
   * @param aql
   * @return
   */
  def aql(aql:String): AsterixRDD = {
    executeQuery(aql, QueryType.AQL)
  }

  def sqlpp(sqlpp:String): AsterixRDD = {
    executeQuery(sqlpp, QueryType.SQLPP)
  }

  private def executeQuery(query: String, queryType: QueryType): AsterixRDD = {
    val resultLocations = queryType match {
      case QueryType.AQL => api.executeAQL(query)
      case QueryType.SQLPP => api.executeSQLPP(query)
    }

    val rdd = new AsterixRDD(sc, query, api, resultLocations, configuration)
    rdd
  }

}
