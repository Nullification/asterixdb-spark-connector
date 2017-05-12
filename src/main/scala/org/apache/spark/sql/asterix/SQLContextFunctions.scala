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
package org.apache.spark.sql.asterix

import org.apache.asterix.connector.QueryType.QueryType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.asterix.connector._
import org.apache.spark.storage.StorageLevel

/**
 * This class extends SQLContext (implicitly) to query AsterixDB
 * using both aql() and sqlpp() methods.
 * @param sqlContext Spark SQLContext
 */
class SQLContextFunctions(@transient sqlContext:SQLContext)
  extends org.apache.spark.Logging with Serializable {

  /**
   * AQL Query should work after https://asterix-gerrit.ics.uci.edu/#/c/1653/
   * AQL might get deprecated soon.
   * @param aqlQuery AQL query.
   * @return
   */
  @transient
  def aql(aqlQuery:String): DataFrame = {
    executeQuery(aqlQuery, QueryType.AQL)
  }

  /**
   * The method takes an AQL query and returns a DataFrame.
   * @param sqlppQuery AQL query.
   * @return
   */
  def sqlpp(sqlppQuery: String): DataFrame = {
    executeQuery(sqlppQuery, QueryType.SQLPP)
  }

  @transient
  private def executeQuery(query: String, queryType: QueryType): DataFrame = {
    val sc = sqlContext.sparkContext
    val rdd = queryType match {
      case QueryType.AQL => sc.aql(query)
      case QueryType.SQLPP => sc.sqlpp(query)
    }

    rdd.persist(StorageLevel.MEMORY_AND_DISK)

    sqlContext.read.json(rdd)
  }
}
