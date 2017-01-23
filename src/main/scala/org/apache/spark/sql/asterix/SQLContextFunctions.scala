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
 *
 * It also adds the ability to let AsterixDB infer the schema without any additional pass.
 *
 * @param sqlContext Spark SQLContext
 */
class SQLContextFunctions(@transient sqlContext:SQLContext)
  extends org.apache.spark.Logging with Serializable {

  private def camelize(value: String): String = {
    value(0) match  {
      case '[' =>
        val arrayType = value.substring(1, value.length - 1).replace("int64", "long")
        "[" + arrayType(0).toUpper + arrayType.substring(1) + "]"

      case _ => value(0).toUpper + value.substring(1)
    }
  }

  @transient
  private def admToCaseClass(adm:String): String = {
    println("/*")
    println(adm)
    println("*/")
    val types = adm.trim.replaceAll("\n", "").split('}')
    val res = types.map { t =>
      val classNameFields = t.split('{')

      val className = camelize(classNameFields(0))
      val fields = classNameFields(1).split(',').map{ f =>
        val nameType = f.split(':')
        val name = nameType(0).replaceAll(" ", "")

        val typeCamelized = camelize(nameType(1))
        val typeString = typeCamelized match {
          case "Int64" => "Long"
          case list if list(0) == '[' => "Array" + list
          case  _ => typeCamelized
        }
        (name, typeString)
      }.sortBy(_._1).map(nt => nt._1 + ": " + nt._2).reduceLeft(_ + ", " + _)


      "case class " + className + "(" + fields + ")\n"
    }.reduceLeft(_ + _)
    res
  }

  /**
   * The method takes an AQL query and returns a DataFrame.
   * @param aqlQuery AQL query.
   * @param infer By default AsterixDB will NOT provide the schema.
   * @param printCaseClasses This will create case classes that represents the schema.
   * @return
   */
  @transient
  def aql(aqlQuery:String, infer:Boolean = false, printCaseClasses:Boolean = false): DataFrame = {
    executeQuery(aqlQuery, QueryType.AQL, infer, printCaseClasses)
  }

  /**
   * The method takes an AQL query and returns a DataFrame.
   * @param sqlppQuery AQL query.
   * @param infer By default AsterixDB will NOT provide the schema.
   * @param printCaseClasses This will create case classes that represents the schema.
   * @return
   */
  def sqlpp(sqlppQuery: String, infer: Boolean = false, printCaseClasses: Boolean = false): DataFrame = {
    executeQuery(sqlppQuery, QueryType.SQLPP, infer, printCaseClasses)
  }

  @transient
  private def executeQuery(query: String, queryType: QueryType, infer: Boolean,
                           printCaseClasses:Boolean): DataFrame = {
    val sc = sqlContext.sparkContext
    val rdd = queryType match {
      case QueryType.AQL => sc.aql(query)
      case QueryType.SQLPP => sc.sqlpp(query)
    }

    val partitionedRdd = rdd
    partitionedRdd.persist(StorageLevel.MEMORY_AND_DISK)

    if(infer) {
      log.info("Preparing schema")
      val schemaJSON = rdd.getSchema
      val dummyRdd = sc.parallelize(Seq(schemaJSON.getString("DUMMY_JSON")))
      val dummyDF = sqlContext.read.json(dummyRdd)
      if(printCaseClasses) {
        println("//------------- BEGIN -------------")
        println(admToCaseClass(schemaJSON.getString("ADM")))
        println("//-------------  END  -------------")
      }
      sqlContext.read.schema(dummyDF.schema).json(partitionedRdd)
    }
    else {
      sqlContext.read.json(partitionedRdd)
    }
  }
}
