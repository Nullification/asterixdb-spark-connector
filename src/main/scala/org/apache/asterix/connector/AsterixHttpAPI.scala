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


import java.io.InputStream

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import net.liftweb.json.DefaultFormats
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpGet}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.hyracks.api.dataset.ResultSetId
import org.apache.hyracks.api.{dataset, job}
import org.apache.hyracks.api.job.JobId
import QueryType._
import net.liftweb.json.Serialization._

case class Handle(jobId: JobId, resultSetId: ResultSetId)
case class AddressPortPair(address: String, port: String)
case class ResultLocations(handle:Handle, locations:Seq[AddressPortPair])

//json de-serializations
private[this] case class LocationBean(locations: Seq[AddressPortPair])

/**
 * AsterixDB HTTP API interface.
 * @param configuration Connector configuration
 */
class AsterixHttpAPI(configuration: Configuration) extends org.apache.spark.Logging {

  private val apiURL = s"http://${configuration.host}:${configuration.port}/"
  private implicit val formats = DefaultFormats

  /**
   * AQL Query should work after https://asterix-gerrit.ics.uci.edu/#/c/1653/
   * @param query
   * @return
   */
  def executeAQL(query: String): ResultLocations =
  {
    throw new AsterixConnectorException("AQL is not yet supported.")
  }

  def executeSQLPP(query: String): ResultLocations =
  {
    val jsonString = executeQuery(query, QueryType.SQLPP)
    getResultLocations(jsonString)
  }


  def getResultLocations(response: String) : ResultLocations = {
    val pair = getHandle(response)

    val locationBean = read[LocationBean](getRequest(pair._2))
    ResultLocations(pair._1, locationBean.locations)
  }

  private def getHandle(response: String): (Handle, String) = {
    val om = new ObjectMapper()

    val jsonResponse =  try {
      om.readTree(response).asInstanceOf[ObjectNode]
      } catch {
      case e:JsonProcessingException => throw new AsterixConnectorException("Error while parsing response", e)
    }

    val handle = jsonResponse.get("handle").asText()
    jsonResponse.get("status").asText("error") match {
      case "success" => (parseHandle(handle), handle)
      case _ => throw new AsterixConnectorException(jsonResponse.get("error-code").asInt(),
        jsonResponse.get("summary").asText())
    }
  }

  private def parseHandle(handleUri: String): Handle = {
    val handle = handleUri.split("/").last.split("-")
    Handle(new JobId(handle(0).toLong), new ResultSetId(handle(1).toLong))
  }

  private def executeQuery(query :String, queryType: QueryType) : String= {

    val response = postRequest(apiURL + s"query/service?mode=location", Some(query))

    response
  }

  private def getRequest(url: String) : String = {
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    val httpGet: HttpGet = new HttpGet(url)
    val response: CloseableHttpResponse = httpclient.execute(httpGet)
    val responseContent: InputStream = response.getEntity.getContent
    val responseString = scala.io.Source.fromInputStream(responseContent).mkString
    httpclient.close()
    responseString
  }

  private def postRequest(url: String , entity:Option[String]) : String = {
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    val httpPost: HttpPost = new HttpPost(url)
    entity match {
      case Some(e) => httpPost.setEntity(new StringEntity(e, "UTF-8"));
      case _ =>
    }
    val response: CloseableHttpResponse = httpclient.execute(httpPost)
    val responseContent: InputStream = response.getEntity.getContent
    val responseString = scala.io.Source.fromInputStream(responseContent).mkString
    response.close()
    httpclient.close()
    responseString
  }
}
