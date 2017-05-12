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
package org.apache.asterix.connector.result

import java.net.InetSocketAddress
import java.nio.ByteBuffer


import org.apache.asterix.connector.{Configuration, AsterixConnectorException, Handle, AddressPortPair}
import org.apache.hyracks.api.comm.{FrameHelper, IFrame}
import org.apache.hyracks.api.dataset.IDatasetInputChannelMonitor
import org.apache.hyracks.client.dataset.DatasetClientContext
import org.apache.hyracks.client.net.ClientNetworkManager
import org.apache.hyracks.comm.channels.DatasetNetworkInputChannel
import org.apache.spark.Logging

/**
 * AsterixDB result reader. This class is responsible to fetch result (tuple-by-tuple)
 * for each AsterixRDD partition and converts the binaraized format to a JSON.
 *
 * @param addressPortPair the address and the port of Hyracks result partition.
 * @param partition Hyracks partition number.
 * @param handle Result handle from AsterixDB HTTP API
 * @param configuration Connector configurations
 */
class ResultReader(
  addressPortPair: AddressPortPair,
  partition: Int,
  handle: Handle,
  val configuration: Configuration)
  extends Serializable with Logging{

  private val netManager = new ClientNetworkManager(configuration.nReaders)
  private val datasetClientContext = new DatasetClientContext(configuration.frameSize)
  private val monitor : IDatasetInputChannelMonitor = new DatasetInputChannelMonitor
  private val resultChannel = {
    netManager.start()
    val socketAddress = new InetSocketAddress(addressPortPair.address, addressPortPair.port.toInt)
    val inputChannel = new DatasetNetworkInputChannel(netManager, socketAddress,handle.jobId,
      handle.resultSetId, partition, configuration.nReaders)
    inputChannel.registerMonitor(monitor)

    inputChannel.open(datasetClientContext)

    inputChannel
  }

  def isPartitionReadComplete: Boolean = monitor.getNFramesAvailable <= 0 && monitor.eosReached()
  private def isFailed = monitor.failed()
  private def waitForNextFrame() = {
    monitor.synchronized{
      while (monitor.getNFramesAvailable <= 0 && !monitor.eosReached() && !monitor.failed())
        monitor.wait()
    }

  }

  def read(frame: IFrame) :Int = {
    frame.reset()
    var readSize = 0

    log.debug("Thread " + Thread.currentThread().getId + ": Read partition " + partition +
      " from " + addressPortPair.address)


    while(readSize < frame.getFrameSize && !isPartitionReadComplete) {
      waitForNextFrame()

      if(isFailed) {
        throw new AsterixConnectorException("Reading result failed")
      }

      if(isPartitionReadComplete)
      {
        resultChannel.close()
        frame.getBuffer.flip()
        0
      }

      //Wrap null
      val nextBuffer = Option(resultChannel.getNextBuffer)

      monitor.notifyFrameRead()
      nextBuffer match {
        case Some(byteBuffer) =>
          if (readSize <= 0) {
            val nBlocks = FrameHelper.deserializeNumOfMinFrame(byteBuffer)
            frame.ensureFrameSize(frame.getMinSize * nBlocks)
            frame.getBuffer.clear()
            frame.getBuffer.put(byteBuffer)
            resultChannel.recycleBuffer(byteBuffer)
            readSize = frame.getBuffer.position()
          }
          else {
            frame.getBuffer.put(byteBuffer)
            resultChannel.recycleBuffer(byteBuffer)
            readSize = frame.getBuffer.position()
          }
        case None => log.info ("received a null byte buffer")
      }
    }
    readSize

  }
}
