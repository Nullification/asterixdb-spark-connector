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
package org.apache.asterix.connector.result;


import org.apache.asterix.connector.AsterixConnectorException;
import org.apache.asterix.connector.Configuration;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.Queue;

/**
 * AsterixDB result reader client with the support of pre-fetching.
 */
public class AsterixClient {

    private static final Logger LOGGER = Logger.getLogger(AsterixClient.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final int NUM_FRAMES = 1;
    public static final int NUM_READERS = 2;
    public static final int PREFETCH_THRESHOLD = 2;

    private final IFrameTupleAccessor fta;
    private final IFrame frame;
    private final Queue<String> resultList;
    private final ResultReader resultReader;
    private final Runnable prefetchThread;
    private final Configuration configuration;

    /**
     * Get AsterixClient for a specific AsterixDB job.
     *
     * @param resultReader AstreixDB result reader.
     * @throws HyracksDataException
     */
    public AsterixClient(ResultReader resultReader) throws HyracksDataException
    {
        configuration = resultReader.configuration();
        fta = new ResultFrameTupleAccessor();
        int frameSize = configuration.frameSize();
        frame = new VSizeFrame(new FrameManager(frameSize));
        this.resultReader = resultReader;
        resultList = new LinkedList<>();
        prefetchThread = this::prefetch;
        prefetch();
    }

    /**
     * A method used by prefetchThread to get result frames.
     *
     * @return number of bytes read.
     */
    private int prefetch(){
        int readSize = 0;
        boolean keepReading = true;
        for (int i=0;i<configuration.nFrames() && keepReading;i++) {
            readSize = resultReader.read(frame);
            if(readSize > 0)
                jsonize();
            else
                keepReading = false;
        }
        return readSize;
    }

    /**
     * Converts result into a String JSON.
     */
    private void jsonize() {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        try {
            fta.reset(frame.getBuffer());
            int last = fta.getTupleCount();
            String result;
            for (int tIndex = 0; tIndex < last; tIndex++) {
                int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                bbis.setByteBuffer(frame.getBuffer(), start);
                byte[] recordBytes = new byte[length];
                int numread = bbis.read(recordBytes, 0, length);

                result = new String(recordBytes, 0, numread, UTF_8);
                resultList.add(result);
            }
            frame.getBuffer().clear();
        } finally {
            try {
                bbis.close();
            } catch (IOException e) {
                LOGGER.error("Could not close buffer.", e);
            }
        }

    }

    /**
     * Get one tuple from the result buffer.
     *
     * @return tuple as a string JSON object.
     * @throws AsterixConnectorException
     */
    public String getResultTuple() throws AsterixConnectorException {
        if(resultList.size() == 0 && resultReader.isPartitionReadComplete())
            return null;
        else if(resultList.size() < configuration.prefetchThreshold()) {
            prefetchThread.run();
        }
        return resultList.remove();
    }


    public boolean hasNext() {
        return resultList.size() > 0  || !resultReader.isPartitionReadComplete();
    }
}
