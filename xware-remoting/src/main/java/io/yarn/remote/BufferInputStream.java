/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.yarn.remote;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 *
 */
public class BufferInputStream extends InputStream {

    private ByteBuffer[] buffers;

    private int bufferIndex = 0;

    public BufferInputStream(ByteBuffer[] buffers) {
        this.buffers = buffers;
    }

    @Override
    public int read() throws IOException {

        for (;;) {

            if (bufferIndex >= buffers.length) {
                return -1;
            }

            ByteBuffer buffer = buffers[bufferIndex];
            if (buffer.hasRemaining()) {
                return buffer.get();
            } else {
                bufferIndex++;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int readCount = 0;
        while (readCount < len) {
            if (bufferIndex >= buffers.length) {
                return -1;
            }

            ByteBuffer buffer = buffers[bufferIndex];
            if (buffer.hasRemaining()) {
                int copySize = len - readCount;
                if (buffer.remaining() < copySize) {
                    copySize = buffer.remaining();
                }

                buffer.get(b, off + readCount, copySize);
                readCount += copySize;
            }

        }

        return readCount;
    }

    @Override
    public int available() throws IOException {
        int available = 0;
        for (int i = bufferIndex; i < buffers.length; i++) {
            ByteBuffer byteBuffer = buffers[i];
            if (byteBuffer.hasRemaining()) {
                available += byteBuffer.remaining();
            }
        }

        return available;
    }
    
}
