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

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 *
 * Cache and allocator ByteBuffers.
 */
public class BufferPool {

    private boolean useDirectByteBuffer = false;

    private int bufferAllocateSize = 4096;

    private int maxBuffers = 512;

    private int cachedBuffers = 0;

    private final LinkedList<ByteBuffer> bufferQueue = new LinkedList<>();

    public boolean isUseDirectByteBuffer() {
        return useDirectByteBuffer;
    }

    public void setUseDirectByteBuffer(boolean useDirectByteBuffer) {
        this.useDirectByteBuffer = useDirectByteBuffer;
    }

    public int getBufferAllocateSize() {
        return bufferAllocateSize;
    }

    public void setBufferAllocateSize(int bufferAllocateSize) {
        this.bufferAllocateSize = bufferAllocateSize;
    }

    public int getMaxBuffers() {
        return maxBuffers;
    }

    public void setMaxBuffers(int maxBuffers) {
        this.maxBuffers = maxBuffers;
    }

    public int getCachedBuffers() {
        return cachedBuffers;
    }

    public void setCachedBuffers(int cachedBuffers) {
        this.cachedBuffers = cachedBuffers;
    }

    public synchronized ByteBuffer[] acquire(int requireSize) {

        int capacity = 0;
        LinkedList<ByteBuffer> chosenByteBuffers = new LinkedList<>();
        while (capacity < requireSize) {
            ByteBuffer byteBuffer;
            if (cachedBuffers > 0) {
                byteBuffer = bufferQueue.pollFirst();
                cachedBuffers--;
            } else {
                byteBuffer = createByteBuffer();
            }

            capacity += byteBuffer.capacity();
            chosenByteBuffers.add(byteBuffer);
        }

        ByteBuffer[] byteBuffers = new ByteBuffer[chosenByteBuffers.size()];
        return byteBuffers;
    }

    /**
     *
     * @return
     */
    private ByteBuffer createByteBuffer() {
        if (useDirectByteBuffer) {
            return ByteBuffer.allocateDirect(bufferAllocateSize);
        } else {
            return ByteBuffer.allocate(bufferAllocateSize);
        }
    }

    /**
     *
     * @param byteBuffers
     */
    public synchronized void release(ByteBuffer[] byteBuffers) {

        for (ByteBuffer byteBuffer : byteBuffers) {
            if (cachedBuffers >= maxBuffers) {
                //discard the buffer
                return;
            }
            bufferQueue.offer(byteBuffer);
            cachedBuffers++;
        }
    }

    /**
     *
     * @param byteBuffer
     */
    public synchronized void release(ByteBuffer byteBuffer) {
        if (cachedBuffers >= maxBuffers) {
            //discard the buffer
            return;
        }

        bufferQueue.offer(byteBuffer);
        cachedBuffers++;
    }

}
