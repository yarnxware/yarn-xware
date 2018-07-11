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
package io.yarn.work;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class DefaultWorkThreadFactory implements ThreadFactory {

    private static AtomicInteger workSchedulerNumber = new AtomicInteger(1);
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private ThreadGroup threadGroup;
    private String namePrefix;

    public DefaultWorkThreadFactory() {
        this(getDefaultThreadGroup(), getDefaultNamePrefix());
    }

    public DefaultWorkThreadFactory(ThreadGroup threadGroup) {
        this(threadGroup, getDefaultNamePrefix());
    }

    public DefaultWorkThreadFactory(String namePrefix) {
        this(getDefaultThreadGroup(), namePrefix);
    }

    public DefaultWorkThreadFactory(ThreadGroup threadGroup, String namePrefix) {
        this.threadGroup = threadGroup;
        this.namePrefix = namePrefix;
    }

    private static ThreadGroup getDefaultThreadGroup() {
        SecurityManager s = System.getSecurityManager();
        ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        return group;
    }

    private static String getDefaultNamePrefix() {
        String prefix = "work-scheduler-"
                + workSchedulerNumber.getAndIncrement()
                + "-thread-";
        return prefix;
    }

    @Override
    public Thread newThread(Runnable run) {
        Thread thread = new Thread(threadGroup, run, namePrefix + threadNumber.getAndIncrement());
        return thread;
    }

}