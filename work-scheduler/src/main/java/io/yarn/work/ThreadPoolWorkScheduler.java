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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

/**
 *
 *
 */
public class ThreadPoolWorkScheduler extends WorkSchedulerBase {

    private int minThread = 0;

    private int maxThread = 5;

    private final ThreadFactory threadFactory;

    private Set<WorkThread> workThreads = new HashSet<WorkThread>();

    private int waitThreads = 0;

    private boolean scheduled = false;

    public int getMinThread() {
        return minThread;
    }

    public void setMinThread(int minThread) {
        this.minThread = minThread;
    }

    public int getMaxThread() {
        return maxThread;
    }

    public void setMaxThread(int maxThread) {
        this.maxThread = maxThread;
    }

    public ThreadPoolWorkScheduler(String name) {
        super(name);
        threadFactory = new DefaultWorkThreadFactory();
    }

    public ThreadPoolWorkScheduler(String name, ThreadFactory threadFactory) {
        super(name);
        this.threadFactory = threadFactory;
    }

    @Override
    public boolean addWork(Work work) {
        boolean added = super.addWork(work);

        if (scheduled) {
            if (waitThreads < maxThread && workThreads.size() < maxThread) {
                WorkThread workThread = new WorkThread(this);
                Thread realThread = threadFactory.newThread(workThread);
                workThread.setThread(realThread);
                workThreads.add(workThread);
                realThread.start();
            }
        }

        return added;
    }

    @Override
    public boolean allowAddingWork(Work work) {
        return super.allowAddingWork(work);
    }

    @Override
    public Work pollWork() {
        return super.pollWork();
    }

    @Override
    public Work pollWork(long timeout) throws InterruptedException {
        return super.pollWork(timeout);
    }

    @Override
    public void scheduleWorks() {
        scheduled = true;

        int taskSize = workQueue.size();
        int createNums = minThread;
        if (taskSize > 2 * maxThread) {
            createNums = maxThread;
        } else if (taskSize > minThread) {
            createNums = minThread + (taskSize - minThread) / 2;
        }

        createThreads(createNums);
    }

    @Override
    public void scheduleWork(Work work) {
        // do nothing
    }

    private void createThreads(int createNums) {
        for (int i = 0; i < createNums; i++) {
            WorkThread workThread = new WorkThread(this);
            Thread realThread = threadFactory.newThread(workThread);
            workThread.setThread(realThread);
            workThreads.add(workThread);
            realThread.start();
        }
    }

    /*package*/ void removeThread(WorkThread workThread) {
        workThreads.remove(workThread);
    }

}
