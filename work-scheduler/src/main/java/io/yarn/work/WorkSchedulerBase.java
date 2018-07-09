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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 *
 */
public abstract class WorkSchedulerBase extends WorkBase implements WorkScheduler, WorkListener {

    /**
     * Default 3 seconds.
     */
    private static final long DEFAULT_CHECK_WORK_INTERVAL = 3000;

    protected final LinkedList<Work> workQueue = new LinkedList<Work>();

    private boolean scheduleBySequence = true;

    private volatile boolean prohibitAddingWork = false;

    private final Lock schedulerLock = new ReentrantLock();

    private final Condition workCoordinator = schedulerLock.newCondition();

    private long checkWorkInterval = DEFAULT_CHECK_WORK_INTERVAL;

    private final Condition checkWorkReady = schedulerLock.newCondition();

    public WorkSchedulerBase(String name) {
        super(name);
    }

    public boolean isScheduleBySequence() {
        return scheduleBySequence;
    }

    public void setScheduleBySequence(boolean scheduleBySequence) {
        this.scheduleBySequence = scheduleBySequence;
    }

    public long getCheckWorkInterval() {
        return checkWorkInterval;
    }

    public void setCheckWorkInterval(long checkWorkInterval) {
        this.checkWorkInterval = checkWorkInterval;
    }
    
    @Override
    public boolean allowAddingWork(Work work) {
        if (prohibitAddingWork) {
            return false;
        }

        return true;
    }

    @Override
    public void prohibitAddingWork() {
        if (prohibitAddingWork) {
            return;
        }

        prohibitAddingWork = true;
    }

    @Override
    public boolean addWork(Work work) {
        boolean workAdded = false;
        try {
            schedulerLock.lock();

            if (allowAddingWork(work)) {
                work.addWorkListener(this);
                workQueue.offer(work);
                workCoordinator.signal();
                workAdded = true;
            }
        } finally {
            schedulerLock.unlock();
        }

        return workAdded;
    }

    @Override
    public void scheduleWorks() {
        while (hasWork()) {
            Work work = pollWork();
            if (work != null) {
                scheduleWork(work);
            }
        }
    }

    @Override
    public boolean hasWork() {
        try {
            schedulerLock.lock();
            if (!isCancelled() && !workQueue.isEmpty()) {
                return true;
            }
        } finally {
            schedulerLock.unlock();
        }

        return false;
    }

    @Override
    public Work pollWork() {

        try {
            schedulerLock.lock();

            Work headerWork = null;
            while (!isCancelled()) {
                if (headerWork != null) {
                    if (headerWork.isCancelled()) {
                        headerWork = null;
                    }

                    if (readyToScheduleWork(headerWork)) {
                        return headerWork;
                    }
                    
                    waitCheckPeriod();
                } else {

                    Iterator<Work> iterator = workQueue.iterator();
                    while (iterator.hasNext()) {
                        Work work = iterator.next();
                        if (work.isCancelled()) {
                            iterator.remove();
                            continue;
                        }

                        if (readyToScheduleWork(work)) {
                            iterator.remove();
                            return work;
                        }

                        if (scheduleBySequence) {
                            headerWork = work;
                            break;
                        }
                    }

                    try {
                        workCoordinator.await();
                    } catch (InterruptedException inEx) {
                    }
                }
            }

        } finally {
            schedulerLock.unlock();
        }

        return null;
    }

    @Override
    public Work pollWork(long timeout) throws InterruptedException {

        long endTime = -1;

        try {
            schedulerLock.lockInterruptibly();

            Work headerWork = null;
            while (!isCancelled()) {

                if (endTime == -1 && timeout > 0) {
                    endTime = System.currentTimeMillis() + timeout;
                } else {
                    if (System.currentTimeMillis() > endTime) {
                        //timeout occured
                        return null;
                    }
                }

                if (headerWork != null) {
                    if (headerWork.isCancelled()) {
                        headerWork = null;
                    }

                    if (readyToScheduleWork(headerWork)) {
                        return headerWork;
                    }

                    waitCheckPeriod();
                } else {

                    Iterator<Work> iterator = workQueue.iterator();
                    while (iterator.hasNext()) {
                        Work work = iterator.next();
                        if (work.isCancelled()) {
                            iterator.remove();
                            continue;
                        }

                        if (readyToScheduleWork(work)) {
                            iterator.remove();
                            return work;
                        }

                        if (scheduleBySequence) {
                            headerWork = work;
                            break;
                        }
                    }

                    try {
                        workCoordinator.await();
                    } catch (InterruptedException inEx) {
                    }
                }
            }

        } finally {
            schedulerLock.unlock();
        }

        return null;
    }

    private void waitCheckPeriod() {
        try {
            long interval = getScheduleWorkInterval();
            if (interval <= 500 || interval >= 60 * 1000) {
                interval = DEFAULT_CHECK_WORK_INTERVAL;
            }
            checkWorkReady.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException inEx) {
        }
    }

    @Override
    public boolean readyToScheduleWork(Work work) {
        return work.readyToExecute();
    }

    @Override
    public long getScheduleWorkInterval() {
        return checkWorkInterval;
    }

    @Override
    public void doWork() {
        scheduleWorks();
    }

    @Override
    public void abortWork() {
    }

    @Override
    public void workStarted(io.yarn.work.Work work) {
    }

    @Override
    public void workAborted(io.yarn.work.Work work, Throwable t) {
    }

    @Override
    public void workCancelled(io.yarn.work.Work work, Throwable t) {
    }

    @Override
    public void workFinished(io.yarn.work.Work work, Throwable t) {
    }


}
