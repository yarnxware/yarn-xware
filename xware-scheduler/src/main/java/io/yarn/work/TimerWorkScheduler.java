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

/**
 *
 *
 */
public class TimerWorkScheduler extends ReactivableWorkScheduler implements WorkScheduler {

    public TimerWorkScheduler(WorkScheduler workScheduler) {
        super(workScheduler);
        super.setScheduleBySequence(false);
    }

    @Override
    public boolean addWork(Work work) {

        if (work.readyToExecute()) {
            work.addWorkListener(this);
            super.scheduleWork(work);
        }

        return super.addWork(work);
    }

    @Override
    public boolean allowAddingWork(Work work) {
        if (!(work instanceof TimerWork)) {
            return false;
        }

        return super.allowAddingWork(work);
    }

    @Override
    public long getScheduleWorkInterval() {
        return getMinInterval();
    }

    private int getMinInterval() {
        int minInterval = -1;
        for (Work work : workQueue) {
            if (work instanceof TimerWork) {
                TimerWork timer = (TimerWork) work;
                if (minInterval == -1) {
                    minInterval = timer.getScheduleInterval();
                } else if (minInterval > timer.getScheduleInterval()) {
                    minInterval = timer.getScheduleInterval();
                }
            }
        }

        return minInterval;
    }

    @Override
    public void workFinished(Work work, Throwable t) {
        super.workFinished(work, t);

        if (work instanceof TimerWork) {
            TimerWork timer = (TimerWork) work;
            if (timer.supportReschedule()) {
                timer.recycle();
                addWork(timer);
            }
        }
    }

}
