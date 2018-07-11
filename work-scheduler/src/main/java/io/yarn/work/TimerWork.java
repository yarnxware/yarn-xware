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

import io.yarn.common.i18n.LocalStringsManager;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 */
public class TimerWork extends WorkBase {

    private static final Logger _logger = Logger.getLogger(TimerWork.class.getName());

    private static final LocalStringsManager strings = LocalStringsManager.getManager(TimerWork.class);

    private boolean repeatableSchedule = true;
    
    private int scheduleInterval = 30 * 1000;

    private long delayTime = -1;

    private long nextSchedueTime = -1;

    private Runnable task;

    public TimerWork(String name) {
        super(name);
    }

    public TimerWork(String name, Runnable task) {
        super(name);
        this.task = task;
    }

    public int getScheduleInterval() {
        return scheduleInterval;
    }

    public void setScheduleInterval(int scheduleInterval) {
        this.scheduleInterval = scheduleInterval;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    @Override
    public boolean readyToExecute() {
        long currentTime = System.currentTimeMillis();
        if (nextSchedueTime == -1 && delayTime > 0) {
            nextSchedueTime = currentTime + delayTime;
        }

        return currentTime > nextSchedueTime;
    }

    @Override
    public void doWork() {
        nextSchedueTime = System.currentTimeMillis() + scheduleInterval;

        try {
            doTimerWork();
        } catch (RuntimeException ex) {
            String errMsg = strings.get("timer.work.encount.exception", getName());
            _logger.log(Level.SEVERE, errMsg, ex);
        }

    }

    protected void doTimerWork() {
        if (task != null) {
            task.run();

            if (task instanceof Work) {
                Work work = (Work) task;
                if (work.hasException()) {
                    Throwable th = work.getException();
                    String errMsg = strings.get("timer.work.encount.exception", work.getName());
                    _logger.log(Level.SEVERE, errMsg, th);
                }

                work.recycle();
            }
        }
    }

    public boolean supportReschedule() {
        return !super.isScheduleOnlyOnce() && repeatableSchedule;
    }

    public void cancelRescheduler() {
        repeatableSchedule = false;
    }

}
