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
public class ReactivableWorkScheduler extends WorkSchedulerBase implements WorkScheduler {

    private WorkScheduler workScheduler;
    
    private boolean inScheduling = false;

    public ReactivableWorkScheduler(String name, WorkScheduler workScheduler) {
        super(name);
        this.workScheduler = workScheduler;
        super.setScheduleOnlyOnce(false);
        super.setFinishAfterScheduling(false);
    }

    @Override
    public boolean addWork(Work work) {
        boolean workAdded = super.addWork(work);
        if (workAdded) {
            activeScheduler();
        }

        return workAdded;
    }

    @Override
    public void scheduleWorks() {
        try {
            super.scheduleWorks();
        } finally {
            deactiveScheduler();
        }
    }

    @Override
    public void scheduleWork(Work work) {
        workScheduler.scheduleWork(work);
    }

    public void activeScheduler() {
        boolean needSchedule = false;
        synchronized (this) {
            if (!inScheduling) {
                inScheduling = true;
                needSchedule = true;
            }
        }

        if (needSchedule) {
            super.addWork(this);
        }
    }

    public void deactiveScheduler() {
        if (!isCancelled()) {
            synchronized (this) {
                inScheduling = false;
            }
        }
    }

}
