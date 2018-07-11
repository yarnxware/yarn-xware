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
public class DelegatingWorkSecheduler implements WorkScheduler {

    private final WorkScheduler delegating;

    public DelegatingWorkSecheduler(WorkScheduler delegating) {
        this.delegating = delegating;
    }

    @Override
    public String getName() {
        return delegating.getName();
    }

    @Override
    public boolean addWork(Work work) {
        return delegating.addWork(work);
    }

    @Override
    public boolean allowAddingWork(Work work) {
        return delegating.allowAddingWork(work);
    }

    @Override
    public void prohibitAddingWork() {
        delegating.prohibitAddingWork();
    }

    @Override
    public void scheduleWorks() {
        delegating.scheduleWorks();
    }

    @Override
    public void scheduleWork(Work work) {
        delegating.scheduleWork(work);
    }

    @Override
    public boolean hasWork() {
        return delegating.hasWork();
    }

    @Override
    public Work pollWork() {
        return delegating.pollWork();
    }

    @Override
    public Work pollWork(long timeout) throws InterruptedException {
        return delegating.pollWork(timeout);
    }

    @Override
    public boolean readyToScheduleWork(Work work) {
        return delegating.readyToScheduleWork(work);
    }

    @Override
    public long getScheduleWorkInterval() {
        return delegating.getScheduleWorkInterval();
    }

    @Override
    public void addWorkListener(WorkListener workListener) {
        delegating.addWorkListener(workListener);
    }

    @Override
    public boolean removeWorkListener(WorkListener workListener) {
        return delegating.removeWorkListener(workListener);
    }

    @Override
    public boolean hasException() {
        return delegating.hasException();
    }

    @Override
    public Throwable getException() {
        return delegating.getException();
    }

    @Override
    public Object getResult() {
        return delegating.getResult();
    }

    @Override
    public void setScheduleOnlyOnce(boolean scheduleOnlyOnce) {
        delegating.setScheduleOnlyOnce(scheduleOnlyOnce);
    }

    @Override
    public boolean isScheduleOnlyOnce() {
        return delegating.isScheduleOnlyOnce();
    }

    @Override
    public boolean isFinishAfterScheduling() {
        return delegating.isFinishAfterScheduling();
    }

    @Override
    public void setFinishAfterScheduling(boolean finishAfterScheduling) {
        delegating.setFinishAfterScheduling(finishAfterScheduling);
    }

    @Override
    public boolean isSupportCancelDuringExecuting() {
        return delegating.isSupportCancelDuringExecuting();
    }

    @Override
    public void setSupportCancelDuringExecuting(boolean supportCancelDuringExecuting) {
        delegating.setSupportCancelDuringExecuting(supportCancelDuringExecuting);
    }

    @Override
    public boolean isCancelled() {
        return delegating.isCancelled();
    }

    @Override
    public boolean cancel() {
        return delegating.cancel();
    }

    @Override
    public boolean supportCancel() {
        return delegating.supportCancel();
    }

    @Override
    public boolean readyToExecute() {
        return delegating.readyToExecute();
    }

    @Override
    public boolean canExecute() {
        return delegating.canExecute();
    }

    @Override
    public void run() {
        delegating.run();
    }

    @Override
    public void doWork() {
        delegating.doWork();
    }

    @Override
    public void abortWork() {
        delegating.abortWork();
    }

    @Override
    public void interruptWork() {
        delegating.interruptWork();
    }

    @Override
    public boolean isFinished() {
        return delegating.isFinished();
    }

    @Override
    public void finish() {
        delegating.finish();
    }

    @Override
    public void waitFinish() {
        delegating.waitFinish();
    }

    @Override
    public void waitFinish(long timeout) throws InterruptedException {
        delegating.waitFinish(timeout);
    }

    @Override
    public void waitFinishInterruptibly() throws InterruptedException {
        delegating.waitFinishInterruptibly();
    }

    @Override
    public void interruptWaitFinish() {
        delegating.interruptWaitFinish();
    }

    @Override
    public void recycle() {
        delegating.recycle();
    }

}
