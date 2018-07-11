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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 */
public abstract class WorkBase implements Work {

    private static final Logger _logger = Logger.getLogger(WorkBase.class.getName());

    private static final LocalStringsManager strings = LocalStringsManager.getManager(WorkBase.class);

    private String name;

    private volatile boolean cancelled = false;

    private volatile boolean finished = false;

    private boolean finishAfterScheduling = true;

    private boolean scheduleOnlyOnce = true;

    private boolean scheduled = false;

    private boolean supportCancelDuringExecuting = false;

    private boolean executing = false;

    private volatile Thread executingThread;

    private Throwable exception;

    private Object result;

    /**
     * Send work event to work listener.
     */
    private WorkEventNotification workEventNotification = new DefaultWorkEventNotification();

    /**
     * The main lock.
     */
    private final Lock mainLock = new ReentrantLock();

    /**
     * Wait for work finish lock.
     */
    private final Object waitFinishLock = new Object();

    @Override
    public String getName() {
        if (name != null) {
            return name;
        }

        return this.getClass().getSimpleName() + "@" + this.hashCode();
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isFinishAfterScheduling() {
        return finishAfterScheduling;
    }

    @Override
    public void setFinishAfterScheduling(boolean finishAfterScheduling) {
        this.finishAfterScheduling = finishAfterScheduling;
    }

    @Override
    public boolean isScheduleOnlyOnce() {
        return scheduleOnlyOnce;
    }

    @Override
    public void setScheduleOnlyOnce(boolean scheduleOnlyOnce) {
        this.scheduleOnlyOnce = scheduleOnlyOnce;
    }

    @Override
    public boolean isSupportCancelDuringExecuting() {
        return supportCancelDuringExecuting;
    }

    @Override
    public void setSupportCancelDuringExecuting(boolean supportCancelDuringExecuting) {
        this.supportCancelDuringExecuting = supportCancelDuringExecuting;
    }

    public WorkEventNotification getWorkEventNotification() {
        return workEventNotification;
    }

    public void setWorkEventNotification(WorkEventNotification workEventNotification) {
        this.workEventNotification = workEventNotification;
    }

    @Override
    public void addWorkListener(WorkListener workListener) {
        if (workEventNotification != null) {
            workEventNotification.addWorkListener(workListener);
        }
    }

    @Override
    public boolean removeWorkListener(WorkListener workListener) {
        if (workEventNotification != null) {
            return workEventNotification.removeWorkListener(workListener);
        }

        return true;
    }

    @Override
    public boolean hasException() {
        return exception != null;
    }

    @Override
    public Throwable getException() {
        return exception;
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean cancel() {
        try {
            mainLock.lock();

            if (cancelled) {
                return true;
            }

            if (!supportCancel()) {
                return false;
            }

            cancelled = true;
        } finally {
            mainLock.unlock();
        }

        return true;
    }

    @Override
    public boolean supportCancel() {
        try {
            mainLock.lock();

            if (finished) {
                return false;
            }

            if (scheduleOnlyOnce && scheduled) {
                return false;
            }

            if (executing && !supportCancelDuringExecuting) {
                return false;
            }
        } finally {
            mainLock.unlock();
        }

        return true;
    }

    @Override
    public boolean readyToExecute() {
        return true;
    }

    @Override
    public void run() {
        boolean scheduledByThisThread = false;
        try {
            try {
                mainLock.lock();
                if (cancelled || finished) {
                    return;
                }

                if (scheduleOnlyOnce && scheduled) {
                    return;
                }

                if (executing) {
                    //executing by other thread
                    return;
                }

                if (!scheduled) {
                    scheduled = true;
                }

                executingThread = Thread.currentThread();
                executing = true;
                scheduledByThisThread = true;
            } finally {
                mainLock.unlock();
            }

            beforeWork();
            if (canExecute()) {
                doWork();
            } else {
                abortWork();
            }
        } catch (Throwable t) {
            handleUndexptedException(t);
        } finally {
            if (scheduledByThisThread) {
                try {
                    mainLock.lock();
                    executing = false;

                    if (finishAfterScheduling) {
                        try {
                            finishInternal();
                        } catch (Throwable t) {
                            handleUndexptedException(t);
                        }
                    }
                } finally {
                    mainLock.unlock();
                }

                //outside main lock
                if (finishAfterScheduling) {
                    synchronized (waitFinishLock) {
                        waitFinishLock.notifyAll();
                    }

                    afterWork();
                }
            }

        }
    }

    @Override
    public boolean canExecute() {
        return true;
    }

    @Override
    public abstract void doWork();

    @Override
    public void abortWork() {
    }

    @Override
    public void interruptWork() {

        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, strings.get("work.interrupt.trying", name));
        }

        boolean interrutped = false;
        Thread thread = null;
        try {
            mainLock.lock();

            if (executing && executingThread != null) {
                executingThread.interrupt();
                interrutped = true;
                thread = executingThread;
            }
        } finally {
            mainLock.unlock();
        }

        if (_logger.isLoggable(Level.FINE)) {
            if (interrutped && thread != null) {
                _logger.log(Level.FINE, strings.get("work.interrupt.sended", name, thread.getName()));
            } else {
                _logger.log(Level.FINE, strings.get("work.interrupt.not.sended", name));
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }

        try {
            mainLock.lock();

            if (finished) {
                return;
            }

            if (executing) {
                throw new IllegalStateException("Could not cancel work " + name + " when work is executing!");
            }

            finishInternal();
        } finally {
            mainLock.unlock();
        }

        synchronized (waitFinishLock) {
            waitFinishLock.notifyAll();
        }

        afterWork();
    }

    private void finishInternal() {
        finished = true;
        executingThread = null;
        executing = false;
    }

    @Override
    public void waitFinish() {
        if (finished || cancelled) {
            return;
        }

        synchronized (waitFinishLock) {
            while (!finished || !cancelled) {
                try {
                    waitFinishLock.wait();
                } catch (InterruptedException inEx) {
                }
            }
        }
    }

    @Override
    public void waitFinish(long timeout) throws InterruptedException {
        if (finished || cancelled) {
            return;
        }

        long beginTime = System.currentTimeMillis();
        long waitTime = timeout;
        synchronized (waitFinishLock) {
            while (!finished || !cancelled) {
                try {
                    waitFinishLock.wait(waitTime);
                    if (timeout > 0) {
                        long elaspe = System.currentTimeMillis() - beginTime;
                        if (elaspe > timeout) {
                            break;
                        } else {
                            waitTime = timeout - elaspe;
                        }
                    }
                } catch (InterruptedException inEx) {
                    throw inEx;
                }
            }
        }
    }

    @Override
    public void waitFinishInterruptibly() throws InterruptedException {
        if (finished || cancelled) {
            return;
        }

        synchronized (waitFinishLock) {
            while (!finished || !cancelled) {
                try {
                    waitFinishLock.wait();
                } catch (InterruptedException inEx) {
                    throw inEx;
                }
            }
        }
    }

    @Override
    public void interruptWaitFinish() {
    }

    public void beforeWork() {
        if (workEventNotification != null) {
            workEventNotification.workStarted(this);
        }
    }

    public void afterWork() {
        if (workEventNotification != null) {
            workEventNotification.workFinished(this, exception);
        }
    }

    protected void handleUndexptedException(Throwable t) {
        if (exception != null) {
            exception.addSuppressed(t);
        } else {
            exception = t;
        }

        _logger.log(Level.SEVERE,
                String.format("Unexcepteted exception occured while executing work %s!", name),
                t);

        if (t instanceof Error) {
            //let it throw up
            throw (Error) t;
        }
    }

    @Override
    public void recycle() {
        try {
            mainLock.lock();

            scheduled = false;
            cancelled = false;
            finished = false;
        } finally {
            mainLock.unlock();
        }
    }
}
