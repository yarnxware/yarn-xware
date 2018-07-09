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

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 *
 */
public class DefaultWorkEventNotification implements WorkEventNotification {

    private final CopyOnWriteArrayList<WorkListener> workListeners = new CopyOnWriteArrayList<WorkListener>();

    @Override
    public void addWorkListener(WorkListener workListener) {
        workListeners.addIfAbsent(workListener);
    }

    @Override
    public boolean removeWorkListener(WorkListener workListener) {
        return workListeners.remove(workListener);
    }

    @Override
    public void workStarted(Work work) {
        for (WorkListener workListenr : workListeners) {
            workListenr.workStarted(work);
        }
    }

    @Override
    public void workAborted(Work work, Throwable t) {
        for (WorkListener workListenr : workListeners) {
            workListenr.workAborted(work, t);
        }
    }

    @Override
    public void workCancelled(Work work, Throwable t) {
        for (WorkListener workListenr : workListeners) {
            workListenr.workCancelled(work, t);
        }
    }

    @Override
    public void workFinished(Work work, Throwable t) {
        for (WorkListener workListenr : workListeners) {
            workListenr.workFinished(work, t);
        }
    }

}
