/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.monitoring;

class MonitorableImpl implements Monitorable
{
    private final String name;
    private final ConstructionTime constructionTime;
    private final long timeout;
    private MonitoringState state;

    MonitorableImpl(String name, ConstructionTime constructionTime, long timeout)
    {
        this.name = name;
        this.constructionTime = constructionTime;
        this.timeout = timeout;
        this.state = MonitoringState.IN_PROGRESS;
    }

    public String name()
    {
        return name;
    }

    public ConstructionTime constructionTime()
    {
        return constructionTime;
    }

    public long timeout()
    {
        return timeout;
    }

    public boolean isInProgress()
    {
        check();
        return state == MonitoringState.IN_PROGRESS;
    }

    public boolean isAborted()
    {
        check();
        return state == MonitoringState.ABORTED;
    }

    public boolean isCompleted()
    {
        check();
        return state == MonitoringState.COMPLETED;
    }

    public boolean abort()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (constructionTime != null)
                MonitoringTask.addFailedOperation(this, ApproximateTime.currentTimeMillis());
            state = MonitoringState.ABORTED;
            return true;
        }

        return state == MonitoringState.ABORTED;
    }

    public boolean complete()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            state = MonitoringState.COMPLETED;
            return true;
        }

        return state == MonitoringState.COMPLETED;
    }

    private void check()
    {
        if (constructionTime == null || state != MonitoringState.IN_PROGRESS)
            return;

        long elapsed = ApproximateTime.currentTimeMillis() - constructionTime.timestamp;
        if (elapsed >= timeout)
            abort();
    }
}
