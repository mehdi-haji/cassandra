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

package org.apache.cassandra.stress.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.ChainableLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A load balancing policy similar to the driver's TokenAwarePolicy except that it uses
 * the token range directly, rather than the parition key values, which are not necessarily
 * aware. It only works with statements created via the makeStatement() method.
 */
public class TokenRangeRoutingPolicy implements ChainableLoadBalancingPolicy
{
    private final LoadBalancingPolicy childPolicy;
    private final boolean shuffleReplicas;
    private volatile Metadata clusterMetadata;

    public TokenRangeRoutingPolicy(LoadBalancingPolicy childPolicy, boolean shuffleReplicas) {
        this.childPolicy = childPolicy;
        this.shuffleReplicas = shuffleReplicas;
    }

    public TokenRangeRoutingPolicy(LoadBalancingPolicy childPolicy) {
        this(childPolicy, true);
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
        return childPolicy;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        clusterMetadata = cluster.getMetadata();
        childPolicy.init(cluster, hosts);
    }

    @Override
    public HostDistance distance(Host host) {
        return childPolicy.distance(host);
    }

    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        String keyspace = statement.getKeyspace();
        if (keyspace == null)
            keyspace = loggedKeyspace;

        TokenRange range = statement.getRoutingTokenRange();
        if (range == null)
            return childPolicy.newQueryPlan(keyspace, statement);

        final Set<Host> replicas = clusterMetadata.getReplicas(Metadata.quote(keyspace), range);
        if (replicas.isEmpty())
            return childPolicy.newQueryPlan(loggedKeyspace, statement);

        final Iterator<Host> iter;
        if (shuffleReplicas) {
            List<Host> l = Lists.newArrayList(replicas);
            Collections.shuffle(l);
            iter = l.iterator();
        } else {
            iter = replicas.iterator();
        }

        return new AbstractIterator<Host>() {

            private Iterator<Host> childIterator;

            @Override
            protected Host computeNext() {
                while (iter.hasNext()) {
                    Host host = iter.next();
                    if (host.isUp() && childPolicy.distance(host) == HostDistance.LOCAL)
                        return host;
                }

                if (childIterator == null)
                    childIterator = childPolicy.newQueryPlan(loggedKeyspace, statement);

                while (childIterator.hasNext()) {
                    Host host = childIterator.next();
                    // Skip it if it was already a local replica
                    if (!replicas.contains(host) || childPolicy.distance(host) != HostDistance.LOCAL)
                        return host;
                }
                return endOfData();
            }
        };
    }

    @Override
    public void onUp(Host host) {
        childPolicy.onUp(host);
    }

    @Override
    public void onDown(Host host) {
        childPolicy.onDown(host);
    }

    @Override
    public void onAdd(Host host) {
        childPolicy.onAdd(host);
    }

    @Override
    public void onRemove(Host host) {
        childPolicy.onRemove(host);
    }

    @Override
    public void close() {
        childPolicy.close();
    }
}

