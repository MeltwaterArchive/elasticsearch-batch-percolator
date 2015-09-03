/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.meltwater.elasticsearch.plugin;

import com.meltwater.elasticsearch.action.BatchPercolateAction;
import com.meltwater.elasticsearch.action.TransportBatchPercolateAction;
import com.meltwater.elasticsearch.modules.BatchPercolatorModule;
import com.meltwater.elasticsearch.modules.BatchPercolatorShardModule;
import com.meltwater.elasticsearch.rest.RestBatchPercolateAction;
import com.meltwater.elasticsearch.shard.BatchPercolatorQueriesRegistry;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.CloseableIndexComponent;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BatchPercolatorPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "elasticsearch-batch-percolator";
    }

    @Override
    public String description() {
        return "Elasticsearch batch percolator";
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        List<Class<? extends Module>> modules = new ArrayList<>(1);
        modules.add(BatchPercolatorModule.class);
        return modules;
    }

    @Override
    public Collection<Class<? extends Module>> shardModules() {
        List<Class<? extends Module>> modules = new ArrayList<>(1);
        modules.add(BatchPercolatorShardModule.class);
        return modules;
    }


    @Override
    public Collection<Class<? extends CloseableIndexComponent>> shardServices() {
        List<Class<? extends CloseableIndexComponent>> shardServices = new ArrayList<>(1);
        shardServices.add(BatchPercolatorQueriesRegistry.class);
        return shardServices;
    }

    public void onModule(ActionModule module) {
        module.registerAction(BatchPercolateAction.INSTANCE, TransportBatchPercolateAction.class);
    }


    public void onModule(RestModule module) {
        module.addRestAction(RestBatchPercolateAction.class);
    }
}
