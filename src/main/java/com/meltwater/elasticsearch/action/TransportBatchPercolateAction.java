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
package com.meltwater.elasticsearch.action;

import com.meltwater.elasticsearch.index.BatchPercolateException;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportBatchPercolateAction extends TransportBroadcastOperationAction<BatchPercolateRequest, BatchPercolateResponse, BatchPercolateShardRequest, BatchPercolateShardResponse> {

    private final BatchPercolatorService percolatorService;

    @Inject
    public TransportBatchPercolateAction(Settings settings,
                                         ThreadPool threadPool,
                                         ClusterService clusterService,
                                         TransportService transportService,
                                         BatchPercolatorService percolatorService,
                                         ActionFilters actionFilters) {
        super(settings, BatchPercolateAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.percolatorService = percolatorService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected BatchPercolateRequest newRequest() {
        return new BatchPercolateRequest();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, BatchPercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, BatchPercolateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected BatchPercolateResponse newResponse(BatchPercolateRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        return mergeResults(request, shardsResponses);
    }

    public static BatchPercolateResponse mergeResults(BatchPercolateRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;

        List<BatchPercolateShardResponse> shardResults = newArrayList();
        List<ShardOperationFailedException> shardFailures = null;

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                BatchPercolateShardResponse batchPercolateShardResponse = (BatchPercolateShardResponse) shardResponse;
                successfulShards++;
                if (!batchPercolateShardResponse.isEmpty()) {
                    shardResults.add(batchPercolateShardResponse);
                }
            }
        }

        long tookInMillis = System.currentTimeMillis() - request.startTime;
        if (shardResults.isEmpty()) {
            return new BatchPercolateResponse(
                    new ArrayList<BatchPercolateResponseItem>(), tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures
            );
        } else {
            Map<String, BatchPercolateResponseItem> mergedItems = Maps.newHashMap();
            for(BatchPercolateShardResponse response : shardResults){
                for(Map.Entry<String, BatchPercolateResponseItem> entry : response.getResponseItems().entrySet()){
                    if(!mergedItems.containsKey(entry.getKey())){
                        mergedItems.put(entry.getKey(), new BatchPercolateResponseItem());
                    }
                    BatchPercolateResponseItem mergedItem = mergedItems.get(entry.getKey());
                    mergedItem.getMatches().putAll(entry.getValue().getMatches());
                    mergedItem.setDocId(entry.getValue().getDocId());
                }
            }

            List<BatchPercolateResponseItem> listItems = Lists.newArrayList(mergedItems.values());
            return new BatchPercolateResponse(
                    listItems, tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures
            );
        }
    }

    @Override
    protected BatchPercolateShardRequest newShardRequest() {
        return new BatchPercolateShardRequest();
    }

    @Override
    protected BatchPercolateShardRequest newShardRequest(int numShards, ShardRouting shard, BatchPercolateRequest request) {
        return new BatchPercolateShardRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected BatchPercolateShardResponse newShardResponse() {
        return new BatchPercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, BatchPercolateRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(null, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, null);
    }

    @Override
    protected BatchPercolateShardResponse shardOperation(BatchPercolateShardRequest request) throws ElasticsearchException {
        long requestId = request.hashCode();
        long startTime = System.currentTimeMillis();
        try {
            logger.debug("{}-{} Got percolation request.",
                    request.shardId(),
                    requestId);
            BatchPercolateShardResponse out = percolatorService.percolate(request);
            logger.debug("{}-{} Done with percolation request. It took '{}' millis and contained '{}' response items.",
                    request.shardId(),
                    requestId,
                    System.currentTimeMillis() - startTime,
                    out.getResponseItems().size());
            return out;
        } catch (Throwable e) {
            logger.debug("{}-{} Failed to percolate request. It used '{}' millis before crashing.", e,
                    request.shardId(),
                    requestId,
                    System.currentTimeMillis() - startTime);
            throw new BatchPercolateException(request.shardId(), "Failed to percolate.", e);
        }
    }


}
