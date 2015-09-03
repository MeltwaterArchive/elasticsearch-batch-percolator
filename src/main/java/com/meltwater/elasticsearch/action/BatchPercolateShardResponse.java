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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/**
 */
public class BatchPercolateShardResponse extends BroadcastShardOperationResponse {

    private Map<String, BatchPercolateResponseItem> responseItems;

    public BatchPercolateShardResponse(){

    }
    public BatchPercolateShardResponse(Map<String, BatchPercolateResponseItem> items, String index, int shardId) {
        super(new ShardId(index, shardId));
        this.responseItems = items;
    }

    public Map<String, BatchPercolateResponseItem> getResponseItems() {
        return responseItems;
    }

    public boolean isEmpty() {
        return responseItems == null || responseItems.size() == 0;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        responseItems = Maps.newHashMap();
        int size = in.readVInt();
        for(int i = 0; i < size; i++){
            String docId = in.readString();
            BatchPercolateResponseItem item = new BatchPercolateResponseItem();
            item.readFrom(in);
            responseItems.put(docId, item);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responseItems.size());
        for(Map.Entry<String, BatchPercolateResponseItem> entry : responseItems.entrySet()){
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }


}
