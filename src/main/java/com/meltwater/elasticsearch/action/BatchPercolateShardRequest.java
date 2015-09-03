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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 */
public class BatchPercolateShardRequest extends BroadcastShardOperationRequest {

    private String documentType;
    private BytesReference source;

    public BatchPercolateShardRequest() {
    }

    public BatchPercolateShardRequest(String index, int shardId, BatchPercolateRequest request) {
        super(new ShardId(index, shardId), request);
        this.documentType = request.getDocumentType();
        this.source = request.source();
    }

    public BatchPercolateShardRequest(ShardId shardId, BatchPercolateRequest request) {
        super(shardId, request);
        this.documentType = request.getDocumentType();
        this.source = request.source();
    }

    public String documentType() {
        return documentType;
    }

    public BytesReference source() {
        return source;
    }

    void documentType(String documentType) {
        this.documentType = documentType;
    }

    void source(BytesReference source) {
        this.source = source;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        documentType = in.readString();
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(documentType);
        out.writeBytesReference(source);
    }

}
