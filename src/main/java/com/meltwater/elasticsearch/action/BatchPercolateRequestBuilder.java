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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;

public class BatchPercolateRequestBuilder extends BroadcastOperationRequestBuilder<BatchPercolateRequest, BatchPercolateResponse, BatchPercolateRequestBuilder, Client> {

    private BatchPercolateSourceBuilder sourceBuilder;

    public BatchPercolateRequestBuilder(Client client) {
        super(client, new BatchPercolateRequest());
    }

    /**
     * Sets the type of the document to percolate.
     *
     * @param type -
     *
     * @return this
     */
    public BatchPercolateRequestBuilder setDocumentType(String type) {
        request.documentType(type);
        return this;
    }


    /**
     * Sets the raw percolate request body.
     *
     * @param source -
     *
     * @return this
     */
    public BatchPercolateRequestBuilder setSource(BatchPercolateSourceBuilder source) {
        sourceBuilder = source;
        return this;
    }

    private BatchPercolateSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new BatchPercolateSourceBuilder();
        }
        return sourceBuilder;
    }

    @Override
    public BatchPercolateRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        return request;
    }

    @Override
    protected void doExecute(ActionListener<BatchPercolateResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        client.execute(BatchPercolateAction.INSTANCE, request, listener);
    }

}
