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
package com.meltwater.elasticsearch.rest;

import com.meltwater.elasticsearch.action.BatchPercolateAction;
import com.meltwater.elasticsearch.action.BatchPercolateRequest;
import com.meltwater.elasticsearch.action.BatchPercolateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;


public class RestBatchPercolateAction extends BaseRestHandler {

    @Inject
    public RestBatchPercolateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_batchpercolate", this);
        controller.registerHandler(POST, "/{index}/_batchpercolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_batchpercolate", this);

        controller.registerHandler(GET, "/_batchpercolate", this);
        controller.registerHandler(GET, "/{index}/_batchpercolate", this);
        controller.registerHandler(GET, "/{index}/{type}/_batchpercolate", this);
    }

    @Override
    public void handleRequest(final RestRequest restRequest, final RestChannel restChannel, final Client client) throws Exception {
        BatchPercolateRequest percolateRequest = new BatchPercolateRequest();
        percolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, percolateRequest.indicesOptions()));
        percolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        percolateRequest.documentType(restRequest.param("type"));
        percolateRequest.documentType(percolateRequest.getDocumentType());
        percolateRequest.source(RestActions.getRestContent(restRequest));

        client.execute(BatchPercolateAction.INSTANCE, percolateRequest, new RestToXContentListener<BatchPercolateResponse>(restChannel));
    }
}