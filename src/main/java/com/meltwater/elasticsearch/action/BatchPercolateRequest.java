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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *
 */
public class BatchPercolateRequest extends BroadcastOperationRequest<BatchPercolateRequest> {

    private String documentType;
    private BytesReference source;

    // Used internally in order to compute tookInMillis, TransportBroadcastOperationAction itself doesn't allow
    // to hold it temporarily in an easy way
    long startTime = System.currentTimeMillis();

    public BatchPercolateRequest() {
    }

    public BatchPercolateRequest(BatchPercolateRequest request, BytesReference docSource) {
        super(request.indices());
        this.documentType = request.getDocumentType();
        this.source = request.source;
        this.startTime = request.startTime;
    }

    public String getDocumentType() {
        return documentType;
    }

    public void documentType(String type) {
        this.documentType = type;
    }

    public BytesReference source() {
        return source;
    }


    public BatchPercolateRequest source(BatchPercolateSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(XContentType.JSON);
        return this;
    }

    public BatchPercolateRequest source(BytesReference source) {
        this.source = source;
        return this;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (documentType == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startTime = in.readVLong();
        documentType = in.readString();
        source = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(startTime);
        out.writeString(documentType);
        out.writeBytesReference(source);
    }
}
