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
package com.meltwater.elasticsearch.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.highlight.HighlightField;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a percolation match for a document
 */
public class QueryMatch implements Streamable, ToXContent{
    String queryId;
    Map<String, HighlightField> hls;

    public QueryMatch(){
        hls = Maps.newHashMap();
    }

    public QueryMatch(String queryId, Map<String, HighlightField> hls) {
        this.queryId = queryId;
        this.hls = hls;
    }

    public String getQueryId() { return queryId; }


    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void setHighlighs(Map<String, HighlightField> highlighs) {
        this.hls = highlighs;
    }

    public Map<String, HighlightField> getHighlights() { return hls;}

    @Override
    public void readFrom(StreamInput in) throws IOException {
        queryId = in.readString();

        int mSize = in.readVInt();
        for (int j = 0; j < mSize; j++) {
            hls.put(in.readString(), HighlightField.readHighlightField(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if(queryId == null){
            throw new ElasticsearchException("No id set to QueryMatch");
        }
        out.writeString(queryId);

        out.writeVInt(hls.size());
        for (Map.Entry<String, HighlightField> entry : hls.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("query_id", queryId);
        builder.startObject("highlights");
        for (HighlightField field : getHighlights().values()) {
            builder.field(field.name());
            if (field.fragments() == null) {
                builder.nullValue();
            } else {
                builder.startArray();
                for (Text fragment : field.fragments()) {
                    builder.value(fragment);
                }
                builder.endArray();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}