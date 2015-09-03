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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder to create the percolate request body.
 */
public class BatchPercolateSourceBuilder implements ToXContent {

    private List<DocBuilder> docBuilders;

    public BatchPercolateSourceBuilder(){
        docBuilders = Lists.newArrayList();
    }



    public BatchPercolateSourceBuilder(List<DocBuilder> docBuilders) {this.docBuilders = docBuilders;}

    /**
     * Sets the document to run the percolate queries against.
     *
     * @param docBuilder -
     *
     * @return this
     */
    public BatchPercolateSourceBuilder addDoc(DocBuilder docBuilder) {
        docBuilders.add(docBuilder);
        return this;
    }


    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("docs");
        for(DocBuilder docBuilder : docBuilders){
            docBuilder.toXContent(builder,params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static DocBuilder docBuilder() {
        return new DocBuilder();
    }

    public static class DocBuilder implements ToXContent {

        private BytesReference doc;

        public DocBuilder setDoc(BytesReference doc) {
            this.doc = doc;
            return this;
        }

        public DocBuilder setDoc(String field, Object value) {
            Map<String, Object> values = new HashMap<>(2);
            values.put(field, value);
            setDoc(values);
            return this;
        }

        public DocBuilder setDoc(String doc) {
            this.doc = new BytesArray(doc);
            return this;
        }

        public DocBuilder setDoc(XContentBuilder doc) {
            this.doc = doc.bytes();
            return this;
        }

        public DocBuilder setDoc(Map<String, Object> doc) {
            return setDoc(doc, Requests.CONTENT_TYPE);
        }

        public DocBuilder setDoc(Map<String, Object> doc, XContentType contentType) {
            try {
                return setDoc(XContentFactory.contentBuilder(contentType).map(doc));
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + doc + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentType contentType = XContentFactory.xContentType(doc);

            try (XContentParser parser = XContentFactory.xContent(contentType).createParser(doc)) {
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }

            return builder;
        }
    }

}
