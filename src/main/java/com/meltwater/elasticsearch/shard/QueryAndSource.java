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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A Query used for percolation, along with source (and relevant metadata).
 */
public class QueryAndSource {
    final Query query;
    final Optional<Filter> limitingFilter;
    final BytesReference source;

    public QueryAndSource(Query query, Optional<Filter> limitingFilter, BytesReference source) {
        this.query = query;
        this.limitingFilter = limitingFilter;
        this.source = source;
    }

    public Query getQuery() {
        return query;
    }

    public Optional<Filter> getLimitingFilter() {
        return limitingFilter;
    }

    public BytesReference getSource() {
        return source;
    }

}