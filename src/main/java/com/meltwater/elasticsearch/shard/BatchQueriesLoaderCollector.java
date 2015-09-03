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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Map;

/**
 */
final class BatchQueriesLoaderCollector extends Collector {

    public static final String ID_FIELD = "id";
    private final Map<String, QueryAndSource> queries = Maps.newHashMap();
    private final JustSourceFieldsVisitor fieldsVisitor = new JustSourceFieldsVisitor();
    private final BatchPercolatorQueriesRegistry percolator;
    private final IndexFieldData idFieldData;
    private final ESLogger logger;

    private SortedBinaryDocValues idValues;
    private AtomicReader reader;

    BatchQueriesLoaderCollector(BatchPercolatorQueriesRegistry percolator, ESLogger logger, MapperService mapperService, IndexFieldDataService indexFieldDataService) {
        this.percolator = percolator;
        this.logger = logger;
        final FieldMapper<?> idMapper = mapperService.smartNameFieldMapper(ID_FIELD); //we use our own ID field.
        this.idFieldData = indexFieldDataService.getForField(idMapper);
    }

    public Map<String, QueryAndSource> queries() {
        return this.queries;
    }

    @Override
    public void collect(int doc) throws IOException {
        idValues.setDocument(doc);
        if (idValues.count() > 0) {
            assert idValues.count() == 1;
            BytesRef id = idValues.valueAt(0);
            fieldsVisitor.reset();
            reader.document(doc, fieldsVisitor);
            try {
                // id is only used for logging, if we fail we log the id in the catch statement
                final QueryAndSource queryAndSource = percolator.parsePercolatorDocument(null, fieldsVisitor.source());
                queries.put(id.utf8ToString(), queryAndSource);
            } catch (Exception e) {
                logger.warn("failed to add query [{}]", e, id.utf8ToString());
            }

        } else {
            logger.error("failed to load query since field [{}] not present", ID_FIELD);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        reader = context.reader();
        idValues = idFieldData.load(context).getBytesValues();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
