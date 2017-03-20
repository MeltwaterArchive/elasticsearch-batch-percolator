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
package com.meltwater.elasticsearch.index;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.List;

/**
 * Indexes {@link ParsedDocument}s into
 * a {@link RAMDirectory}
 */
public class RamDirectoryPercolatorIndex {

    private final MapperService mapperService;

    public RamDirectoryPercolatorIndex(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    public Directory indexDocuments(List<ParsedDocument> parsedDocuments) {
        try{
            Directory directory = new RAMDirectory();
            IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_4_10_4,
                    mapperService.analysisService().defaultIndexAnalyzer());
            IndexWriter iwriter = new IndexWriter(directory, conf);
            for(ParsedDocument document : parsedDocuments){
                for(ParseContext.Document doc : document.docs()){
                    iwriter.addDocument(doc, document.analyzer());
                }
            }
            iwriter.close();
            return directory;
        } catch(IOException e) {
            throw new ElasticsearchException("Failed to write documents to RAMDirectory", e);
        }
    }
}
