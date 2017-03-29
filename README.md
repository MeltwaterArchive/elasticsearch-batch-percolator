Elasticsearch batch percolator
============================

[![Build Status](https://travis-ci.org/meltwater/elasticsearch-batch-percolator.svg?branch=master)](https://travis-ci.org/meltwater/elasticsearch-batch-percolator)
[ ![Download](https://api.bintray.com/packages/meltwater/elasticsearch-batch-percolator/elasticsearch-batch-percolator/images/download.svg) ](https://bintray.com/meltwater/elasticsearch-batch-percolator/elasticsearch-batch-percolator/_latestVersion)

The batch percolator is a fork of the official elasticsearch [percolator](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/percolate.html). 
It's highly optimized for large volume percolation with complex Lucene queries like wildcards, spans and phrases.

Using the official multi percolator we were able to reach ~1 document/second with 100.000 registered queries. With the batch-percolator, we are currently
handling ~1000 documents/second with 225.000 registered queries. However, this will differ greatly depending on the 
nature of you queries and if you have an efficient strategy for filtering out queries.

For more information, see [this blog post](http://underthehood.meltwater.com/blog/2015/09/29/supercharging-the-elasticsearch-percolator/).

## Installation

    elasticsearch/bin/plugin --install elasticsearch-batch-percolator -u "https://dl.bintray.com/meltwater/elasticsearch-batch-percolator/com/meltwater/elasticsearch-batch-percolator/1.0.1/elasticsearch-batch-percolator-1.0.1.zip"

Version matrix:

    ┌─────────────────────────────────────────┬──────────────────────────┐
    │ Elasticsearch batch percolator          │ ElasticSearch            │
    ├─────────────────────────────────────────┼──────────────────────────┤
    │ 1.x.x                                   │ 1.7.0 ─► 1.7.6           │
    └─────────────────────────────────────────┴──────────────────────────┘


## API documentations

### Create index with mapping

    curl -XPUT localhost:9200/index -d '{
      "mappings": {
        "type": {
          "_source": {
            "enabled": false
          },
          "properties": {
            "field1": {
              "type": "string",
              "index": "analyzed"
            },
            "field2": {
              "type": "string",
              "index": "analyzed"
            }
          }
        }
      }
    }'

You could also use a template, or store a document to get dynamic mapping for the document type to percolate agains

### Registration of queries
    curl -XPOST localhost:9200/index/.batchpercolator/query1 -d '{
      "query": {
        "term": {
          "field1": "fox"
        }
      },
      "highlight": {
        "pre_tags": [
          "<b>"
        ],
        "post_tags": [
          "</b>"
        ],
        "fields": {
          "field1": {}
        }
      }
    }'


### Sending in documents

    curl -XPOST localhost:9200/index/type/_batchpercolate -d '{
      "docs": [
        {
          "_id": "doc1",
          "field1": "the fox is here",
          "field2": "meltwater"
        },
        {
          "_id": "doc2",
          "field1": "the fox is not here",
          "field2": "percolator"
        }
      ]
    }'
   
example response:

    {
      "took": 23,
      "_shards": {
        "total": 5,
        "successful": 5,
        "failed": 0
      },
      "results": [
        {
          "doc": "doc1",
          "matches": [
            {
              "query_id": "query1",
              "highlights": {
                "field1": [
                  "the <b>fox</b> is here"
                ]
              }
            }
          ]
        }
      ]
    }
    

## How does it differ from the official (multi) percolator?
### Batching of documents
The official multi-percolator uses a 'MemoryIndex' which is a highly optimized index often used for percolation. The downside with the MemoryIndex is that it can
only hold one document at a time. 
 
The batch percolator instead uses a RamDirectory which means that we can process documents in batches.

### Two-phase query execution
Complex queries like Span, Phrase and especially MultiPhraseQueries are magnitudes slower than Term or Boolean queries. All complex queries can be
approximated using cheaper queries (for example, a SpanNear can be approximated using an AND query).

In the batch-percolator, a simplified approximation of each query is first executed on the batch of documents. We only execute the original expensive
query if the approximated query has any matches in the batch. This is similar to how Lucene 5 executes those queries,
and we expect to phase out this step once Elasticsearch 2.0 has a stable release.

### Less features
We've removed a lot of features from the official multi-percolator. This means that you can no longer use filter queries or
aggregations on matching queries. You should consider this plugin to be 'vanilla percolation'.  Some of the features 
were removed because they cannot be supported in batch-mode. Some have been removed to reduce the complexity of 
the code.

## Release
The project is hosted at jCenter and builds are uploaded by our Jenkins server. If you are a project maintainer with the necessary credentials, you can build a release locally by running:

    ./gradlew clean build bintray











