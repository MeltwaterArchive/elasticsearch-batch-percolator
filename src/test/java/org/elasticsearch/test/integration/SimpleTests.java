package org.elasticsearch.test.integration;

import com.meltwater.elasticsearch.action.BatchPercolateRequestBuilder;
import com.meltwater.elasticsearch.action.BatchPercolateResponse;
import com.meltwater.elasticsearch.action.BatchPercolateResponseItem;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import com.meltwater.elasticsearch.shard.QueryMatch;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 */
public class SimpleTests extends AbstractNodesTests {
    static Client client;

    @BeforeClass
    public static void createNodes() throws Exception {
        startNode("node1");
        client = client("node1");
    }

    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }

    @Test
    public void testBasicPercolation() throws IOException, InterruptedException {
        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> register a queries");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                        .startObject()
                            .field("query", matchQuery("field1", "b"))
                        .field("group", "g1")
                        .field("query_hash", "hash1")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject()
                            .field("query", matchQuery("field1", "c"))
                        .field("group", "g2")
                        .field("query_hash", "hash2")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject()
                        .field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c")))
                        .field("group", "g3")
                        .field("query_hash", "hash3").endObject()
                ).execute().actionGet();
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchAllQuery())
                        .field("group", "g4")
                        .field("query_hash", "hash4").endObject()
                ).execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(jsonBuilder().startObject().field("_id", "1").field("field1", "b").endObject())))
                .execute().actionGet();

        assertThat(response.getResults().size(), is(1));
        BatchPercolateResponseItem firstDocResponse = response.getResults().get(0);
        assertThat(firstDocResponse.getDocId(), is("1"));
        assertThat(firstDocResponse.getMatches().size(), is(2));
        assertThat(firstDocResponse.getMatches().keySet(), hasItems("1", "4"));

        QueryMatch firstMatch = firstDocResponse.getMatches().get("1");
        QueryMatch secondMatch = firstDocResponse.getMatches().get("4");

        assertThat(firstMatch.getQueryId(), is("1"));
        assertThat(firstMatch.getHighlights().size(), is(0));

        assertThat(secondMatch.getQueryId(), is("4"));
        assertThat(secondMatch.getHighlights().size(), is(0));

    }

    @Test
    public void twoDocsOneQuery() throws IOException {
        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register a query");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(getSource(
                        boolQuery().should(termQuery("field1", "fox")).should(termQuery("field2", "meltwater")).minimumNumberShouldMatch(1), new HighlightBuilder()))
                        .execute().actionGet();

        logger.info("--> percolate batch of two docs");
        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder()
                        .addDoc(docBuilder().setDoc(
                            jsonBuilder()
                                    .startObject()
                                    .field("_id", "id1")
                                    .field("field1", "the fox is here")
                                    .field("field2", "no match")
                                    .endObject()))
                        .addDoc(docBuilder().setDoc(
                                jsonBuilder()
                                        .startObject()
                                        .field("_id", "id2")
                                        .field("field1", "no match")
                                        .field("field2", "meltwater percolator")
                                        .endObject())))
                .execute().actionGet();

        assertThat(response.getResults().size(), is(2));
        BatchPercolateResponseItem firstDocResponse = response.getResults().get(1);
        BatchPercolateResponseItem secondDocResponse = response.getResults().get(0);
        assertThat(firstDocResponse.getDocId(), is("id1"));
        assertThat(secondDocResponse.getDocId(), is("id2"));
        assertThat(firstDocResponse.getMatches().size(), is(1));
        assertThat(secondDocResponse.getMatches().size(), is(1));

    }

    @Test
    public void twoQueriesWithHighlights() throws IOException, InterruptedException {
        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register a query with highlights");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(getSource(termQuery("field1", "fox"), new HighlightBuilder().field("field1").preTags("<b>").postTags("</b>")))
                .execute().actionGet();

        //Register one more query
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "2")
                .setSource(getSource(termQuery("field2", "meltwater"),
                        new HighlightBuilder()
                                .requireFieldMatch(true)
                                .order("score")
                                .highlightQuery(termQuery("field2", "meltwater"))
                                .field("field2")
                                    .preTags("<b>")
                                    .postTags("</b>")))
                .execute().actionGet();

        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("_id", "docId")
                                .field("field1", "the fox is here")
                                .field("field2", "meltwater percolator")
                                .endObject())))
                .execute().actionGet();

        assertThat(response.getResults().size(), is(1));
        assertThat(response.getResults().size(), is(1));
        BatchPercolateResponseItem item = response.getResults().get(0);
        Map<String, QueryMatch> matches = item.getMatches();

        assertThat(matches.get("1").getHighlights().size(), is(1));
        assertThat(matches.get("2").getHighlights().size(), is(1));

        assertThat(matches.get("1").getHighlights().get("field1").fragments()[0].string(), equalTo("the <b>fox</b> is here"));
        assertThat(matches.get("2").getHighlights().get("field2").fragments()[0].string(), equalTo("<b>meltwater</b> percolator"));
    }

    @Test
    public void dynamicAddingRemovingQueries() throws Exception {
        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1").setSource("field1", "value").execute().actionGet();

        logger.info("--> register a query 1");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "kuku")
                .setSource(getSource(termQuery("field1", "value1")))
                        .setRefresh(true)
                        .execute().actionGet();

        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("_id", "docId")
                                .field("field1", "value1")
                                .endObject())))
                .execute().actionGet();


        assertThat(response.getResults().get(0).getMatches().keySet(), hasItems("kuku"));

        logger.info("--> deleting query 1");
        client.prepareDelete("test", BatchPercolatorService.TYPE_NAME, "kuku").setRefresh(true).execute().actionGet();

        response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("_id", "docId")
                                .field("field1", "value1")
                                .endObject())))
                .execute().actionGet();

        assertThat(response.getResults().get(0).getMatches().size(), is(0));
    }

    @Test
    public void testNestedPercolation() throws IOException, ExecutionException, InterruptedException {
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        initNestedIndexAndPercolation(client);
        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setDocumentType("company").setSource(
                new BatchPercolateSourceBuilder().addDoc(
                        BatchPercolateSourceBuilder.docBuilder().setDoc(
                                getNotMatchingNestedDoc()
                        )
                )
        ).execute().actionGet();

        assert response.getResults().get(0).getMatches().size() == 0;

        response = new BatchPercolateRequestBuilder(client).setDocumentType("company").setSource(
                new BatchPercolateSourceBuilder().addDoc(
                        BatchPercolateSourceBuilder.docBuilder().setDoc(
                                getMatchingNestedDoc()
                        )
                )
        ).execute().actionGet();

        assert response.getResults().get(0).getMatches().size() == 1;
        assert response.getResults().get(0).getMatches().containsKey("Q");
    }

    void initNestedIndexAndPercolation(Client client) throws IOException, ExecutionException, InterruptedException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "string").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "string").endObject().endObject().endObject().endObject()
                .endObject();

        client.admin().indices().prepareCreate("nestedindex").addMapping("company", mapping).execute().actionGet();
        waitForYellowStatus(client);

        client.prepareIndex("nestedindex", BatchPercolatorService.TYPE_NAME, "Q").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee", QueryBuilders.matchQuery("employee.name", "virginia potts").operator(MatchQueryBuilder.Operator.AND)).scoreMode("avg")).endObject()).get();

        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    XContentBuilder getMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject()
                .field("_id", "id1")
                .field("companyname", "stark").startArray("employee")
                .startObject().field("name", "virginia potts").endObject()
                .startObject().field("name", "tony stark").endObject()
                .endArray().endObject();
        return doc;
    }

    XContentBuilder getNotMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject()
                .field("_id", "id2")
                .field("companyname", "notstark").startArray("employee")
                .startObject().field("name", "virginia stark").endObject()
                .startObject().field("name", "tony potts").endObject()
                .endArray().endObject();
        return doc;
    }
}
