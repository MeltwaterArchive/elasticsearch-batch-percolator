package org.elasticsearch.test.batchpercolator.rest;

import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class BatchPercolatorRestTest extends AbstractNodesTests {
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
    public void basicRestPercolationTest() throws ExecutionException, InterruptedException, IOException {
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register query1 with highlights");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(getSource(termQuery("field1", "fox"), new HighlightBuilder().field("field1").preTags("<b>").postTags("</b>")))
                .execute().actionGet();

        logger.info("--> register query2 with highlights");
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

        logger.info("--> Doing percolation with Rest API");
        BytesReference source = new BatchPercolateSourceBuilder().addDoc(
                docBuilder().setDoc(jsonBuilder()
                        .startObject()
                        .field("_id", "docId")
                        .field("field1", "the fox is here")
                        .field("field2", "meltwater percolator")
                        .endObject()))
                .toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS).bytes();

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/test/type/_batchpercolate")
                .setHeader("Content-type", "application/json")
                .setBody(source.toUtf8())
                .execute()
                .get();


        assertThat(restResponse.getStatusCode(), equalTo(200));
        //TODO would be nicer to map to a Java object.
        assertThat(restResponse.getResponseBody(), endsWith(
                "\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0},\"results\":[{\"doc\":\"docId\",\"matches\":[{\"query_id\":\"1\",\"highlights\":{\"field1\":[\"the <b>fox</b> is here\"]}},{\"query_id\":\"2\",\"highlights\":{\"field2\":[\"<b>meltwater</b> percolator\"]}}]}]}"));

    }

}
