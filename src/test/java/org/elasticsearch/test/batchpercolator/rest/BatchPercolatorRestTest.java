package org.elasticsearch.test.batchpercolator.rest;

import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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

        final String docId = "docId";
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
                        .field("_id", docId)
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

        String responseBody = restResponse.getResponseBody();


        List<String> results = JsonPath.read(responseBody, "$.results");
        assertThat(results.size(), is(1));
        String matchedDoc = JsonPath.read(responseBody, "$.results[0].doc");
        assertThat(matchedDoc, is(docId));
        List<String> matches = JsonPath.read(responseBody, "$.results[0].matches");
        assertThat(matches.size(), is(2));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].query_id").get(0), is("1"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].highlights.field1[0]").get(0), is("the <b>fox</b> is here"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].query_id").get(0), is("2"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].highlights.field2[0]").get(0), is("<b>meltwater</b> percolator"));

    }

}
