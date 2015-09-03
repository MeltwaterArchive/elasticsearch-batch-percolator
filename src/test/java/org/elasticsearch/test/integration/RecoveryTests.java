package org.elasticsearch.test.integration;

import com.meltwater.elasticsearch.action.BatchPercolateRequestBuilder;
import com.meltwater.elasticsearch.action.BatchPercolateResponse;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class RecoveryTests extends AbstractNodesTests {
    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }

    @Test
    @Ignore("This issue exists in official percolator as well. Fixed in elasticsearch 2.0")
    public void testRestartNode() throws IOException, ExecutionException, InterruptedException {
        Settings extraSettings = ImmutableSettings.settingsBuilder()
                .put("index.gateway.type", "local").build();

        logger.info("--> Starting one nodes");
        startNode("node1",extraSettings);
        Client client = client("node1");

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> Register query");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                                .startObject()
                                .field("query", matchQuery("field1", "b"))
                                .field("id", 1)
                                .field("group", "g1")
                                .field("query_hash", "hash1")
                                .endObject()
                ).setRefresh(true).execute().actionGet();
        logger.info("--> Restarting node");
        closeNode("node1");
        startNode("node1", extraSettings);
        client = client("node1");
        logger.info("Waiting for cluster health to be yellow");
        waitForYellowStatus(client);

        logger.info("--> Percolate doc with field1=b");
        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(jsonBuilder().startObject().field("_id", "1").field("field1", "b").endObject())))
                .execute().actionGet();

        assertThat(response.getResults().get(0).getMatches().size(), is(1));

        logger.info("--> Restarting node again (This will trigger another code-path since translog is flushed)");
        closeNode("node1");
        startNode("node1", extraSettings);
        client = client("node1");
        logger.info("Waiting for cluster health to be yellow");
        waitForYellowStatus(client);

        logger.info("--> Percolate doc with field1=b");
        response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(jsonBuilder().startObject().field("_id", "1").field("field1", "b").endObject())))
                .execute().actionGet();

        assertThat(response.getResults().get(0).getMatches().size(), is(1));
    }
}
