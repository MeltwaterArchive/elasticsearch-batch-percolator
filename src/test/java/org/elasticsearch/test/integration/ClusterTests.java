package org.elasticsearch.test.integration;

import com.meltwater.elasticsearch.action.BatchPercolateRequestBuilder;
import com.meltwater.elasticsearch.action.BatchPercolateResponse;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author pelle.berglund
 */
public class ClusterTests extends AbstractNodesTests {

    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }


    @Test
    public void usingRemoteTransport() throws IOException {
        //Make sure there is only one shard
        Settings extraSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        logger.info("--> start first node!");
        startNode("node1",extraSettings);
        Client client = client("node1");

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1")
                .setSource("field1", "value").execute().actionGet();

        logger.info("--> register a query with highlights");
        client.prepareIndex("test", BatchPercolatorService.TYPE_NAME, "1")
                .setSource(getSource(termQuery("field1", "fox"), new HighlightBuilder().field("field1").preTags("<b>").postTags("</b>")))
                .execute().actionGet();

        //Start a second node
        logger.info("--> start second node");
        startNode("node2",extraSettings);
        client = client("node2");

        //Do percolation request - now the response must be serialized/deserialized
        BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("_id", "docId")
                                .field("field1", "the fox is here")
                                .endObject())))
                .execute().actionGet();

        assertThat(response.getShardFailures().length, is(0));
        assertThat(response.getResults().get(0).getMatches().size(), is(1));

    }

}
