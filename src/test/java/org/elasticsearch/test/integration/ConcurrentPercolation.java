package org.elasticsearch.test.integration;

import com.meltwater.elasticsearch.action.BatchPercolateRequestBuilder;
import com.meltwater.elasticsearch.action.BatchPercolateResponse;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class ConcurrentPercolation extends AbstractNodesTests {

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
    public void testSimpleConcurrentPercolator() throws Exception {
        // We need to index a document / define mapping, otherwise field1 doesn't get reconized as number field.
        // If we don't do this, then 'test2' percolate query gets parsed as a TermQuery and not a RangeQuery.
        // The percolate api doesn't parse the doc if no queries have registered, so it can't lazily create a mapping
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.admin().indices().prepareCreate("index").addMapping("type", "field1", "type=long", "field2", "type=string"); // random # shards better has a mapping!
        ensureGreen(client);

        final BatchPercolateSourceBuilder onlyField1 = new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                jsonBuilder()
                        .startObject()
                        .field("_id", "id1")
                        .field("field1", 1)
                        .endObject()
        ));
        final BatchPercolateSourceBuilder onlyField2 = new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                jsonBuilder()
                        .startObject()
                        .field("_id", "id1")
                        .field("field2", "value")
                        .endObject()
        ));
        final BatchPercolateSourceBuilder bothFields = new BatchPercolateSourceBuilder().addDoc(docBuilder().setDoc(
                jsonBuilder()
                        .startObject()
                        .field("_id", "id1")
                        .field("field1", 1)
                        .field("field2", "value")
                        .endObject()
        ));

        client.prepareIndex("index", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .field("field1", 1)
                .field("field2", "value")
                .endObject()).execute().actionGet();

        client.prepareIndex("index", BatchPercolatorService.TYPE_NAME, "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();
        client.prepareIndex("index", BatchPercolatorService.TYPE_NAME, "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .setRefresh(true)
                .execute().actionGet();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger counts = new AtomicInteger(0);
        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        Thread[] threads = new Thread[5];

        for (int i = 0; i < threads.length; i++) {
            Runnable r = percolationRunnable(onlyField1, onlyField2,
                    bothFields, start, stop, counts, exceptionHolder);
            threads[i] = new Thread(r);
            threads[i].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        Throwable assertionError = exceptionHolder.get();
        if (assertionError != null) {
            assertionError.printStackTrace();
        }
        assertThat(assertionError + " should be null", assertionError, nullValue());
    }

    @Test
    public void testConcurrentAddingAndRemovingWhilePercolating() throws Exception {
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        ensureGreen(client);

        client.prepareIndex("index", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .field("field1", "dummyvalue")
                .endObject()).execute().actionGet();


        final int numIndexThreads = 3;
        final int numberPercolateOperation = 100;
        final Random random = new Random();

        final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
        final AtomicInteger idGen = new AtomicInteger(0);
        final Set<String> liveIds = ConcurrentCollections.newConcurrentSet();
        final AtomicBoolean run = new AtomicBoolean(true);
        Thread[] indexThreads = new Thread[numIndexThreads];
        final Semaphore semaphore = new Semaphore(numIndexThreads, true);
        for (int i = 0; i < indexThreads.length; i++) {
            Runnable r = indexingRunnable(random, exceptionHolder, idGen,
                    liveIds, run, semaphore);
            indexThreads[i] = new Thread(r);
            indexThreads[i].start();
        }

        BatchPercolateSourceBuilder source = new BatchPercolateSourceBuilder().addDoc(
                docBuilder().setDoc(jsonBuilder()
                        .startObject()
                        .field("_id", "id1")
                        .field("field1", "value")
                        .endObject())
        );

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int counter = 0; counter < numberPercolateOperation; counter++) {
            Thread.sleep(5);
            semaphore.acquire(numIndexThreads);
            try {
                if (!run.get()) {
                    break;
                }
                int atLeastExpected = liveIds.size();
                BatchPercolateResponse response = new BatchPercolateRequestBuilder(client).setIndices("index").setDocumentType("type")
                        .setSource(source).execute().actionGet();
                assertThat(response.getShardFailures(), emptyArray());
                assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                assertThat(response.getResults().get(0).getMatches().size(), equalTo(atLeastExpected));

            } finally {
                semaphore.release(numIndexThreads);
            }
        }
        run.set(false);
        for (Thread thread : indexThreads) {
            thread.join();
        }
        assertThat("exceptionHolder should have been empty, but holds: " + exceptionHolder.toString(), exceptionHolder.get(), nullValue());
    }

    private Runnable percolationRunnable(final BatchPercolateSourceBuilder onlyField1,
                                         final BatchPercolateSourceBuilder onlyField2,
                                         final BatchPercolateSourceBuilder bothFields,
                                         final CountDownLatch start,
                                         final AtomicBoolean stop,
                                         final AtomicInteger counts,
                                         final AtomicReference<Throwable> exceptionHolder) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    start.await();
                    while (!stop.get()) {
                        int count = counts.incrementAndGet();
                        if ((count > 10000)) {
                            stop.set(true);
                        }
                        BatchPercolateResponse percolate;
                        if (count % 3 == 0) {
                            percolate = new BatchPercolateRequestBuilder(client).setIndices("index").setDocumentType("type")
                                    .setSource(bothFields)
                                    .execute().actionGet();
                            assertThat(percolate.getResults().get(0).getMatches().size(), is(2));
                            assertThat(percolate.getResults().get(0).getMatches().keySet(), hasItems("test1", "test2"));
                        } else if (count % 3 == 1) {
                            percolate = new BatchPercolateRequestBuilder(client).setIndices("index").setDocumentType("type")
                                    .setSource(onlyField2)
                                    .execute().actionGet();

                            assertThat(percolate.getResults().get(0).getMatches().size(), is(1));
                            assertThat(percolate.getResults().get(0).getMatches().keySet(), hasItems("test1"));
                        } else {
                            percolate = new BatchPercolateRequestBuilder(client).setIndices("index").setDocumentType("type")
                                    .setSource(onlyField1)
                                    .execute().actionGet();
                            assertThat(percolate.getResults().get(0).getMatches().size(), is(1));
                            assertThat(percolate.getResults().get(0).getMatches().keySet(), hasItems("test2"));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    exceptionHolder.set(e);
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    private Runnable indexingRunnable(final Random random,
                                      final AtomicReference<Throwable> exceptionHolder,
                                      final AtomicInteger idGen,
                                      final Set<String> liveIds,
                                      final AtomicBoolean run,
                                      final Semaphore semaphore) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                            .field("query", termQuery("field1", "value")).endObject();
                    outer:
                    while (run.get()) {
                        semaphore.acquire();
                        try {
                            if (!liveIds.isEmpty() && random.nextInt(100) < 19) {
                                String id;
                                do {
                                    if (liveIds.isEmpty()) {
                                        continue outer;
                                    }
                                    id = Integer.toString(random.nextInt(idGen.get()));
                                } while (!liveIds.remove(id));

                                DeleteResponse response = client.prepareDelete("index", BatchPercolatorService.TYPE_NAME, id)
                                        .execute().actionGet();
                                assertThat(response.getId(), equalTo(id));
                                assertThat("doc[" + id + "] should have been deleted, but isn't", response.isFound(), equalTo(true));
                            } else {
                                String id = Integer.toString(idGen.getAndIncrement());
                                IndexResponse response = client.prepareIndex("index", BatchPercolatorService.TYPE_NAME, id)
                                        .setSource(doc)
                                        .execute().actionGet();
                                liveIds.add(id);
                                assertThat(response.isCreated(), equalTo(true)); // We only add new docs
                                assertThat(response.getId(), equalTo(id));
                            }
                        } finally {
                            semaphore.release();
                        }
                    }
                } catch (InterruptedException iex) {
                    logger.error("indexing thread was interrupted...", iex);
                    run.set(false);
                } catch (Throwable t) {
                    run.set(false);
                    exceptionHolder.set(t);
                    logger.error("Error in indexing thread...", t);
                }
            }
        };
    }
}
