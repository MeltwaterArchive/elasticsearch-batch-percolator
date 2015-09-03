package com.meltwater.elasticsearch.action;

import com.meltwater.elasticsearch.shard.QueryMatch;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * The percolation result from one document
 */
public class BatchPercolateResponseItem implements Streamable, ToXContent {
    private String docId;
    private Map<String, QueryMatch> matches;

    public BatchPercolateResponseItem(String docId){
        this.matches = Maps.newHashMap();
        this.docId = docId;
    }

    public BatchPercolateResponseItem(){
        this.matches = Maps.newHashMap();
    }

    public BatchPercolateResponseItem(Map<String, QueryMatch> matches, String docId) {
        this.matches = matches;
        this.docId = docId;
    }

    public Map<String, QueryMatch> getMatches() {
        return matches;
    }

    public String getDocId() {
        return docId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        docId = in.readString();
        matches = Maps.newHashMap();
        int mSize = in.readVInt();
        for (int j = 0; j < mSize; j++) {
            QueryMatch queryMatch = new QueryMatch();
            queryMatch.readFrom(in);
            matches.put(in.readString(), queryMatch);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeVInt(matches.size());
        for (Map.Entry<String, QueryMatch> entry : matches.entrySet()){
            entry.getValue().writeTo(out);
            out.writeString(entry.getKey());
        }
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("doc", docId);
        builder.field("matches");
        builder.startArray();
        for (Map.Entry<String, QueryMatch> match : matches.entrySet()) {
            match.getValue().toXContent(builder,params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
