package in.ds256.Assignment1.giraph;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class scc extends BasicComputation<IntWritable, sccVertexWritable, NullWritable, IntWritable> {

    public static final String AGGREGATOR_PHASE = "app.phase";
    public static final String AGGREGATOR_FWD_DONE = "app.fwddone";
    public static final String AGGREGATOR_BWD_DONE = "app.bwddone";

    public static final int PHASE_TRANSPOSE = 0;
    public static final int PHASE_TRIM = 1;
    public static final int PHASE_FWD_TRAVERSAL = 2;
    public static final int PHASE_BWD_TRAVERSAL_START = 3;
    public static final int PHASE_BWD_TRAVERSAL_REST = 4;

    private int phase;

    @Override
    public void compute(Vertex<IntWritable, sccVertexWritable, NullWritable> vertex, Iterable<IntWritable> messages)
            throws IOException {
        sccVertexWritable vertexValue = vertex.getValue();
        if (!vertexValue.isActive()) {
            vertex.voteToHalt();
            return;
        }
        phase = ((IntWritable)getAggregatedValue(AGGREGATOR_PHASE)).get();
        switch(phase) {
            case PHASE_TRANSPOSE:
                vertexValue.clearParents();
                sendMessageToAllEdges(vertex, vertex.getId());
                break;
            case PHASE_TRIM:
                for(IntWritable parent: messages) {
                    vertexValue.addParent(parent.get());
                }
                vertexValue.setValue(vertex.getId().get());
                if(vertex.getNumEdges() == 0 || vertexValue.getParents().size() == 0) {
                    vertexValue.makeInactive();
                } else {
                    sendMessageToAllEdges(vertex, new IntWritable(vertexValue.getValue()));
                }
                break;
            case PHASE_FWD_TRAVERSAL:
                boolean done = true;
                for(IntWritable m : messages) {
                    if (m.get() > vertexValue.getValue()) {
                        vertexValue.setValue(m.get());
                        done = false;
                    }
                }
                if (!done) {
                    sendMessageToAllEdges(vertex, new IntWritable(vertexValue.getValue()));
                    aggregate(AGGREGATOR_FWD_DONE, new BooleanWritable(false));
                }
                break;
            case PHASE_BWD_TRAVERSAL_START:
                if(vertexValue.getValue() == vertex.getId().get()) {
                    for (Integer p : vertexValue.getParents()) {
                        sendMessage(new IntWritable(p), new IntWritable(vertexValue.getValue()));
                    }
                    vertexValue.makeInactive();
                }
                break;
            case PHASE_BWD_TRAVERSAL_REST:
                for(IntWritable m : messages) {
                    if(vertexValue.getValue() == m.get()) {
                        for (Integer p : vertexValue.getParents()) {
                            sendMessage(new IntWritable(p), new IntWritable(vertexValue.getValue()));
                        }
                        aggregate(AGGREGATOR_BWD_DONE, new BooleanWritable(false));
                        vertexValue.makeInactive();
                        vertex.voteToHalt();
                        break;
                    }
                }
                break;
        }
    }

}