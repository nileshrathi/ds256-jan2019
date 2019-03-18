package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class span extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
	
	public static final IntConfOption SOURCE_ID = new IntConfOption("span.sourceId", 1, "Source of Spanning tree");

	private boolean isSource(Vertex<IntWritable, ?, ?> vertex) {
		return vertex.getId().get() == SOURCE_ID.get(getConf());
	}

	@Override
	public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages)
			throws IOException {
		
		if (getSuperstep() == 0) {
            vertex.setValue(new IntWritable(-1));
            if(isSource(vertex)) {
                for (Edge<IntWritable, NullWritable> edge: vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), vertex.getId());
                }
            }
        }
        
        if (!isSource(vertex) && vertex.getValue().get() == -1) {
            for (IntWritable message : messages) {
                vertex.setValue(message);
                break;
            }
            if (vertex.getValue().get() != -1) {
                for (Edge<IntWritable, NullWritable> edge: vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), vertex.getId());
                }
            }
        }

		vertex.voteToHalt();
	}
}
