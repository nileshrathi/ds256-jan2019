package in.ds256.Assignment1.Giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class wccG extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {


        if(vertex.getId().get() == -1L) { // Vertex -1 is used to override faulty edge entries like comments etc.
            vertex.voteToHalt();
        }
        else if (getSuperstep() == 0) {
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) { // Used to add target vertices
                sendMessage(edge.getTargetVertexId(), vertex.getId());
            }
        }
        else if (getSuperstep() == 1) {
            vertex.setValue(vertex.getId());

            int replicate = Integer.parseInt(getContext().getConfiguration().get("replicate"));

            if(replicate==1) {
                for (LongWritable message : messages) {
                    vertex.addEdge(EdgeFactory.create(new LongWritable(message.get()))); // Add reverse edges
                }
            }
        }
        else if (getSuperstep() == 2) {

            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), vertex.getId()); // Send my ID
            }

            vertex.voteToHalt();
        }
        else {
            long maxValue = vertex.getValue().get();

            for (LongWritable message : messages) {
                maxValue = Math.max(maxValue, message.get());
            }

            if (maxValue > vertex.getValue().get()) {
                vertex.setValue(new LongWritable(maxValue));

                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new LongWritable(maxValue)); // Send message if I am changed
                }
            }

            vertex.voteToHalt(); // Halt by default
        }

    }
}