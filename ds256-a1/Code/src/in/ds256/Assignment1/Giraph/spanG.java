package in.ds256.Assignment1.Giraph;

import in.ds256.Assignment1.Giraph.payLoad.LongTupleWritable;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

public class spanG extends BasicComputation<LongWritable, LongTupleWritable, NullWritable, LongTupleWritable> {

    @Override
    public void compute(Vertex<LongWritable, LongTupleWritable, NullWritable> vertex, Iterable<LongTupleWritable> messages) throws IOException {

        if(vertex.getId().get() == -1L) {  // Vertex -1 is used to override faulty edge entries like comments etc.
            vertex.voteToHalt();
        }
        else if (getSuperstep() == 0) { // Used to add target vertices
            LongWritable[] val = { vertex.getId(),  vertex.getId()};
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(),  new LongTupleWritable(val));
            }
        }
        else if (getSuperstep() == 1) {

            int replicate = Integer.parseInt(getContext().getConfiguration().get("replicate"));

            if(replicate==1) {
                for (LongTupleWritable message : messages) {
                    vertex.addEdge(EdgeFactory.create((LongWritable) message.get()[0])); // Add reverse edges
                }
            }
        }
        else if (getSuperstep() == 2) {

            long sourceId = Long.parseLong(getContext().getConfiguration().get("sourceId"));

            // [Distance, Parent]
            LongWritable[] val = {new LongWritable(sourceId == vertex.getId().get() ? 0: Long.MAX_VALUE), new LongWritable(-1)}; // Init vertices

            vertex.setValue(new LongTupleWritable(val));

            if (val[0].get() < Long.MAX_VALUE) {
                LongWritable[] new_val = {new LongWritable(val[0].get()), vertex.getId()};
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new LongTupleWritable(new_val));
                }
            }

            vertex.voteToHalt();

        } else {

            long dis = ((LongWritable) vertex.getValue().get()[0]).get();
            long parent = ((LongWritable) vertex.getValue().get()[1]).get();
            boolean isChanged = false;

            for (LongTupleWritable message : messages) {
                if (((LongWritable) message.get()[0]).get() + 1 < dis) { // Parse messages
                    dis = ((LongWritable) message.get()[0]).get() + 1;
                    parent = ((LongWritable) message.get()[1]).get();
                    isChanged = true;
                }
            }

            if (isChanged) {

                LongWritable[] val = {new LongWritable(dis), new LongWritable(parent)};
                vertex.setValue(new LongTupleWritable(val));
                LongWritable[] new_val = {new LongWritable(val[0].get()), vertex.getId()};

                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), new LongTupleWritable(new_val)); // Send updated value
                }

            }

            vertex.voteToHalt();

        }

    }

}
