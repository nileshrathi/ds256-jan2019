package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;



public class gpr2 extends BasicComputation<LongWritable,
		DoubleWritable, FloatWritable, DoubleWritable> {
	/** Number of supersteps for this test */
	public static final int MAX_SUPERSTEPS = 10;

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() >= 1) {
			double sum = 0;
			for (DoubleWritable message : messages) {
				sum += message.get();
			}
			DoubleWritable vertexValue =
					new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
			vertex.setValue(vertexValue);
		}

		if (getSuperstep() < MAX_SUPERSTEPS) {
			long edges = vertex.getNumEdges();
			sendMessageToAllEdges(vertex,
					new DoubleWritable(vertex.getValue().get() / edges));
		} else {
			vertex.voteToHalt();
		}
	}
}
