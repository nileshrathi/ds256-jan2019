package in.ds256.Assignment1.giraph;


import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class pr extends BasicComputation<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

  private static final Double CONVERGENCE_THRESHOLD = 0.001d;
   
  @Override
  public void compute(Vertex<IntWritable, DoubleWritable, NullWritable> vertex, 
      Iterable<DoubleWritable> messages) throws IOException {
    if(getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(1.0d / getTotalNumVertices()));
    }
    double change = 1d;
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      double old_value = vertex.getValue().get();
      double new_value = 0.15d / getTotalNumVertices() + 0.85d * sum;
      vertex.setValue(new DoubleWritable(new_value));
      change = Math.abs(old_value - new_value);
    }
    if (change < CONVERGENCE_THRESHOLD) {
      vertex.voteToHalt();
    }
    else {
      int numEdges = vertex.getNumEdges();
      DoubleWritable message = new DoubleWritable(vertex.getValue().get() / numEdges);
      for (Edge<IntWritable, NullWritable> edge: vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), message);
      }
    }
  } 
}
