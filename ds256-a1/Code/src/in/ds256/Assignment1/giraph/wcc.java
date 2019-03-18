package in.ds256.Assignment1.giraph;


import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class wcc extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
   
  @Override
  public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, 
      Iterable<IntWritable> messages) throws IOException {
    if(getSuperstep() == 0) {
      vertex.setValue(vertex.getId());
      for (Edge<IntWritable, NullWritable> edge: vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getValue());
      }
    }
    if (getSuperstep() >= 1) {
      int new_value = vertex.getValue().get();
      boolean changed = false;
      for (IntWritable message : messages) {
        if (message.get() > new_value) {
          new_value = message.get();
          changed = true;
        }
      }
      if(changed) {
        vertex.setValue(new IntWritable(new_value));
        for (Edge<IntWritable, NullWritable> edge: vertex.getEdges()) {
          sendMessage(edge.getTargetVertexId(), vertex.getValue());
        }
      } 
    }
    vertex.voteToHalt();
  } 
}