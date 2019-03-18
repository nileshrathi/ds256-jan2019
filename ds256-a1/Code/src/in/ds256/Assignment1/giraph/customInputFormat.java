package in.ds256.Assignment1.giraph;


import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class customInputFormat extends
    TextEdgeInputFormat<IntWritable, NullWritable> {
  private static final Pattern SEPARATOR = Pattern.compile("[\t | \\\\s* | ,]");

  @Override
  public EdgeReader<IntWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new IntNullTextEdgeReader();
  }

  public class IntNullTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<IntPair> {
    @Override
    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      if(tokens.length == 2) {
      return new IntPair(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
      }
      else
    	  return null;
    	  
    }

    @Override
    protected IntWritable getSourceVertexId(IntPair endpoints)
      throws IOException {
    	if(endpoints == null )
    		return null;
      return new IntWritable(endpoints.getFirst());
    }

    @Override
    protected IntWritable getTargetVertexId(IntPair endpoints)
      throws IOException {
    	if(endpoints == null )
    		return null;
      return new IntWritable(endpoints.getSecond());
    }

    @Override
    protected NullWritable getValue(IntPair endpoints) throws IOException {
      return NullWritable.get();
    }
  }
}
