package in.ds256.Assignment1.giraph;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.LongWritable;

public class maxCombiner implements MessageCombiner<LongWritable, LongWritable> {

	@Override
	public LongWritable createInitialMessage() {
		return new LongWritable(Long.MIN_VALUE);
	}
	
	@Override
	public void combine(LongWritable vid, LongWritable curr_msg, LongWritable msg_to_combine) {
		if (curr_msg.get() < msg_to_combine.get()) {
			curr_msg.set(msg_to_combine.get());
		}
	}

}