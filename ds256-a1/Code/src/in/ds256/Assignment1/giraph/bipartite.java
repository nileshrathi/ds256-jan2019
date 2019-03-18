package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*class VertexValue implements Writable {
	private boolean matched = false;
	private long matchedVertex = -1;

	public boolean isMatched() {
		return matched;
	}

	public long getMatchedVertex() {
		return matchedVertex;
	}

	public void setMatchedVertex(long matchedVertex) {
		this.matched = true;
		this.matchedVertex = matchedVertex;
	}

}*/

/*public class Message implements Writable {
	private long senderVertex;

	private enum Type {
		MATCH_REQUEST, REQUEST_GRANTED, REQUEST_DENIED
	}

	private Message.Type type = Type.MATCH_REQUEST;

	public Message() {
	}

	public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		senderVertex = vertex.getId().get();
		type = Type.MATCH_REQUEST;
	}

	public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex, boolean isGranting) {
		this(vertex);
		type = isGranting ? Type.REQUEST_GRANTED : Type.REQUEST_DENIED;
	}

	public long getSenderVertex() {
		return senderVertex;
	}

	public boolean isGranting() {
		return type.equals(Type.REQUEST_GRANTED);
	}
}*/
public class bipartite {}
//public class bipartite extends BasicComputation<LongWritable, VertexValue, NullWritable, Message> {
/*	boolean isLeft(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return vertex.getId().get() % 2 == 1;
	}

	boolean isRight(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return !isLeft(vertex);
	}

	private boolean isNotMatchedYet(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return !vertex.getValue().isMatched();
	}

	private Message createRequestMessage(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return new Message(vertex);
	}

	private Message createGrantingMessage(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return new Message(vertex, true);
	}

	private Message createDenyingMessage(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
		return new Message(vertex, false);
	}

	public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex, Iterable<Message> messages)
			throws IOException {
		int phase = (int) (getSuperstep() % 4);
		switch (phase) {
		case 0:
			if (isLeft(vertex)) {
				if (isNotMatchedYet(vertex)) {
					sendMessageToAllEdges(vertex, createRequestMessage(vertex));
					vertex.voteToHalt();
				}
			}
			break;
		case 1: // "In phase 1 of a cycle,"
			// "each right vertex not yet matched"
			if (isRight(vertex)) {
				if (isNotMatchedYet(vertex)) {
					int i = 0;
					for (Message msg : messages) {
						Message reply;
						reply = (i == 0) ? createGrantingMessage(vertex) : createDenyingMessage(vertex);
						sendMessage(new LongWritable(msg.getSenderVertex()), reply);
						++i;
					}
					vertex.voteToHalt();
				}
			}
			break;
		case 2:
			if (isLeft(vertex)) {
				if (isNotMatchedYet(vertex)) {
					for (Message msg : messages) {
						if (msg.isGranting()) {
							sendMessage(new LongWritable(msg.getSenderVertex()), createGrantingMessage(vertex));
							vertex.getValue().setMatchedVertex(msg.getSenderVertex());
							break;
						}
					}
					vertex.voteToHalt();
				}
			}
			break;
		case 3:
			if (isRight(vertex)) {
				if (isNotMatchedYet(vertex)) {
					for (Message msg : messages) {
						vertex.getValue().setMatchedVertex(msg.getSenderVertex());
						break;
					}
					vertex.voteToHalt();
				}
			}
			break;
		}
	}*/
//}
