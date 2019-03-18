package in.ds256.Assignment1.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class sccVertexWritable implements Writable {

    private List<Integer> parents = new ArrayList<>();
    private int value = Integer.MIN_VALUE;
    private boolean active = true;

    public sccVertexWritable() {
    }

    public List<Integer> getParents() {
        return parents;
    }

    public void addParent(int id) {
        parents.add(id);
    }

    public void clearParents() {
        parents.clear();
    }

    public int getValue() {
        return value;
    }

    public void setValue(int v) {
        value = v;
    }

    public boolean isActive() {
        return active;
    }

    public void makeInactive() {
        active = false;
    }

	@Override
	public void write(DataOutput out) throws IOException {
        out.writeInt(value);
        out.writeInt(parents.size());
        if (parents.size() != 0) {
            for (int id : parents) {
                out.writeInt(id);
            }
        }
        out.writeBoolean(active);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
        value = in.readInt();
        int size = in.readInt();
        parents.clear();
        for (int i = 0; i < size; i++) {
            addParent(in.readInt());
        }
        active = in.readBoolean();
	}

}