package in.ds256.Assignment1.Giraph.payLoad;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

@Public
@Stable
public class LongTupleWritable implements Writable {
    private Class<? extends Writable> valueClass;
    private Writable[] values;

    public LongTupleWritable() {
            this.valueClass = LongWritable.class;
    }

    public LongTupleWritable(Writable[] values) {
        this.valueClass = LongWritable.class;
        this.values = values;
    }

    public Class getValueClass() {
        return this.valueClass;
    }

    public String toString() {
        String strings = new String();

        if (this.values == null) {
            return strings;
        }

        for(int i = 0; i < this.values.length; ++i) {
            strings = strings.concat(this.values[i].toString());
            strings = strings.concat(",");
        }

        return strings;
    }

    public Object toArray() {
        Object result = Array.newInstance(this.valueClass, this.values.length);

        for(int i = 0; i < this.values.length; ++i) {
            Array.set(result, i, this.values[i]);
        }

        return result;
    }

    public void set(Writable[] values) {
        this.values = values;
    }

    public Writable[] get() {
        return this.values;
    }

    public void readFields(DataInput in) throws IOException {
        this.values = new Writable[in.readInt()];

        for(int i = 0; i < this.values.length; ++i) {
            Writable value = WritableFactories.newInstance(this.valueClass);
            value.readFields(in);
            this.values[i] = value;
        }

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.values.length);

        for(int i = 0; i < this.values.length; ++i) {
            this.values[i].write(out);
        }

    }
}
