package in.ds256.Assignment1.Giraph.payLoad;

public class LongPair {
    private long first;
    private long second;

    public LongPair(long fst, long snd) {
        this.first = fst;
        this.second = snd;
    }

    public long getFirst() {
        return this.first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public long getSecond() {
        return this.second;
    }

    public void setSecond(long second) {
        this.second = second;
    }
}
