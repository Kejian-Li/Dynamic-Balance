package slb2;

import java.util.Random;

public class SG_Partitioner extends AbstractPartitioner {

    private int numServers;
    private int nextIndex;

    public SG_Partitioner(int numSevers) {
        super();
        this.numServers = numSevers;
        nextIndex = new Random().nextInt(numSevers);
    }

    @Override
    public int partition(Object key) throws Exception {  // round-robin for all keys
        add(key);
        return ++nextIndex % numServers;
    }

    @Override
    public String getName() {
        return "Shuffle";
    }
}
