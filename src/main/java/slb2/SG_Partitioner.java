package slb2;

import java.util.Random;

public class SG_Partitioner implements StreamPartitioner {

    private int numServers;
    private int nextIndex;

    public SG_Partitioner(int numSevers) {
        this.numServers = numSevers;
        nextIndex = new Random().nextInt(numSevers);
    }

    @Override
    public int partition(Object key) throws Exception {  // round-robin for all keys
        return ++nextIndex % numServers;
    }

}
