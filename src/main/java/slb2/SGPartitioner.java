package slb2;

import java.util.Random;

public class SGPartitioner implements StreamPartitioner {

    private int numServers;
    private int nextIndex;

    public SGPartitioner(int numSevers) {
        this.numServers = numSevers;
        nextIndex = new Random().nextInt(numSevers);
    }

    @Override
    public int partition(Object key) throws Exception {  // round-robin for all keys
        return ++nextIndex % numServers;
    }

}
