package slb2;

import com.google.common.hash.Hashing;

public class HashPartitioner implements StreamPartitioner {

    private int numServers;

    public HashPartitioner(int numServers) {
        this.numServers = numServers;
    }

    @Override
    public int partition(Object key) throws Exception {
        return Math.abs(Hashing.murmur3_128(13).hashBytes(key.toString().getBytes()).asInt() % numServers);
    }

}
