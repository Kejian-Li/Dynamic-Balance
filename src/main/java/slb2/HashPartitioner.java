package slb2;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashPartitioner implements StreamPartitioner {

    private int numServers;

    private HashFunction hash;

    public HashPartitioner(int numServers) {
        this.numServers = numServers;
        this.hash = Hashing.murmur3_128(13);
    }

    @Override
    public int partition(Object key) throws Exception {
//        return Math.abs(MurmurHash.getInstance().hash(key) % numServers);
        return Math.abs(hash.hashBytes(key.toString().getBytes()).asInt() % numServers);
    }

    @Override
    public String getName() {
        return "Hash";
    }
}
