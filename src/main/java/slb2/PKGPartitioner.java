package slb2;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PKGPartitioner implements StreamPartitioner {

    private int numServers;
    private long[] localLoad;

    private Seed seed;
    private HashFunction[] hash;

    private int CHOICES = 2;

    public PKGPartitioner(int numServers) {
        this.numServers = numServers;
        localLoad = new long[numServers];
        seed = new Seed(numServers);
        hash = new HashFunction[CHOICES];

        hash[0] = Hashing.murmur3_128(seed.getSeed(0));
        hash[1] = Hashing.murmur3_128(seed.getSeed(1));
    }

    private int[] selected = new int[CHOICES];

    @Override
    public int partition(Object key) {
        selected[0] = hash[0].hashBytes(key.toString().getBytes()).asInt() % numServers;
        selected[1] = hash[1].hashBytes(key.toString().getBytes()).asInt() % numServers;
        return selectMinLoad();
    }

    private int selectMinLoad() {
        return localLoad[selected[0]] < localLoad[selected[1]] ? selected[0] : selected[1];
    }

}
