package slb2;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import util.cardinality.Hash;
import util.cardinality.MurmurHash;

public class PKG_Partitioner implements StreamPartitioner {

    private int numServers;
    private long[] localLoad;

    private Seed seed;
    private HashFunction[] hashes;

//    private Hash[] hashes;
    private int CHOICES = 2;

    public PKG_Partitioner(int numServers) {
        this.numServers = numServers;
        localLoad = new long[numServers];
        seed = new Seed(numServers);
        hashes = new HashFunction[CHOICES];

//        hashes = new Hash[CHOICES];
//        hashes[0] = MurmurHash.getInstance();
//        hashes[1] = MurmurHash.getInstance();

        hashes[0] = Hashing.murmur3_128(seed.getSeed(0));
        hashes[1] = Hashing.murmur3_128(seed.getSeed(1));
    }

    private int[] selected = new int[CHOICES];

    @Override
    public int partition(Object key) {
        selected[0] = Math.abs(hashes[0].hashBytes(key.toString().getBytes()).asInt() % numServers);
        selected[1] = Math.abs(hashes[1].hashBytes(key.toString().getBytes()).asInt() % numServers);
//        selected[0] = Math.abs(hashes[0].hash(key) % numServers);
//        selected[1] = Math.abs(hashes[0].hash(key) % numServers);
        return chooseMinLoad();
    }

    private int chooseMinLoad() {
        int chosen = localLoad[selected[0]] < localLoad[selected[1]] ? selected[0] : selected[1];
        localLoad[chosen]++;
        return chosen;
    }

    @Override
    public String getName() {
        return "PKG";
    }
}
