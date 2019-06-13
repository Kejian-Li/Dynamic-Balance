package test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import util.cardinality.MurmurHash;

public class HashTest {


    public static void main(String[] args) {

//        HashFunction hash_1 = Hashing.murmur3_128(13);
////        HashFunction hash_2 = Hashing.murmur3_128(13);
//
//        String key = "hello, world.....aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
//
//        int numServers = 10;
//
//        int x = hash_1.hashBytes(key.getBytes()).asInt() % numServers;
//        int y = hash_1.hashBytes(key.getBytes()).asInt() % numServers;
//        System.out.print(x + "  " + y);

        int z = Integer.parseInt("1");

        int x1 = MurmurHash.getInstance().hash(Integer.parseInt("222222"));
        int x2 = MurmurHash.getInstance().hash(Integer.parseInt("222222"));
        System.out.println(x1 + "  " + x2);
    }
}
