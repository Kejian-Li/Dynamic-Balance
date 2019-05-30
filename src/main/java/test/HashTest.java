package test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashTest {


    public static void main(String[] args) {

        HashFunction hash_1 = Hashing.murmur3_128(13);
//        HashFunction hash_2 = Hashing.murmur3_128(13);

        String key = "hello, world.....aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        int numServers = 10;

        int x = hash_1.hashBytes(key.getBytes()).asInt() % numServers;
        int y = hash_1.hashBytes(key.getBytes()).asInt() % numServers;
        System.out.print(x + "  " + y);
    }
}
