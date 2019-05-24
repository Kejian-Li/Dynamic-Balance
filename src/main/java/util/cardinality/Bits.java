package util.cardinality;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class Bits {

    public static int[] getBits(byte[] mBytes) throws IOException {
        int bitSize = mBytes.length / 4;
        int[] bits = new int[bitSize];
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(mBytes));
        for (int i = 0; i < bitSize; i++) {
            bits[i] = dis.readInt();
        }
        return bits;
    }

}
