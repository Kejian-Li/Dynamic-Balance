package slb2.partitioners;

public class Seed {

    private int currentPrime = 31;

    private int[] seeds;

    public Seed(int num) {
        seeds = new int[num];
        for (int i = 0; i < num; i++) {
            seeds[i] = currentPrime;
            currentPrime = nextPrime(currentPrime);
        }
    }

    private int nextPrime(int currentPrime) {
        int x = currentPrime + 1;
        while(!isPrime(x)) {
            x++;
        }
        return x;
    }

    private boolean isPrime(int x) {
        for (int i = 2; i < x / 2; i++) {
            if (x % i == 0) {
                return false;
            }
        }
        return true;
    }

    public int getSeed(int index) {
        return seeds[index];
    }


}
