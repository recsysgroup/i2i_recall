package match.llr;

public class Main {
    public static void main(String[] args) {

        test1();

    }

    private static void test1() {
        long k11 = 100;
        long k12 = 100;
        long k21 = 100;
        long k22 = 10000;
        double rowEntropy = entropy(k11 + k12, k21 + k22);
        double columnEntropy = entropy(k11 + k21, k12 + k22);
        double matrixEntropy = entropy(k11, k12, k21, k22);
        System.out.println(rowEntropy);
        System.out.println(columnEntropy);
        System.out.println(matrixEntropy);
        System.out.println(2 * (rowEntropy + columnEntropy - matrixEntropy));

    }

    private static double entropy(long... elements) {
        long sum = 0;
        double result = 0.0;
        for (long x : elements) {
            sum += x;
        }
        for (long x : elements) {
            long zeroFlag = (x == 0 ? 1 : 0);
            result += x * Math.log((x + zeroFlag + 0.0) / sum);
        }
        return -result;
    }
}
