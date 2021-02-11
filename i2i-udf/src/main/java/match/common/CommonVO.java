package match.common;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import java.util.List;

public class CommonVO {
    public static class ItemUvNode {
        public String itemId;
        public long uv;

        @Override
        public int hashCode() {
            return itemId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ItemUvNode && itemId != null) {
                return itemId.equals(((ItemUvNode) obj).itemId);
            }
            return false;
        }


        @Override
        public String toString() {
            return itemId + ":" + uv;
        }
    }

    public static class ItemScoreNode implements Comparable {
        public String itemId;
        public double score;


        @Override
        public int compareTo(Object o) {
//            return Double.compare(((ItemScoreNode) o).score, score);
            return Double.compare(score, ((ItemScoreNode) o).score);

        }

        @Override
        public String toString() {
            return itemId + ":" + score;
        }
    }

    public static class ArrayAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
        public List<List<Object>> container;
        public long uv;
    }
}
