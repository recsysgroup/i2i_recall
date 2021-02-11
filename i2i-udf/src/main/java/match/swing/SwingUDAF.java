package match.swing;

import match.common.CommonVO;
import match.common.I2ICommonEvaluator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

@Description(
        name = "collect",
        value = "_FUNC_(col) - The parameter is a column name. "
                + "The return value is a set of the column.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) from src;"
)
public class SwingUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(SwingUDAF.class.getName());

    public SwingUDAF() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        return new SwingEvaluator();
    }

    public static class SwingEvaluator extends I2ICommonEvaluator {

        @Override
        public List<CommonVO.ItemScoreNode> i2iAlgo(CommonVO.ArrayAggregationBuffer myAgg) {
            List<Set<CommonVO.ItemUvNode>> buffer = new ArrayList<Set<CommonVO.ItemUvNode>>();
            Collections.shuffle(myAgg.container);
            final int itemMaxKeep = 500;
            int cnt = 0;
            for (Object o : myAgg.container) {
                if (cnt++ >= itemMaxKeep) break;
                List<Text> itemNeighborsOri = (List<Text>) o;
                Set<CommonVO.ItemUvNode> itemNeighbors = new HashSet<>();
                for (Text txt : itemNeighborsOri) {
                    String str = txt.toString();
                    CommonVO.ItemUvNode node = new CommonVO.ItemUvNode();
                    node.itemId = str.split(":")[0];
                    node.uv = Long.parseLong(str.split(":")[1]);
                    itemNeighbors.add(node);
                }
                buffer.add(itemNeighbors);
            }

            return itemCF(buffer, myAgg.uv);
        }

        private List<CommonVO.ItemScoreNode> itemCF(List<Set<CommonVO.ItemUvNode>> buffer, long uv) {
            int topK = 100;
            Map<String, Double> statMap = new HashMap<String, Double>();
            Set<CommonVO.ItemUvNode> tmpSet = new HashSet<>();
            for (int indexI = 0; indexI < buffer.size(); indexI++) {
                double weightI = Math.pow(buffer.get(indexI).size() + 5, -0.3);
                for (int indexJ = indexI + 1; indexJ < buffer.size(); indexJ++) {
                    double weightJ = Math.pow(buffer.get(indexJ).size() + 5, -0.3);
                    tmpSet.clear();
                    tmpSet.addAll(buffer.get(indexI));
                    tmpSet.retainAll(buffer.get(indexJ));
                    for (CommonVO.ItemUvNode node : tmpSet) {
                        statMap.put(node.itemId,
                                weightI * weightJ / (1.0 + tmpSet.size()) +
                                        statMap.getOrDefault(node.itemId, 0.0));
                    }
                }
            }

            PriorityQueue<CommonVO.ItemScoreNode> queue = new PriorityQueue<CommonVO.ItemScoreNode>(topK);
            for (Map.Entry<String, Double> entry : statMap.entrySet()) {
                if (queue.size() < topK) {
                    CommonVO.ItemScoreNode isNode = new CommonVO.ItemScoreNode();
                    isNode.itemId = entry.getKey();
                    isNode.score = entry.getValue();
                    queue.add(isNode);
                } else {
                    if (entry.getValue() > queue.peek().score) {
                        CommonVO.ItemScoreNode isNode = new CommonVO.ItemScoreNode();
                        isNode.itemId = entry.getKey();
                        isNode.score = entry.getValue();
                        queue.remove();
                        queue.add(isNode);
                    }
                }
            }

            List<CommonVO.ItemScoreNode> result = new ArrayList<>(queue);

            Set<String> swingFilter = new HashSet<String>();
            if (!result.isEmpty()) {
                result.sort(null);
                Collections.reverse(result);
                for (CommonVO.ItemScoreNode node : result) {
                    swingFilter.add(node.itemId);
                }

                double maxScore = result.get(0).score;
                double minScore = result.get(result.size() - 1).score;
                for (CommonVO.ItemScoreNode node : result) {
                    node.score = (node.score - minScore) / (maxScore - minScore + 1e-6) * 0.6 + 0.4;
                }
            }

            // adamic
            if (result.size() < topK) {
                Map<String, Double> adamicMap = new HashMap<String, Double>();
                for (Set<CommonVO.ItemUvNode> list : buffer) {
                    for (CommonVO.ItemUvNode node : list) {
                        adamicMap.put(node.itemId,
                                1.0 / Math.log(2.0 + list.size()) +
                                        adamicMap.getOrDefault(node.itemId, 0.0));
                    }
                }

                PriorityQueue<CommonVO.ItemScoreNode> adamicQueue = new PriorityQueue<CommonVO.ItemScoreNode>(topK);
                for (Map.Entry<String, Double> entry : adamicMap.entrySet()) {
                    if (adamicQueue.size() < topK) {
                        CommonVO.ItemScoreNode isNode = new CommonVO.ItemScoreNode();
                        isNode.itemId = entry.getKey();
                        isNode.score = entry.getValue();
                        adamicQueue.add(isNode);
                    } else {
                        if (entry.getValue() > adamicQueue.peek().score) {
                            CommonVO.ItemScoreNode isNode = new CommonVO.ItemScoreNode();
                            isNode.itemId = entry.getKey();
                            isNode.score = entry.getValue();
                            adamicQueue.remove();
                            adamicQueue.add(isNode);
                        }
                    }
                }

                List<CommonVO.ItemScoreNode> adamicResult = new ArrayList<>(adamicQueue);
                adamicResult.sort(null);
                Collections.reverse(adamicResult);


                double adamicMaxScore = adamicResult.get(0).score;
                double adamicMinScore = adamicResult.get(adamicResult.size() - 1).score;


                for (CommonVO.ItemScoreNode node : adamicResult) {
                    if (swingFilter.contains(node.itemId)) {
                        continue;
                    }
                    node.score = (node.score - adamicMinScore) / (adamicMaxScore - adamicMinScore + 1e-6) * 0.4;
                    result.add(node);
                    if (result.size() >= topK) {
                        break;
                    }
                }
            }

            return result;
        }
    }


}

