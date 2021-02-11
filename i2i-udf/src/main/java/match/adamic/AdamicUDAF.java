package match.adamic;

import match.common.CommonVO;
import match.common.I2ICommonEvaluator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.util.*;

@Description(
        name = "collect",
        value = "_FUNC_(col) - The parameter is a column name. "
                + "The return value is a set of the column.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) from src;"
)
public class AdamicUDAF extends AbstractGenericUDAFResolver {
    private static final Log LOG = LogFactory.getLog(AdamicUDAF.class.getName());

    public AdamicUDAF() {
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

        return new AdamicEvaluator();
    }

    public static class AdamicEvaluator extends I2ICommonEvaluator {

        @Override
        public List<CommonVO.ItemScoreNode> i2iAlgo(CommonVO.ArrayAggregationBuffer myAgg) {
            List<List<CommonVO.ItemUvNode>> buffer = new ArrayList<List<CommonVO.ItemUvNode>>();
            for (Object o : myAgg.container) {
                List<Text> itemNeighborsOri = (List<Text>) o;
                List<CommonVO.ItemUvNode> itemNeighbors = new ArrayList<CommonVO.ItemUvNode>();
                for (Text txt : itemNeighborsOri) {
                    String str = txt.toString();
                    CommonVO.ItemUvNode node = new CommonVO.ItemUvNode();
                    node.itemId = str.split(":")[0]
                    ;
                    node.uv = Long.parseLong(str.split(":")[1]);
                    itemNeighbors.add(node);
                }
                buffer.add(itemNeighbors);
            }

            List<CommonVO.ItemScoreNode> result = itemCF(buffer, myAgg.uv);

            return result;
        }

        private List<CommonVO.ItemScoreNode> itemCF(List<List<CommonVO.ItemUvNode>> buffer, long uv) {
            int topk = 100;
            Map<String, Double> statMap = new HashMap<String, Double>();
            for (List<CommonVO.ItemUvNode> list : buffer) {
                for (CommonVO.ItemUvNode node : list) {
                    statMap.put(node.itemId,
                            1.0 / Math.log(2.0 + list.size()) +
                                    statMap.getOrDefault(node.itemId, 0.0));
                }
            }
            PriorityQueue<CommonVO.ItemScoreNode> queue = new PriorityQueue<CommonVO.ItemScoreNode>(topk);
            for (Map.Entry<String, Double> entry : statMap.entrySet()) {
                if (queue.size() < topk) {
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
            List<CommonVO.ItemScoreNode> result = new ArrayList<>();
            while (!queue.isEmpty()) {
                result.add(queue.remove());
            }
            Collections.reverse(result);
            return result;
        }
    }


}

