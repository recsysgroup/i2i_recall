package match.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

@SuppressWarnings("deprecation")
public abstract class I2ICommonEvaluator extends GenericUDAFEvaluator {
    private static final Log LOG = LogFactory.getLog(I2ICommonEvaluator.class.getName());

    private ListObjectInspector inputItemsOI;
    private PrimitiveObjectInspector inputUvOI;

    // input For merge()
    StructObjectInspector soi;
    StructField uvField;
    StructField itemsField;
    LongObjectInspector uvFieldOI;
    StandardListObjectInspector itemsFieldOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
        super.init(m, parameters);

        LOG.error(m.toString() + ":" + m.name() + ":" + parameters[0].getClass());

        if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
            inputItemsOI = (ListObjectInspector) parameters[0];
            inputUvOI = (PrimitiveObjectInspector) parameters[1];
            /*
             * 构造Struct的OI实例，用于设定聚合结果数组的类型
             * 需要字段名List和字段类型List作为参数来构造
             */
        } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
            soi = (StructObjectInspector) parameters[0];
            uvField = soi.getStructFieldRef("uv");
            itemsField = soi.getStructFieldRef("items");
            //数组中的每个数据，需要其各自的基本类型OI实例解析
            uvFieldOI = (LongObjectInspector) uvField.getFieldObjectInspector();
            itemsFieldOI = (StandardListObjectInspector) itemsField.getFieldObjectInspector();
            inputItemsOI = (StandardListObjectInspector) itemsFieldOI.getListElementObjectInspector();
            LOG.error(uvFieldOI);
            LOG.error(itemsFieldOI);
        }

        // output
        if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
            ArrayList<String> fname = new ArrayList<String>();
            fname.add("items");
            fname.add("uv");
            ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
            foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector
                    )
            ));
            foi.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
        } else {
            return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING);
        }
    }


    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        CommonVO.ArrayAggregationBuffer ret = new CommonVO.ArrayAggregationBuffer();
        reset(ret);
        return ret;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ((CommonVO.ArrayAggregationBuffer) agg).container = new ArrayList<List<Object>>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] param)
            throws HiveException {
        if (param.length == 2) {
            CommonVO.ArrayAggregationBuffer myAgg = (CommonVO.ArrayAggregationBuffer) agg;
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param[0], this.inputItemsOI);
            myAgg.container.add((List<Object>) pCopy);
            myAgg.uv = PrimitiveObjectInspectorUtils.getLong(param[1], this.inputUvOI);
        }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
            throws HiveException {
        CommonVO.ArrayAggregationBuffer myAgg = (CommonVO.ArrayAggregationBuffer) agg;

        Object partialUv = soi.getStructFieldData(partial, uvField);
        Object partialItems = soi.getStructFieldData(partial, itemsField);
        //通过基本数据类型的OI实例解析Object的值

        myAgg.uv = uvFieldOI.get(partialUv);

        List<Object> list = (List<Object>) itemsFieldOI.getList(partialItems);

        for (Object param : list) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, inputItemsOI);
            myAgg.container.add((List<Object>) pCopy);
        }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
            throws HiveException {
        CommonVO.ArrayAggregationBuffer myAgg = (CommonVO.ArrayAggregationBuffer) agg;
        ArrayList<List<Object>> list = new ArrayList<List<Object>>();
        list.addAll(myAgg.container);
        return new Object[]{list, myAgg.uv};
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        CommonVO.ArrayAggregationBuffer myAgg = (CommonVO.ArrayAggregationBuffer) agg;


        List<CommonVO.ItemScoreNode> result = this.i2iAlgo(myAgg);

        if (!result.isEmpty()){
            DecimalFormat df = new DecimalFormat("0.00000");
            df.setRoundingMode(RoundingMode.HALF_UP);
            StringBuilder sb = new StringBuilder();
            for (int index = 0; index < result.size(); index++) {
                sb.append(",")
                        .append(result.get(index).itemId)
                        .append(":")
                        .append(df.format(result.get(index).score));
            }
            return sb.substring(1).toString();
        }
        return null;
    }

    public abstract List<CommonVO.ItemScoreNode> i2iAlgo(CommonVO.ArrayAggregationBuffer myAgg);

}