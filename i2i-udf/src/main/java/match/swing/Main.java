package match.swing;

import match.common.CommonVO;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class Main {
    public static void main(String[] args) {
        CommonVO.ItemScoreNode a = new CommonVO.ItemScoreNode();
        a.itemId = "1";
        a.score = 0.8;

        CommonVO.ItemScoreNode b = new CommonVO.ItemScoreNode();
        b.itemId = "2";
        b.score = 0.4;

        PriorityQueue<CommonVO.ItemScoreNode> queue = new PriorityQueue<CommonVO.ItemScoreNode>(2);
        queue.add(b);

        queue.add(a);

        List<CommonVO.ItemScoreNode> tmpDebugList = new ArrayList<>();
        while (!queue.isEmpty()) {
            tmpDebugList.add(queue.remove());
        }
        System.out.println(tmpDebugList);

    }
}
