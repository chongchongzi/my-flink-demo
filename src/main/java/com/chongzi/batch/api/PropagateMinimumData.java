package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class PropagateMinimumData {
    /**
     * 顶点
     */
    public static final long[] VERTICES  = new long[] {
            1, 2, 3, 4, 5, 6, 7 };

    public static DataSet<Long> getDefaultVertexDataSet(ExecutionEnvironment env) {
        List<Long> verticesList = new LinkedList<Long>();
        for (long vertexId : VERTICES) {
            verticesList.add(vertexId);
        }
        return env.fromCollection(verticesList);
    }

    /**
     * 边
     */
    public static final Object[][] EDGES = new Object[][] {
            new Object[]{1L, 2L},
            new Object[]{2L, 3L},
            new Object[]{2L, 4L},
            new Object[]{3L, 4L},
            new Object[]{5L, 6L},
            new Object[]{5L, 7L},
            new Object[]{6L, 7L}
    };

    public static DataSet<Tuple2<Long, Long>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Tuple2<Long, Long>> edgeList = new LinkedList<Tuple2<Long, Long>>();
        for (Object[] edge : EDGES) {
            edgeList.add(new Tuple2<Long, Long>((Long) edge[0], (Long) edge[1]));
        }
        return env.fromCollection(edgeList);
    }
}
