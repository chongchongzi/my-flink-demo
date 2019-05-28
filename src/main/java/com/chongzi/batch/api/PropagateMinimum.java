package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *
 * Connected Components即连通体算法用id标注图中每个连通体，将连通体中序号最小的顶点的id作为连通体的id，联通体ID相等的顶点组成一个联通体
 * 特例：initialSolutionSet和initialWorkset相同
 */
public class PropagateMinimum {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final int maxIterations = 5;

        // read vertex and edge data
        DataSet<Long> vertices = PropagateMinimumData.getDefaultVertexDataSet(env);

        //单向边--》双向边
        DataSet<Tuple2<Long, Long>> edges = PropagateMinimumData.getDefaultEdgeDataSet(env).flatMap(new UndirectEdge());


        //为每个顶点分配唯一的ID作为初始ID也就是解集(等于顶点id)，也是initialSolutionSet，同时也是initialWorkset
        DataSet<Tuple2<Long, Long>> initialSolutionSet =
                vertices.map(new DuplicateValue<Long>());

        DataSet<Tuple2<Long, Long>> initialWorkset =
                vertices.map(new DuplicateValue<Long>());

        //open a delta iteration
        //在initialSolutionSet上调用iterateDelta（initialWorkset，maxIterations，key）
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                initialSolutionSet.iterateDelta(initialWorkset, maxIterations, 0);

        // apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
        //step函数有两个参数：（solutionSet，workset），并且必须返回两个值：（solutionSetDelta，newWorkset）

        //在每一迭代中，一个顶点选择它自己的ID和它邻居的ID的最小值作为它的新Component-ID，并告诉它的邻居它的新Component-ID,迭代完成之后，同一Component中的所有顶点具有相同的Component-ID。
        DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
                .groupBy(0).aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new ComponentIdFilter());

        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

        result.print();

    }

    /**
     * 单向边转双向边，方便找到每个顶点的相邻顶点
     */
    public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

        @Override
        public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
            invertedEdge.f0 = edge.f1;
            invertedEdge.f1 = edge.f0;
            out.collect(edge);
            out.collect(invertedEdge);
        }
    }

    public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

        @Override
        public Tuple2<T, T> map(T vertex) {
            return new Tuple2<T, T>(vertex, vertex);
        }
    }

    public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
            return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
        }
    }

    public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
            //等于就结束
            if (candidate.f1 < old.f1) {
                out.collect(candidate);
            }
        }
    }
}
