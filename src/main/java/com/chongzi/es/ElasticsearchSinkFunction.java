package com.chongzi.es;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public interface ElasticsearchSinkFunction<T> extends Serializable, Function {
    void process(T element, RuntimeContext ctx, RequestIndexer indexer);
}