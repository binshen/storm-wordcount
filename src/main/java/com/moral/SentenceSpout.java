package com.moral;

import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;

public class SentenceSpout extends BaseRichSpout{

    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector outputCollector;

    private static final String[] sentences={
        "my doc has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "i don't think i like fleas"
    };

    private int index = 0;

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    //核心逻辑
    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.outputCollector.emit(values, msgId);
        index++;
        if(index >= sentences.length){
            index = 0;
        }
        Utils.sleep(1);
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentences"));
    }

    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.outputCollector.emit(this.pending.get(msgId), msgId);
    }
}