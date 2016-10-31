package com.moral;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReportBolt extends BaseRichBolt {

    private static final Logger log = LoggerFactory.getLogger(ReportBolt.class);

    private Map<String, Long> counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        counts.put(word, count);
        //打印更新后的结果
        printReport();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //无下游输出,不需要代码
    }

    //主要用于将结果打印出来,便于观察
    private void printReport(){
        log.info("--------------------------begin-------------------");
        Set<String> words = counts.keySet();
        for(String word : words){
            log.info("@report-bolt@: " + word + " ---> " + counts.get(word));
        }
        log.info("--------------------------end---------------------");
    }

    @Override
    public void cleanup() {
        super.cleanup();

        //集群运行时，此处不一定会被执行到
    }
}