package com.huawei.storm.hcc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class SearchCarSumPerMinuteBolt extends BaseWindowedBolt {
  private static final long serialVersionUID = 668785327917400661L;
  private static final Logger LOGGER = LoggerFactory.getLogger(SearchCarSumPerMinuteBolt.class);
  private OutputCollector collector;

  private Map<String, Integer> map;
  private long lastTimeMillis = 0L;
  private long rate = 0l;

  public SearchCarSumPerMinuteBolt(long rate) {
    this.rate = rate;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    map = new HashMap<String, Integer>();
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    long currentTimeMillis = System.currentTimeMillis();
    if (lastTimeMillis == 0) {
      lastTimeMillis = currentTimeMillis;
      return;
    }

    if (currentTimeMillis - lastTimeMillis > (rate * 1000)) {
      lastTimeMillis = currentTimeMillis;
      List<Tuple> tuplesInWindow = inputWindow.get();

      String bayonet = null;
      Integer count = 0;
      for (Tuple tupleInWindow : tuplesInWindow) {
        bayonet = (String) tupleInWindow.getStringByField("bayonet");
        count = map.get(bayonet);
        if (count == null) {
          count = 0;
        }
        count++;
        map.put(bayonet, count);
      }
      List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(
              map.entrySet());
      Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
        public int compare(Map.Entry<String, Integer> o1,
                           Map.Entry<String, Integer> o2) {
          return (o2.getValue() - o1.getValue());
        }
      });
      StringBuilder builder = new StringBuilder("");
      for (Entry<String, Integer> entry : list) {
        if (builder.toString() == "" || builder.toString().isEmpty()) {
          builder = builder.append(entry.getKey() + "," + entry.getValue());
        } else {
          builder.append("\n").append(entry.getKey() + "," + entry.getValue());
        }
      }
      LOGGER.info("**************************SearchCarSumPerMinuteBolt emit:**********" + builder.toString() + "***************************");
      collector.emit(new Values("key", builder.toString()));
      LOGGER.info("**************************SearchCarSumPerMinuteBolt emit success*************************************");
      map.clear();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "message"));
  }
}