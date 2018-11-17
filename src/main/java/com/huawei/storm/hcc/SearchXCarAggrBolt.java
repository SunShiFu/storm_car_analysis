package com.huawei.storm.hcc;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.Map;

public class SearchXCarAggrBolt implements IBasicBolt {
  private static final long serialVersionUID = -2741339868952866216L;
  private int day = -1;
  private int count = 0;
  private LinkedList<String> fakeCars = new LinkedList<String>();

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "message"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

    String carNo = tuple.getStringByField("carNo");
    String gate1 = tuple.getStringByField("gate1");
    String gate2 = tuple.getStringByField("gate2");
    String time1 = tuple.getStringByField("time1");
    String time2 = tuple.getStringByField("time2");

    int dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
    if (day != -1 && day != dayOfMonth) {
      count = 0;
    } else {
      count++;
    }
    day = dayOfMonth;

    StringBuilder sb = new StringBuilder(carNo);
    sb.append(",")
            .append(gate1)
            .append(",")
            .append(time1)
            .append(",")
            .append(gate2)
            .append(",")
            .append(time2);

    if (fakeCars.size() > 10) {
      fakeCars.removeFirst();
    }
    fakeCars.addLast(sb.toString());

    StringBuilder emitResult = new StringBuilder();
    for (String item : fakeCars) {
      emitResult.append(item).append("\n");
    }
    emitResult.append(count);

    collector.emit(new Values("key", emitResult.toString()));
  }

  @Override
  public void prepare(Map arg0, TopologyContext arg1) {
  }

}
