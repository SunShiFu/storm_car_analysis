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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchXCarBolt extends BaseWindowedBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(SearchXCarBolt.class);

  private OutputCollector collector;
  private String carNo = null;
  private String time = null;
  //卡口号
  private String new_bayonet = null;
  private String old_bayonet = null;
  //经度
  private double new_longitude;
  private double old_longitude;
  //纬度
  private double new_latitude;
  private double old_latitude;
  //车牌号
  private String carNoInWindow = null;
  private String timeInWindow = null;

  //输入的时间和距离
  private long input_timeString = 0l;
  private double input_distence = 0d;


  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public SearchXCarBolt(long input_timeString, double input_distence) {
    this.input_timeString = input_timeString;
    this.input_distence = input_distence;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }

  private long getTimeDiff(String time, String timeInWindow) {
    //计算时间差
    long timeDiff = 0l;
    try {
      Date new_date = sdf.parse(time);
      Date old_date = sdf.parse(timeInWindow);
      //计算时间差，精确到分钟
      timeDiff = Math.abs((new_date.getTime() - old_date.getTime()) / (1000 * 60));
    } catch (ParseException e) {
      System.out.println(e.getMessage());
    }
    return timeDiff;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    List<Tuple> tuplesInWindow = inputWindow.get();
    List<Tuple> newTuples = inputWindow.getNew();

    Map<String, String> fakeCar = new HashMap<String, String>();

    for (Tuple tuple : newTuples) {
      carNo = tuple.getStringByField("carNo");
      for (Tuple tupleInWindow : tuplesInWindow) {
        carNoInWindow = tupleInWindow.getStringByField("carNo");
        if (carNo != null && carNoInWindow != null && carNo.equals(carNoInWindow)) {
          time = tuple.getStringByField("time");
          new_bayonet = tuple.getStringByField("bayonet");
          timeInWindow = tupleInWindow.getStringByField("time");
          old_bayonet = tupleInWindow.getStringByField("bayonet");

          //由于newTuples包含在tuplesInWindow中，所以判断车辆信息是否相同，排除自己与自己的对比的情况
          if (time.equals(timeInWindow) && (new_bayonet.equals(old_bayonet))) {
            continue;
          }

          //计算距离差
          new_longitude = Double.valueOf(tuple.getStringByField("longitude"));
          new_latitude = Double.valueOf(tuple.getStringByField("latitude"));

          old_longitude = Double.valueOf(tupleInWindow.getStringByField("longitude"));
          old_latitude = Double.valueOf(tupleInWindow.getStringByField("latitude"));
          double distance = distanceSimplify(new_latitude, new_longitude, old_latitude, old_longitude) / 1000.0;

          // 计算时间差
          long timeDiff = getTimeDiff(time, timeInWindow);

          //格式化系统时间
          //判断是否为套牌车的逻辑，当前逻辑为5分钟内超过20公里即认为套牌车，时间单位为分钟，距离单位为公里
          if ((timeDiff < input_timeString) && distance > input_distence) {

            // 当两辆套牌车在同一个new窗口内时，通过A能找到B，通过B也能找到A，所以需要先放到Map里去
            fakeCar.put(carNo, old_bayonet + "," + new_bayonet + "," + timeInWindow + "," + time);
//            collector.emit(new Values(carNo, old_bayonet, new_bayonet, timeInWindow, time));
            LOGGER.info("===== find one fake car " + carNo + " gate1: " + new_bayonet + " gate2: " + old_bayonet + "=====");
          }
        }
      }
    }

    if (!fakeCar.isEmpty()) {
      for (Map.Entry<String, String> entry : fakeCar.entrySet()) {
        String carNo = entry.getKey();
        String extraInfo = entry.getValue();
        String[] extraInfoParts = extraInfo.split(",");
        if (extraInfoParts.length == 4) {
          // carno, gate1, gate2, time1, time2
          collector.emit(new Values(carNo, extraInfoParts[0], extraInfoParts[1], extraInfoParts[2], extraInfoParts[3]));
        }
      }
    }
  }

  //通过经纬度计算距离
  public static double distanceSimplify(double lat1, double lng1, double lat2, double lng2) {
    double dx = lng1 - lng2;//经度差值
    double dy = lat1 - lat2;//纬度差值
    double b = (lat1 + lat2) / 2.0;//平均纬度
    double Lx = Math.toRadians(dx) * 6367000.0 * Math.cos(Math.toRadians(b));//东西距离
    double Ly = 6367000.0 * Math.toRadians(dy);//南北距离
    return Math.sqrt(Lx * Lx + Ly * Ly);//用平面的矩形对角距离公式计算总距离
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("carNo", "gate1", "gate2", "time1", "time2"));
  }
}