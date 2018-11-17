package com.huawei.storm.hcc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SearchXCarTopology {

  private static final Logger LOG = LoggerFactory.getLogger(SearchXCarTopology.class);
  private static final String DEFAULT_STREAM_NAME = "test_stream";
  private static final String DEFAULT_GROUP_ID = "kafkaSpoutTestGroup";

  /*
   *  params：
   *  0 topology_name 拓扑名称 ;
   *  1 stream_name(输入流名称);
   *  2 stream_name(输出流名称);
   *  3 broker list;
   *  4 更新频率（单位：秒）; window length
   *  5  sliding integer
   *  6 套牌车规则中的时间（单位：分钟）;
   *  7 套牌车规则中的距离（单位：公里）
   */
  public static void main(String[] args) throws Exception {

    Config conf = new Config();
    conf.setMessageTimeoutSecs(3600);

    // 定义拓扑
    TopologyBuilder builder = new TopologyBuilder();
    if (args.length >= 8) {
      String topologyName = args[0];
      String inputTopic = args[1];
      String outputTopic = args[2];
      String kafkaBrokerList = args[3];
      int windowLength = Integer.valueOf(args[4]);
      int slidingInteger = Integer.valueOf(args[5]);

      long time = Long.valueOf(args[6]);
      double distance = Double.valueOf(args[7]);

      String consumerConfig = "";
      if (args.length == 9) {
        consumerConfig = args[8];
      }
      String producerConfig = "";
      if (args.length == 10) {
        producerConfig = args[9];
      }

      KafkaSpout kafkaSpout = new KafkaSpout<String, String>(
              KafkaUtils.getKafkaSpoutConfig(
                      KafkaUtils.getKafkaSpoutStreams(inputTopic, DEFAULT_STREAM_NAME),
                      inputTopic, kafkaBrokerList,
                      DEFAULT_GROUP_ID,
                      consumerConfig
              ));

      KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();
      kafkaBolt.withTopicSelector(new DefaultTopicSelector(outputTopic))
              .withTupleToKafkaMapper(
                      new FieldNameBasedTupleToKafkaMapper(null, "message"));
      kafkaBolt.withProducerProperties(KafkaUtils.getKafkaProducerProps(kafkaBrokerList, producerConfig));

      builder.setSpout("kafka-spout", kafkaSpout);
      builder.setBolt("bolt",
              new SearchXCarBolt(time, distance).withWindow(
                      new Duration(windowLength, TimeUnit.SECONDS),
                      new Duration(slidingInteger, TimeUnit.SECONDS)),
              10).fieldsGrouping("kafka-spout", DEFAULT_STREAM_NAME, new Fields("carNo"));

      builder.setBolt("aggr-bolt", new SearchXCarAggrBolt(), 1).shuffleGrouping("bolt");

      builder.setBolt("result", kafkaBolt, 1).shuffleGrouping("aggr-bolt");

      StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    } else {
      System.out.println("Invalid args number: " + args.length);
    }
  }
}