package com.huawei.storm.hcc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SearchCarSumTopology {

  private static final Logger LOG = LoggerFactory.getLogger(SearchCarSumTopology.class);
  private static final String DEFAULT_STREAM_NAME = "test_stream_sum";
  private static final String DEFAULT_GROUP_ID = "search-car-sum-group";

  /*
   * 参数列表：
   * 0. topology_name(拓扑名称);
   * 1. stream_name(输入流名称);
   * 2. stream_name(输出流名称);
   * 3. brokerlist
   * 4. 窗口大小;
   * 5. 更新频率
   */
  public static void main(String[] args) throws Exception {

    //设置拓扑配置
    Config conf = new Config();
    conf.setMessageTimeoutSecs(3600);

    //定义拓扑
    TopologyBuilder builder = new TopologyBuilder();
    if (args.length >= 6) {
      String topologyName = args[0];
      String inputTopic = args[1];
      String outputTopic = args[2];
      String brokerList = args[3];
      int windowLength = Integer.valueOf(args[4]);
      int slidingInteger = Integer.valueOf(args[5]);

      String consumerConfig = "";
      if (args.length == 7) {
        consumerConfig = args[6];
      }
      String producerConfig = "";
      if (args.length == 8) {
        producerConfig = args[7];
      }
      // 定义KafkaSpout
      KafkaSpout kafkaSpout = new KafkaSpout<String, String>(
              KafkaUtils.getKafkaSpoutConfig(
                      KafkaUtils.getKafkaSpoutStreams(inputTopic, DEFAULT_STREAM_NAME),
                      inputTopic,
                      brokerList,
                      DEFAULT_GROUP_ID,
                      consumerConfig));

      // KafkaBolt配置信息
      KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();
      kafkaBolt.withTopicSelector(new DefaultTopicSelector(outputTopic))
              .withTupleToKafkaMapper(
                      new FieldNameBasedTupleToKafkaMapper(null, "message"));
      kafkaBolt.withProducerProperties(KafkaUtils.getKafkaProducerProps(brokerList, producerConfig));

      builder.setSpout("spout", kafkaSpout, 1);
      builder.setBolt(
              "bolt",
              new SearchCarSumPerMinuteBolt(slidingInteger).withWindow(new Duration(windowLength,
                      TimeUnit.SECONDS), new Duration(slidingInteger,
                      TimeUnit.SECONDS)), 1).shuffleGrouping("spout", DEFAULT_STREAM_NAME);
      builder.setBolt("result", kafkaBolt, 10).shuffleGrouping("bolt");

      //命令行提交拓扑
      StormSubmitter.submitTopology(topologyName, conf,
              builder.createTopology());
    } else {
      System.out.println("Invalid args number.");
    }
  }
}
