package com.huawei.storm.hcc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 从consumerRecord中构造一条 Tuple
 */
public class TopicsTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K, V> {

  private static final long serialVersionUID = -2521108716808001532L;
  private static final Logger LOG = LoggerFactory.getLogger(TopicsTupleBuilder.class);

  public TopicsTupleBuilder(String... topics) {
    super(topics);
  }

  @Override
  public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {

    String record = consumerRecord.value().toString();
    String[] info = record.split(",");
    if (info.length == 5) {
      return new Values(info[0], info[1], info[2], info[3], info[4]);
    } else {
      LOG.warn("malform format for data: " + record);
      return null;
    }
  }

}
