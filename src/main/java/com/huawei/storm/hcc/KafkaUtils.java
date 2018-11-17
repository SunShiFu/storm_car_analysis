package com.huawei.storm.hcc;

import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaUtils {

  /**
   * ==========================================================
   * kafka new consumer/producer 参数名称
   * ==========================================================
   */
  private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  private static final String SECURITY_PROTOCOL = "security.protocol";
  private static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
  private static final String GROUP_ID = "group.id";

  // kafka new consumer/producer 序列化/反序列化参数
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VALUE_SERIALIZER = "value.serializer";
  private static final String KEY_DESERIALIZER = "key.deserializer";
  private static final String VALUE_DESERIALIZER = "value.deserializer";
  private static final String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

  /**
   * ================================================================
   * kafka new consumer.producer 配置默认值
   * ================================================================
   */
  //kerberos服务的domain，请根据集群的真实域名进行配置，命名规则： hadoop.toLowerCase(${default_realm})，比如当前域名为HADOOP.COM，那么domain就应该为hadoop.hadoop.com
  private static final String DEFAULT_KERBEROS_DOMAIN_NAME = "hadoop.hadoop.com";

  // kafka服务名称，不能修改
  private static final String DEFAULT_SERVICE_NAME = "kafka";

  // kafka安全认证协议，当前支持'SASL_PLAINTEXT'和'PLAINTEXT'两种
  private static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

  // 默认序列化/反序列化类
  private static final String DEFAULT_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";


  /**
   * ========================================================
   * kafkaSpout.KafkaSpoutConfig 配置
   * ========================================================
   */
  // 默认offset提交周期，单位：毫秒
  private static final int DEFAULT_OFFSET_COMMIT_PERIOD_MS = 10000;
  // 默认最大容许的未提交的offset数，若kafkaSpout内部的uncommit_offset数大于该值，则kafkaSpout会暂停消费，直到uncommit_offset小于该值
  private static final int DEFAULT_MAX_UNCOMMIT_OFFSET_NUM = 500000;
  /*
   * kafkaSpout首次执行poll操作时选择offset的策略，当前提供四种策略模式
   * EARLIEST： 从最开始的offset开始
   * LATEST： 从最后的offset开始
   * UNCOMMITTED_EARLIEST： 从最后提交的offset开始，如果没有offset被提交，则等同于EARLIEST
   * UNCOMMITTED_LATEST： 从最后提交的offset开始，如果没有offset被提交，则等同于LATEST
   */
  private static final KafkaSpoutConfig.FirstPollOffsetStrategy DEFAULT_STRATEGY = FirstPollOffsetStrategy.LATEST;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  /**
   * ==========================================================
   * kafkaSpout.retryService 配置
   * ==========================================================
   */
  //第一次重试的初始延迟，单位：微秒（1毫秒=1000微秒）
  private static final int DEFAULT_DELAY = 500;
  //延迟周期，单位: 毫秒
  private static final int DEFAULT_DELAY_PERIOD = 2;
  //默认最大重试次数
  private static final int DEFAULT_MAX_RETRY_TIMES = Integer.MAX_VALUE;
  //最大延迟时间，单位：秒
  private static final int DEFAULT_MAX_DELAY = 10;


  public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(
          KafkaSpoutStreams kafkaSpoutStreams,
          String inputTopic,
          String kafkaBrokerList,
          String groupId,
          String consumerConfig) {
    return new KafkaSpoutConfig.Builder<String, String>(
            getKafkaConsumerProps(kafkaBrokerList, groupId, consumerConfig), kafkaSpoutStreams, getTuplesBuilder(inputTopic),
            getRetryService())
            .setOffsetCommitPeriodMs(DEFAULT_OFFSET_COMMIT_PERIOD_MS)
            .setFirstPollOffsetStrategy(DEFAULT_STRATEGY)
            .setMaxUncommittedOffsets(DEFAULT_MAX_UNCOMMIT_OFFSET_NUM)
            .build();
  }

  private static Map<String, Object> getKafkaConsumerProps(String kafkaBrokerList, String groupId, String consumerConfig) {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(BOOTSTRAP_SERVERS, kafkaBrokerList);
    props.put(GROUP_ID, groupId);
    props.put(SASL_KERBEROS_SERVICE_NAME, DEFAULT_SERVICE_NAME);
    props.put(SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
    props.put(KEY_DESERIALIZER, DEFAULT_DESERIALIZER);
    props.put(VALUE_DESERIALIZER, DEFAULT_DESERIALIZER);
    props.put(KERBEROS_DOMAIN_NAME, DEFAULT_KERBEROS_DOMAIN_NAME);
    props.put("session.timeout.ms", "300000");
    props.put("request.timeout.ms", "350000");

    if (consumerConfig == "") {
      return props;
    }

    Properties p = new Properties();
    InputStream is = null;
    try {
      File f = new File(consumerConfig);
      if (f.exists() && f.isFile()) {
        is = new FileInputStream(f);
        p.load(is);
      } else {
        LOG.warn("consumer config path does not exists, will ignore it: " + consumerConfig);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage());
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOG.warn("failed to close input stream.");
        }
      }
    }

    for (Map.Entry<Object, Object> entry : p.entrySet()) {
      props.put(entry.getKey().toString(), entry.getValue());
    }
    return props;
  }

  public static Properties getKafkaProducerProps(String kafkaBrokerList, String producerConfig) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS, kafkaBrokerList);
    props.put(SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
    props.put(KEY_SERIALIZER, DEFAULT_SERIALIZER);
    props.put(VALUE_SERIALIZER, DEFAULT_SERIALIZER);
    props.put(SASL_KERBEROS_SERVICE_NAME, DEFAULT_SERVICE_NAME);
    props.put(KERBEROS_DOMAIN_NAME, DEFAULT_KERBEROS_DOMAIN_NAME);

    if (producerConfig == "") {
      return props;
    }

    Properties p = new Properties();
    InputStream is = null;
    try {
      File f = new File(producerConfig);
      if (f.exists() && f.isFile()) {
        is = new FileInputStream(f);
        p.load(is);
      } else {
        LOG.warn("producer config path does not exists, will ignore it: " + producerConfig);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage());
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOG.warn("failed to close input stream.");
        }
      }
    }

    for (Map.Entry<Object, Object> entry : p.entrySet()) {
      props.put(entry.getKey().toString(), entry.getValue());
    }
    return props;
  }

  /**
   * 构造KafkaSpoutTuplesBuilder，用于从ConsumerRecords中构造tuples
   *
   * @return
   */
  private static KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder(String inputTopic) {
    return new KafkaSpoutTuplesBuilderNamedTopics.Builder<String, String>(
            new TopicsTupleBuilder<String, String>(inputTopic)).build();
  }

  /**
   * 构造KafkaSpoutRetryService，用于管理并重试发送失败的tuples
   *
   * @return
   */
  private static KafkaSpoutRetryService getRetryService() {
    return new KafkaSpoutRetryExponentialBackoff(getTimeInterval(
            DEFAULT_DELAY, TimeUnit.MICROSECONDS),
            TimeInterval.milliSeconds(DEFAULT_DELAY_PERIOD),
            DEFAULT_MAX_RETRY_TIMES,
            TimeInterval.seconds(DEFAULT_MAX_DELAY));
  }

  private static TimeInterval getTimeInterval(long delay, TimeUnit timeUnit) {
    return new TimeInterval(delay, timeUnit);
  }

  /**
   * 构造KafkaSpoutStreams，用于定义并添加stream
   *
   * @return
   */
  public static KafkaSpoutStreams getKafkaSpoutStreams(String inputTopic, String streamName) {
    final Fields outputFields = new Fields("carNo", "time", "bayonet", "longitude", "latitude");
    return new KafkaSpoutStreamsNamedTopics.Builder(outputFields, streamName,
            new String[]{inputTopic}).build();
  }
}
