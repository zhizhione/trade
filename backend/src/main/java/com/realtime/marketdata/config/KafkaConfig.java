package com.realtime.marketdata.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * 集中声明行情 topic。分区数在本地和生产必须由部署策略统一管理，避免应用升级时
 * 隐式改变已有 topic 的分区布局或消费者顺序语义。
 */
@EnableKafka
@Configuration
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConfig {

    public static final String MARKET_TICK = "market.tick";
    public static final String MARKET_TRADE = "market.trade";
    public static final String MARKET_ORDER_BOOK = "market.order_book";
    public static final String MARKET_MBO = "market.mbo";
    public static final String MARKET_SIGNAL = "market.signal";

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
        @Value("${spring.kafka.consumer.group-id:market-data-service}") String groupId,
        @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties));
        factory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD
        );
        return factory;
    }

    @Bean
    NewTopic marketTickTopic() {
        return topic(MARKET_TICK);
    }

    @Bean
    NewTopic marketTradeTopic() {
        return topic(MARKET_TRADE);
    }

    @Bean
    NewTopic marketOrderBookTopic() {
        return topic(MARKET_ORDER_BOOK);
    }

    @Bean
    NewTopic marketMboTopic() {
        return topic(MARKET_MBO);
    }

    @Bean
    NewTopic marketSignalTopic() {
        return topic(MARKET_SIGNAL);
    }

    private NewTopic topic(String name) {
        // 这里只有新 topic 的默认值；Kafka 已存在 topic 的真实配置不会被该 Bean 覆盖。
        return TopicBuilder.name(name)
            .partitions(3)
            .replicas(1)
            .build();
    }
}
