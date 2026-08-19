package com.realtime.marketdata.adapter.kafka;

import com.realtime.marketdata.config.KafkaConfig;
import com.realtime.marketdata.market.processor.MarketEventService;
import com.realtime.marketdata.orderbook.engine.MboBookInvariantException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

/**
 * Kafka 到领域服务的薄入口。格式错误和不可解析合约由领域服务记录后跳过，避免一条
 * 脏消息阻塞整个分区；当前版本不实现 DLT，若需要失败重试应在监听器容器层配置。
 */
@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaMarketConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMarketConsumer.class);

    private final MarketEventService marketEventService;

    public KafkaMarketConsumer(MarketEventService marketEventService) {
        this.marketEventService = marketEventService;
    }

    @KafkaListener(
        topics = {
            KafkaConfig.MARKET_TICK,
            KafkaConfig.MARKET_TRADE,
            KafkaConfig.MARKET_ORDER_BOOK,
            KafkaConfig.MARKET_MBO,
            KafkaConfig.MARKET_SIGNAL
        },
        groupId = "${spring.kafka.consumer.group-id:market-data-service}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        try {
            marketEventService.process(record.topic(), record.value());
        } catch (MboBookInvariantException exception) {
            // Defensive boundary: the realtime adapter normally converts this into a
            // desynchronized result, but it must never poison a Kafka partition if a future
            // adapter path lets the invariant escape.
            log.warn(
                "Skipping invalid MBO event from topic {} partition {} offset {}: {}",
                record.topic(), record.partition(), record.offset(), exception.getMessage()
            );
        } catch (JacksonException | IllegalArgumentException exception) {
            log.warn(
                "Skipping malformed market event from topic {} partition {} offset {}: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception.getMessage()
            );
        }
    }
}
