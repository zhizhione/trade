package com.realtime.marketdata.adapter.simulator;

import com.realtime.marketdata.config.KafkaConfig;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * 仅用于本地链路验证的 Kafka 行情模拟器。它生成的 DEMO-USD 不代表真实合约，
 * 也不应写入生产历史表；关闭 ``app.simulator.enabled`` 后不会创建该定时任务。
 */
@Component
@ConditionalOnProperty(prefix = "app.simulator", name = "enabled", havingValue = "true")
public class MarketDataSimulator {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AtomicLong sequence = new AtomicLong();
    private BigDecimal price = new BigDecimal("100.0000");

    public MarketDataSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${app.simulator.interval-ms:500}")
    public void publishTick() {
        // sequence 只在本次模拟进程内单调递增，用来验证消费、快照和前端刷新链路。
        ThreadLocalRandom random = ThreadLocalRandom.current();
        BigDecimal move = BigDecimal.valueOf(random.nextDouble(-0.18, 0.18));
        price = price.add(move).setScale(4, RoundingMode.HALF_UP);
        BigDecimal spread = new BigDecimal("0.0200");
        BigDecimal bid = price.subtract(spread);
        BigDecimal ask = price.add(spread);

        Map<String, Object> tick = new LinkedHashMap<>();
        tick.put("eventType", "tick");
        tick.put("symbol", "DEMO-USD");
        tick.put("eventTime", Instant.now());
        tick.put("sequence", sequence.incrementAndGet());
        tick.put("price", price);
        tick.put("bidPrice", bid);
        tick.put("bidQuantity", random.nextInt(10, 100));
        tick.put("askPrice", ask);
        tick.put("askQuantity", random.nextInt(10, 100));
        tick.put("orderFlow", BigDecimal.valueOf(random.nextDouble(-50, 50)).setScale(2, RoundingMode.HALF_UP));

        kafkaTemplate.send(KafkaConfig.MARKET_TICK, "DEMO-USD", objectMapper.writeValueAsString(tick));

        // 低频发布完整盘口和信号，模拟不同 topic 的到达频率，而不是每个 tick 都重复发送。
        if (sequence.get() % 10 == 0) {
            publishOrderBook(bid, ask, random);
        }
        if (sequence.get() % 20 == 0) {
            publishSignal(random);
        }
    }

    private void publishOrderBook(BigDecimal bid, BigDecimal ask, ThreadLocalRandom random) {
        List<List<Object>> bids = java.util.stream.IntStream.range(0, 8)
            .mapToObj(index -> List.<Object>of(
                bid.subtract(new BigDecimal("0.0200").multiply(BigDecimal.valueOf(index))),
                random.nextInt(10, 120)
            ))
            .toList();
        List<List<Object>> asks = java.util.stream.IntStream.range(0, 8)
            .mapToObj(index -> List.<Object>of(
                ask.add(new BigDecimal("0.0200").multiply(BigDecimal.valueOf(index))),
                random.nextInt(10, 120)
            ))
            .toList();
        Map<String, Object> book = Map.of(
            "eventType", "order_book",
            "symbol", "DEMO-USD",
            "eventTime", Instant.now(),
            "bids", bids,
            "asks", asks
        );
        kafkaTemplate.send(KafkaConfig.MARKET_ORDER_BOOK, "DEMO-USD", objectMapper.writeValueAsString(book));
    }

    private void publishSignal(ThreadLocalRandom random) {
        Map<String, Object> signal = Map.of(
            "eventType", "signal",
            "symbol", "DEMO-USD",
            "eventTime", Instant.now(),
            "signalValue", BigDecimal.valueOf(random.nextDouble(-1, 1)).setScale(4, RoundingMode.HALF_UP),
            "name", "demo-momentum"
        );
        kafkaTemplate.send(KafkaConfig.MARKET_SIGNAL, "DEMO-USD", objectMapper.writeValueAsString(signal));
    }
}
