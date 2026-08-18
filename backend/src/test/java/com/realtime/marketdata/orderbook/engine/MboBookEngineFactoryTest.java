package com.realtime.marketdata.orderbook.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtime.marketdata.mbo.model.MboEvent;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class MboBookEngineFactoryTest {

    @Test
    void historicalAndLiveEnginesLimitSnapshotsToFourHundredPriceLevels() {
        MboBookEngineFactory factory = new MboBookEngineFactory();

        assertThat(snapshotBidCount(factory::createHistorical)).isEqualTo(MboBookEngine.MAX_DEPTH);
        assertThat(snapshotBidCount(factory::createLive)).isEqualTo(MboBookEngine.MAX_DEPTH);
    }

    @Test
    void directEngineDefaultsAlsoLimitSnapshotsToFourHundredPriceLevels() {
        assertThat(snapshotBidCount(MboBookEngine::new)).isEqualTo(MboBookEngine.MAX_DEPTH);
    }

    private int snapshotBidCount(Supplier<MboBookEngine> engineSupplier) {
        MboBookEngine engine = engineSupplier.get();
        for (int level = 0; level < MboBookEngine.MAX_DEPTH + 50; level++) {
            if (level == MboBookEngine.MAX_DEPTH + 49) {
                return engine.apply(event(level, MboBookEngine.F_LAST)).orElseThrow().bids().size();
            }
            engine.apply(event(level, 0));
        }
        throw new AssertionError("last record was not applied");
    }

    private MboEvent event(int level, int flags) {
        return new MboEvent(
            level,
            1_700_000_000_000_000_000L + level,
            1_700_000_000_000_000_000L + level,
            160,
            1,
            750,
            'A',
            'B',
            10_000L - level,
            1,
            0,
            level + 1L,
            flags,
            0,
            level
        );
    }
}
