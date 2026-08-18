package com.realtime.marketdata.replay.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ReplayStreamRequestTest {

    @Test
    void usesOneHundredDepthLevelsByDefault() {
        ReplayStreamRequest request = new ReplayStreamRequest(1, 750, 100, 0, 1_000, 1_000, 1.0);

        assertThat(request.diagnostic()).isFalse();
        assertThat(request.depth()).isEqualTo(ReplayStreamRequest.DEFAULT_DEPTH);
    }

    @Test
    void enablesFourHundredDepthLevelsOnlyInDiagnosticMode() {
        ReplayStreamRequest request = new ReplayStreamRequest(
            1, 750, 100, 0, 1_000, 1_000, 1.0, true
        );

        assertThat(request.diagnostic()).isTrue();
        assertThat(request.depth()).isEqualTo(ReplayStreamRequest.DIAGNOSTIC_DEPTH);
    }
}
