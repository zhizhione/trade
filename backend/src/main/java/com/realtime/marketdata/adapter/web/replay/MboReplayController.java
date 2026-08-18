package com.realtime.marketdata.adapter.web.replay;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.realtime.marketdata.replay.engine.MboReplayService;
import com.realtime.marketdata.replay.model.ReplayCatalogEntry;
import com.realtime.marketdata.replay.model.ReplaySession;
import com.realtime.marketdata.replay.source.ReplayDataAccessException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/replay")
/**
 * 历史回放的 HTTP 适配器。目录接口用于填充前端合约选择器；会话接口一次性返回有限窗口，
 * 适合调试和批处理。需要播放/暂停/倍速控制时应使用 /ws/replay 而不是轮询 HTTP。
 */
public class MboReplayController {
    private final MboReplayService replayService;

    public MboReplayController(MboReplayService replayService) {
        this.replayService = replayService;
    }

    @GetMapping("/catalog")
    /** 返回当前可回放的来源文件与合约身份组合，不暴露底层原始表实现。 */
    public ResponseEntity<List<ReplayCatalogEntry>> catalog() {
        log.info("HTTP replay catalog requested");
        try {
            return ResponseEntity.ok(replayService.catalog());
        } catch (ReplayDataAccessException exception) {
            log.error("HTTP replay catalog failed", exception);
            throw replayUnavailable(exception);
        }
    }

    @GetMapping("/session")
    /**
     * 重建并返回一个静态回放会话。时间参数使用 Unix 毫秒，limit 限制可见帧数，
     * barIntervalMs 控制服务端中间价 OHLC 聚合周期；diagnostic=true 才返回 400 档深度，
     * 普通查询固定返回最多 100 档。
     */
    public ResponseEntity<ReplaySession> session(
        @RequestParam int publisherId,
        @RequestParam long instrumentId,
        @RequestParam(defaultValue = "100") int bucketMs,
        @RequestParam long startMs,
        @RequestParam long endMs,
        @RequestParam(defaultValue = "6000") int limit,
        @RequestParam(defaultValue = "1000") int barIntervalMs,
        @RequestParam(defaultValue = "false") boolean diagnostic
    ) {
        log.info(
            "HTTP replay session requested: publisherId={}, instrumentId={}, startMs={}, endMs={}, bucketMs={}, limit={}, barIntervalMs={}, diagnostic={}",
            publisherId, instrumentId, startMs, endMs, bucketMs, limit, barIntervalMs, diagnostic
        );
        try {
            return ResponseEntity.ok(replayService.session(
                publisherId, instrumentId, bucketMs, startMs, endMs, limit, barIntervalMs, diagnostic
            ));
        } catch (ReplayDataAccessException exception) {
            log.error(
                "HTTP replay session failed: publisherId={}, instrumentId={}, startMs={}, endMs={}",
                publisherId, instrumentId, startMs, endMs, exception
            );
            throw replayUnavailable(exception);
        }
    }

    private ResponseStatusException replayUnavailable(ReplayDataAccessException exception) {
        return new ResponseStatusException(
            HttpStatus.SERVICE_UNAVAILABLE,
            "ClickHouse replay data is temporarily unavailable",
            exception
        );
    }
}
