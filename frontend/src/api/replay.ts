import type { ReplayCatalogEntry, ReplaySession } from '../domain/replay';
import { getJson } from '../shared/http';

export type { ReplayCatalogEntry, ReplaySession } from '../domain/replay';

/** 请求可供历史回放选择的“文件 × 发布者 × 合约”目录。 */
export function loadReplayCatalog(): Promise<ReplayCatalogEntry[]> {
  return getJson('/api/replay/catalog');
}

/**
 * 通过 REST 重建一个有限历史窗口，主要供调试、批量检查或不需要实时播放控制的调用方使用。
 * 交互式页面通常改用 replay WebSocket，以逐帧接收数据而非一次性等待完整响应。
 */
export function loadReplaySession(
  entry: ReplayCatalogEntry,
  startMs: number,
  endMs: number,
  limit = 6000,
  barIntervalMs = 1000,
  diagnostic = false,
): Promise<ReplaySession> {
  const params = new URLSearchParams({
    publisherId: String(entry.publisherId),
    instrumentId: String(entry.instrumentId),
    bucketMs: String(entry.bucketMs),
    startMs: String(startMs),
    endMs: String(endMs),
    limit: String(limit),
    barIntervalMs: String(barIntervalMs),
    diagnostic: String(diagnostic),
  });
  return getJson(`/api/replay/session?${params}`);
}
