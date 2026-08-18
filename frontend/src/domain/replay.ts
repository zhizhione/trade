/** 历史回放 REST 与 WebSocket 协议使用的领域数据模型。 */
export interface ReplayCatalogEntry {
  /** 源 DBN 文件内容哈希，与文件名和路径无关。 */
  fileSha256: string;
  /** Databento 发布者与合约编号共同确定一张订单簿。 */
  publisherId: number;
  instrumentId: number;
  symbol: string;
  startMs: number;
  endMs: number;
  bucketMs: number;
  eventCount: number;
}

export interface ReplayDepthLevel {
  priceNano: number;
  size: number;
  orderCount: number;
}

export interface ReplayFrame {
  /** 采样桶起点的绝对 Unix 毫秒时间。 */
  timeMs: number;
  /** 原始 DBN 解码流的绝对顺序号，用于审计与稳定排序。 */
  sourceOrdinal: number;
  sequence: number;
  bucketMs: number;
  /** action=A 在当前采样桶中的总数量。 */
  addedSize: number;
  /** action=C 在当前采样桶中的总数量。 */
  cancelledSize: number;
  /** action=T 在当前采样桶中的主动成交量；不含 F，避免双算。 */
  tradedSize: number;
  bids: ReplayDepthLevel[];
  asks: ReplayDepthLevel[];
  /** 首条 Clear 到达前为 false，此时盘口初始状态尚不完整。 */
  complete: boolean;
  /** 最优买价大于等于最优卖价时为 true；策略不应将其作为有效 BBO。 */
  crossed: boolean;
}

export interface ReplayBar {
  timeMs: number;
  openNano: number;
  highNano: number;
  lowNano: number;
  closeNano: number;
}

export interface ReplaySession {
  fileSha256: string;
  publisherId: number;
  instrumentId: number;
  symbol: string;
  bucketMs: number;
  depth: number;
  barIntervalMs: number;
  bars: ReplayBar[];
  frames: ReplayFrame[];
  nextStartMs?: number;
}

export interface ReplayStreamReady {
  publisherId: number;
  instrumentId: number;
  symbol: string;
  bucketMs: number;
  barIntervalMs: number;
  depth: number;
  diagnostic: boolean;
  startMs: number;
  endMs: number;
}

export type ReplayStreamMessage =
  | { type: 'status'; payload: { connected: boolean; serverTime: string } }
  | { type: 'replay_ready'; payload: ReplayStreamReady }
  | { type: 'replay_frame'; payload: ReplayFrame }
  | { type: 'replay_bar'; payload: ReplayBar }
  | { type: 'replay_complete'; payload: { startMs: number; endMs: number; truncated?: boolean } }
  | { type: 'replay_error'; payload: { message: string } };

export type ReplayStreamStatus = 'connecting' | 'connected' | 'disconnected';

export interface ReplayStreamConnection {
  send(type: string, payload?: Record<string, unknown>): void;
  close(): void;
}

/** 将后端精确纳元价格转换为前端展示及图表使用的普通价格。 */
export function nanoPrice(value: number): number {
  return value / 1_000_000_000;
}
