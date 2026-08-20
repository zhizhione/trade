/** 实时行情 WebSocket 协议使用的领域数据模型。 */
export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';

export interface DepthLevel {
  price: number;
  quantity: number;
}

export interface MarketSnapshot {
  symbol: string;
  eventTime: string;
  lastPrice?: number;
  bids: DepthLevel[];
  asks: DepthLevel[];
  orderFlow: number;
  signalValue: number;
  lastEventType: string;
  /** OK means the incremental L3 book is usable; DESYNCHRONIZED must not be traded on. */
  bookStatus?: 'OK' | 'DESYNCHRONIZED';
  /** True when the best bid is at or above the best ask; retained for diagnostics. */
  crossed?: boolean;
}

export interface MarketEvent {
  eventId: string;
  topic: string;
  eventType: string;
  symbol: string;
  eventTime: string;
  receivedAt: string;
  sequence?: number;
  price?: number;
  quantity?: number;
  side?: string;
  data: Record<string, unknown>;
}

export type MarketMessage =
  | { type: 'snapshot'; payload: MarketSnapshot }
  | { type: 'event'; payload: MarketEvent }
  | { type: 'status'; payload: { connected: boolean; serverTime: string } };
