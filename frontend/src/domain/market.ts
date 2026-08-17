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
