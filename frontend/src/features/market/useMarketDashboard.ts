import { useEffect, useMemo, useState } from 'react';
import type { MarketEvent, MarketSnapshot } from '../../domain/market';
import { connectMarketWebSocket } from '../../realtime/marketSocket';
import type { PricePoint } from '../../charts/PriceChart';
import type { SignalPoint } from '../../charts/SignalChart';

const HISTORY_LIMIT = 240;
const EVENT_LIMIT = 18;

function appendLimited<T>(items: T[], item: T): T[] {
  const next = [...items, item];
  return next.length > HISTORY_LIMIT ? next.slice(next.length - HISTORY_LIMIT) : next;
}

/**
 * 聚合实时 WebSocket 消息为行情仪表盘所需的状态。图表只由服务器快照派生，避免事件与
 * 快照对同一成交重复计数；事件列表则保留网络到达顺序，便于观察传输与处理延迟。
 */
export function useMarketDashboard() {
  const [status, setStatus] = useState<'connecting' | 'connected' | 'disconnected'>('connecting');
  const [snapshot, setSnapshot] = useState<MarketSnapshot>();
  const [events, setEvents] = useState<MarketEvent[]>([]);
  const [prices, setPrices] = useState<PricePoint[]>([]);
  const [signals, setSignals] = useState<SignalPoint[]>([]);

  useEffect(() => connectMarketWebSocket((message) => {
    if (message.type === 'event') {
      setEvents((current) => [message.payload, ...current].slice(0, EVENT_LIMIT));
      return;
    }
    if (message.type !== 'snapshot') return;

    const next = message.payload;
    setSnapshot(next);
    const lastPrice = next.lastPrice;
    if (lastPrice !== undefined) {
      setPrices((current) => appendLimited(current, { time: next.eventTime, value: lastPrice }));
    }
    setSignals((current) => appendLimited(current, {
      time: next.eventTime,
      orderFlow: next.orderFlow ?? 0,
      signal: next.signalValue ?? 0,
    }));
  }, setStatus), []);

  const spread = useMemo(() => {
    const bestBid = snapshot?.bids[0]?.price;
    const bestAsk = snapshot?.asks[0]?.price;
    return bestBid === undefined || bestAsk === undefined ? undefined : bestAsk - bestBid;
  }, [snapshot]);

  return { status, snapshot, events, prices, signals, spread };
}
