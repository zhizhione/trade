import { useEffect, useRef } from 'react';
import type { CSSProperties } from 'react';
import { nanoPrice, type ReplayFrame } from '../domain/replay';

interface DomLadderProps {
  frame?: ReplayFrame;
  previousFrame?: ReplayFrame;
  searchPrice?: string;
  priceRange?: { min: number; max: number };
}

export function DomLadder({ frame, previousFrame, searchPrice = '', priceRange }: DomLadderProps) {
  const rowsRef = useRef<HTMLDivElement>(null);
  const centeringAnimationRef = useRef<number | undefined>(undefined);
  const bids = frame?.bids ?? [];
  const asks = frame?.asks ?? [];
  const normalizedSearch = searchPrice.trim().replaceAll(',', '');
  const matchesSearch = (priceNano: number) => (
    !normalizedSearch || nanoPrice(priceNano).toFixed(2).includes(normalizedSearch)
  );
  const levels = [
    ...[...asks].reverse().map((level) => ({ ...level, side: 'ask' as const })),
    ...bids.map((level) => ({ ...level, side: 'bid' as const })),
  ].filter((level) => matchesSearch(level.priceNano));
  const previousByKey = new Map(
    [...(previousFrame?.bids ?? []).map((level) => ['bid:', level] as const),
      ...(previousFrame?.asks ?? []).map((level) => ['ask:', level] as const)]
      .map(([side, level]) => [`${side}${level.priceNano}`, level] as const),
  );
  const maxSize = Math.max(1, ...levels.map((level) => level.size));
  const bestBid = bids[0]?.priceNano;
  const bestAsk = asks[0]?.priceNano;
  // 每个回放帧都把买三/卖三之间的位置保持在可视区域中央，
  // 让盘口围绕更稳定的近端深度展示；不足三档时退回到最深可用档位。
  useEffect(() => {
    if (!frame) return;
    const element = rowsRef.current;
    if (!element) return;
    const rowHeight = 27;
    const askAnchor = asks[Math.min(2, asks.length - 1)];
    const bidAnchor = bids[Math.min(2, bids.length - 1)];
    const askIndex = askAnchor
      ? levels.findIndex((level) => level.side === 'ask' && level.priceNano === askAnchor.priceNano)
      : -1;
    const bidIndex = bidAnchor
      ? levels.findIndex((level) => level.side === 'bid' && level.priceNano === bidAnchor.priceNano)
      : -1;
    const visibleIndices = [askIndex, bidIndex].filter((index) => index >= 0);
    const anchorRow = visibleIndices.length > 0
      ? visibleIndices.reduce((sum, index) => sum + index, 0) / visibleIndices.length
      : levels.length / 2;
    const target = Math.max(0, (anchorRow + 0.5) * rowHeight - element.clientHeight / 2);
    if (centeringAnimationRef.current !== undefined) {
      window.cancelAnimationFrame(centeringAnimationRef.current);
    }
    const animateCentering = () => {
      const distance = target - element.scrollTop;
      if (Math.abs(distance) <= 0.5) {
        element.scrollTop = target;
        centeringAnimationRef.current = undefined;
        return;
      }
      element.scrollTop += distance * 0.08;
      centeringAnimationRef.current = window.requestAnimationFrame(animateCentering);
    };
    centeringAnimationRef.current = window.requestAnimationFrame(animateCentering);
    return () => {
      if (centeringAnimationRef.current !== undefined) {
        window.cancelAnimationFrame(centeringAnimationRef.current);
        centeringAnimationRef.current = undefined;
      }
    };
  }, [frame, levels.length, searchPrice]);

  return (
    <div
      className="dom"
      aria-label="当前全深度订单簿"
      data-price-min={priceRange?.min}
      data-price-max={priceRange?.max}
    >
      <div className="dom__header">
        <span>挂单</span>
        <span>价格</span>
        <span>撤单 <small>Σ {frame?.cancelledSize?.toLocaleString() ?? '—'}</small></span>
      </div>
      <div
        className="dom__rows"
        ref={rowsRef}
      >
        {levels.length === 0 && (
          <p className="empty-state">
            {normalizedSearch ? '没有匹配的价格' : '选择回放数据后显示当前盘口'}
          </p>
        )}
        {levels.map((level) => {
          const ratio = `${Math.max(4, level.size / maxSize * 100)}%`;
          const previous = previousFrame ? previousByKey.get(`${level.side}:${level.priceNano}`) : undefined;
          const delta = previous ? level.size - previous.size : 0;
          const orderDelta = previous ? level.orderCount - previous.orderCount : 0;
          const changeKind = previousFrame && !previous ? 'new' : delta > 0 ? 'add' : delta < 0 ? 'reduce' : orderDelta !== 0 ? 'modify' : undefined;
          const changeLabel = changeKind === 'new' ? '新' : changeKind === 'add' ? `+${delta}` : changeKind === 'reduce' ? `${delta}` : orderDelta > 0 ? `+${orderDelta}单` : orderDelta < 0 ? `${orderDelta}单` : '';
          const isBbo = level.priceNano === bestBid || level.priceNano === bestAsk;
          return (
            <div className={`dom__row dom__row--${level.side} ${isBbo ? 'dom__row--bbo' : ''} ${changeKind ? `dom__row--${changeKind}` : ''}`} key={`${level.side}-${level.priceNano}`}>
              <span className={`dom__quantity dom__quantity--${level.side}`} style={{ '--depth': ratio } as CSSProperties}>
                <span>{level.size.toLocaleString()} <small>{level.orderCount}</small></span>
                {changeKind && <em className="dom__change" title="相对上一回放帧的档位数量变化">{changeLabel}</em>}
              </span>
              <strong>{nanoPrice(level.priceNano).toFixed(2)}</strong>
              <span className="dom__event-cell dom__event-cell--cancel" title="逐价位撤单数据待后端补充">—</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
