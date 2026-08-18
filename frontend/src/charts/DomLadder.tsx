import { useCallback, useEffect, useRef } from 'react';
import type { CSSProperties } from 'react';
import { nanoPrice, type ReplayFrame } from '../domain/replay';

const DOM_CENTERING_EASE = 0.035;

interface DomLadderProps {
  frame?: ReplayFrame;
  previousFrame?: ReplayFrame;
  searchPrice?: string;
  priceRange?: { min: number; max: number };
}

export function DomLadder({
  frame,
  previousFrame,
  searchPrice = '',
  priceRange,
}: DomLadderProps) {
  const rowsRef = useRef<HTMLDivElement>(null);
  const centeringAnimationRef = useRef<number | undefined>(undefined);
  const manualScrollRef = useRef(false);
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
  const cancelCentering = useCallback(() => {
    if (centeringAnimationRef.current !== undefined) {
      window.cancelAnimationFrame(centeringAnimationRef.current);
      centeringAnimationRef.current = undefined;
    }
  }, []);

  const centerRows = useCallback((force = false) => {
    if (!force && manualScrollRef.current) return;
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
    cancelCentering();
    const animateCentering = () => {
      const distance = target - element.scrollTop;
      if (Math.abs(distance) <= 0.5) {
        element.scrollTop = target;
        centeringAnimationRef.current = undefined;
        return;
      }
      element.scrollTop += distance * DOM_CENTERING_EASE;
      centeringAnimationRef.current = window.requestAnimationFrame(animateCentering);
    };
    centeringAnimationRef.current = window.requestAnimationFrame(animateCentering);
  }, [asks, bids, cancelCentering, levels]);

  // 用户开始浏览上下档位后，暂停逐帧自动居中，避免滚动位置被回放抢回。
  const suspendAutoCentering = useCallback(() => {
    manualScrollRef.current = true;
    cancelCentering();
  }, [cancelCentering]);

  useEffect(() => {
    if (!frame) {
      manualScrollRef.current = false;
    }
  }, [frame]);

  useEffect(() => {
    if (!frame || manualScrollRef.current) return;
    centerRows();
    return cancelCentering;
  }, [cancelCentering, centerRows, frame, levels.length, searchPrice]);

  const recenter = () => {
    manualScrollRef.current = false;
    centerRows(true);
  };

  return (
    <div
      className="dom"
      aria-label="当前订单簿"
      data-price-min={priceRange?.min}
      data-price-max={priceRange?.max}
    >
      <div className="dom__header">
        <span>挂单</span>
        <span>价格</span>
        <button type="button" title="回到买三和卖三附近" onClick={recenter}>居中</button>
      </div>
      <div
        className="dom__rows"
        ref={rowsRef}
        tabIndex={0}
        onPointerDown={suspendAutoCentering}
        onTouchStart={suspendAutoCentering}
        onWheel={suspendAutoCentering}
        onKeyDown={(event) => {
          if (['ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', 'Home', 'End', ' '].includes(event.key)) {
            suspendAutoCentering();
          }
        }}
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
            </div>
          );
        })}
      </div>
    </div>
  );
}
