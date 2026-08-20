import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { DomLadder } from './charts/DomLadder';
import { ReplayCandleChart } from './charts/ReplayCandleChart';
import {
  loadReplayCatalog,
} from './api/replay';
import {
  nanoPrice,
  type ReplayCatalogEntry,
  type ReplayFrame,
  type ReplaySession,
  type ReplayStreamConnection,
  type ReplayStreamMessage,
} from './domain/replay';
import { connectReplayWebSocket } from './realtime/replaySocket';
import {
  formatNewYorkDateTime,
  parseNewYorkDateTimeInput,
  toNewYorkDateTimeInput,
} from './time';

const SPEEDS = [1, 5, 20];
const FRAME_WINDOW_SIZE = 1_000;

function formatQuantity(value: number | undefined): string {
  return value === undefined ? '—' : value.toLocaleString();
}

function formatSignedQuantity(value: number | undefined): string {
  if (value === undefined) return '—';
  return `${value > 0 ? '+' : ''}${value.toLocaleString()}`;
}

function formatRate(value: number | undefined): string {
  return value === undefined ? '—' : `${value.toFixed(1)}%`;
}

function formatDecimal(value: number | undefined): string {
  return value === undefined ? '—' : value.toFixed(2);
}

function identityKey(entry: Pick<ReplayCatalogEntry, 'publisherId' | 'instrumentId'>): string {
  return `${entry.publisherId}:${entry.instrumentId}`;
}

export function HistoricalReplay() {
  const [catalog, setCatalog] = useState<ReplayCatalogEntry[]>([]);
  const [selectedKey, setSelectedKey] = useState('');
  const [queryStartTime, setQueryStartTime] = useState('');
  const [queryEndTime, setQueryEndTime] = useState('');
  const [session, setSession] = useState<ReplaySession>();
  const [cursor, setCursor] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [barIntervalMs, setBarIntervalMs] = useState(1000);
  const [replayFinished, setReplayFinished] = useState(false);
  const [replayTruncated, setReplayTruncated] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [priceSearch, setPriceSearch] = useState('');
  const cursorRef = useRef(0);
  const replayRequestIdRef = useRef(0);
  const replayConnectionRef = useRef<ReplayStreamConnection | undefined>(undefined);
  const playingRef = useRef(false);
  const receivedFrameCountRef = useRef(0);
  const frameStoreRef = useRef<Map<number, ReplayFrame>>(new Map());

  const appendBar = useCallback((bar: ReplaySession['bars'][number]) => {
    setSession((current) => {
      if (!current) return current;
      const last = current.bars.at(-1);
      if (!last || last.timeMs !== bar.timeMs) {
        return { ...current, bars: [...current.bars, bar] };
      }
      return { ...current, bars: [...current.bars.slice(0, -1), bar] };
    });
  }, []);

  const handleStreamMessage = useCallback((message: ReplayStreamMessage) => {
    if (message.type === 'replay_ready') {
      const ready = message.payload;
      setSession({
        fileSha256: 'STREAM',
        publisherId: ready.publisherId,
        instrumentId: ready.instrumentId,
        symbol: ready.symbol,
        bucketMs: ready.bucketMs,
        depth: ready.depth,
        barIntervalMs: ready.barIntervalMs,
        bars: [],
        frames: [],
      });
      setCursor(0);
      cursorRef.current = 0;
      receivedFrameCountRef.current = 0;
      frameStoreRef.current.clear();
      setLoading(false);
      // 查询会创建服务端任务并立即开始播放，让用户无需再次点击即可看到首个历史盘口。
      playingRef.current = true;
      setPlaying(true);
      setReplayFinished(false);
      setReplayTruncated(false);
      setError(undefined);
      replayConnectionRef.current?.send('replay_play');
      return;
    }
    if (message.type === 'replay_frame') {
      const nextIndex = receivedFrameCountRef.current;
      receivedFrameCountRef.current += 1;
      frameStoreRef.current.set(nextIndex, message.payload);
      const nextWindowStart = Math.max(0, nextIndex - FRAME_WINDOW_SIZE + 1);
      const nextWindow: ReplayFrame[] = [];
      for (let index = nextWindowStart; index <= nextIndex; index += 1) {
        const stored = frameStoreRef.current.get(index);
        if (stored) nextWindow.push(stored);
      }
      setSession((current) => current ? {
        ...current,
        frames: nextWindow,
      } : current);
      setLoading(false);
      if (playingRef.current) {
        cursorRef.current = nextIndex;
        setCursor(nextIndex);
      }
      return;
    }
    if (message.type === 'replay_bar') {
      appendBar(message.payload);
      return;
    }
    if (message.type === 'replay_complete') {
      if (message.payload.hasNext && message.payload.nextCursor) {
        // 分段完成不是整次回放完成，继续使用同一个服务端订单簿任务。
        replayConnectionRef.current?.send('replay_continue', {
          cursor: message.payload.nextCursor,
        });
        return;
      }
      playingRef.current = false;
      setPlaying(false);
      setLoading(false);
      setReplayFinished(true);
      setReplayTruncated(Boolean(message.payload.truncated));
      return;
    }
    if (message.type === 'replay_error') {
      setError(message.payload.message);
      playingRef.current = false;
      setPlaying(false);
      setLoading(false);
      setReplayFinished(true);
    }
  }, [appendBar]);

  useEffect(() => {
    const connection = connectReplayWebSocket(handleStreamMessage, () => {});
    replayConnectionRef.current = connection;
    return () => {
      connection.send('replay_stop');
      connection.close();
      if (replayConnectionRef.current === connection) replayConnectionRef.current = undefined;
    };
  }, [handleStreamMessage]);

  const selected = useMemo(
    () => catalog.find((entry) => identityKey(entry) === selectedKey),
    [catalog, selectedKey],
  );
  const contractOptions = useMemo(() => {
    const seen = new Set<string>();
    return catalog.filter((entry) => {
      const key = identityKey(entry);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }, [catalog]);
  const availableRange = useMemo(() => {
    if (!selected) return undefined;
    const matchingEntries = catalog.filter((entry) => (
      entry.publisherId === selected.publisherId && entry.instrumentId === selected.instrumentId
    ));
    if (matchingEntries.length === 0) return undefined;
    return matchingEntries.reduce((range, entry) => ({
      startMs: Math.min(range.startMs, entry.startMs),
      endMs: Math.max(range.endMs, entry.endMs),
    }), { startMs: matchingEntries[0].startMs, endMs: matchingEntries[0].endMs });
  }, [catalog, selected]);
  const frame = frameStoreRef.current.get(cursor);
  const queryStartMs = useMemo(() => parseNewYorkDateTimeInput(queryStartTime), [queryStartTime]);
  const queryEndMs = useMemo(() => parseNewYorkDateTimeInput(queryEndTime), [queryEndTime]);
  const queryRangeValid = Boolean(
    selected && queryStartMs !== undefined && queryEndMs !== undefined
    && queryStartMs <= queryEndMs,
  );

  const fetchCatalog = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const entries = await loadReplayCatalog();
      setCatalog(entries);
      if (entries[0]) {
        setSelectedKey((current) => (
          entries.some((entry) => identityKey(entry) === current)
            ? current
            : identityKey(entries[0])
        ));
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void fetchCatalog(); }, [fetchCatalog]);

  useEffect(() => {
    replayRequestIdRef.current += 1;
    replayConnectionRef.current?.send('replay_stop');
    setError(undefined);
    playingRef.current = false;
    setPlaying(false);
    setSession(undefined);
    setReplayFinished(false);
    setReplayTruncated(false);
    setCursor(0);
    cursorRef.current = 0;
    receivedFrameCountRef.current = 0;
    frameStoreRef.current.clear();
    setQueryStartTime(availableRange ? toNewYorkDateTimeInput(availableRange.startMs) : '');
    setQueryEndTime(availableRange ? toNewYorkDateTimeInput(availableRange.endMs) : '');
  }, [availableRange]);

  const queryReplay = useCallback(() => {
    if (queryStartMs === undefined || queryEndMs === undefined) {
      setError('请选择开始和结束时间');
      return;
    }
    if (queryStartMs > queryEndMs) {
      setError('开始时间不能晚于结束时间');
      return;
    }
    if (!selected) {
      setError('回放目录暂无可用数据');
      return;
    }
    replayRequestIdRef.current += 1;
    setLoading(true);
    setError(undefined);
    setReplayFinished(false);
    setReplayTruncated(false);
    playingRef.current = false;
    setPlaying(false);
    setSession(undefined);
    setCursor(0);
    cursorRef.current = 0;
    receivedFrameCountRef.current = 0;
    frameStoreRef.current.clear();
    replayConnectionRef.current?.send('replay_start', {
      publisherId: selected.publisherId,
      instrumentId: selected.instrumentId,
      bucketMs: selected.bucketMs,
      startMs: queryStartMs,
      endMs: queryEndMs,
      barIntervalMs,
      speed,
    });
  }, [barIntervalMs, queryEndMs, queryStartMs, selected, speed]);

  const refreshFrameWindow = useCallback((center: number) => {
    const total = receivedFrameCountRef.current;
    if (total === 0) return;
    const start = Math.max(0, Math.min(center - Math.floor(FRAME_WINDOW_SIZE / 2), total - FRAME_WINDOW_SIZE));
    const end = Math.min(total, start + FRAME_WINDOW_SIZE);
    const nextWindow: ReplayFrame[] = [];
    for (let index = start; index < end; index += 1) {
      const stored = frameStoreRef.current.get(index);
      if (stored) nextWindow.push(stored);
    }
    setSession((current) => current ? { ...current, frames: nextWindow } : current);
  }, []);

  const seekFrame = useCallback((index: number, pause = false) => {
    const maximum = Math.max(0, receivedFrameCountRef.current - 1);
    const next = Math.min(Math.max(0, index), maximum);
    if (pause) {
      playingRef.current = false;
      setPlaying(false);
      replayConnectionRef.current?.send('replay_pause');
    }
    cursorRef.current = next;
    setCursor(next);
    refreshFrameWindow(next);
  }, [refreshFrameWindow]);

  const togglePlayback = useCallback(() => {
    if (!session || replayFinished) return;
    const next = !playing;
    playingRef.current = next;
    setPlaying(next);
    replayConnectionRef.current?.send(next ? 'replay_play' : 'replay_pause');
  }, [playing, replayFinished, session]);

  const changeSpeed = useCallback((nextSpeed: number) => {
    setSpeed(nextSpeed);
    if (session) replayConnectionRef.current?.send('replay_speed', { speed: nextSpeed });
  }, [session]);

  const seekTime = useCallback((timeMs: number) => {
    if (receivedFrameCountRef.current === 0) return;
    let low = 0;
    let high = receivedFrameCountRef.current - 1;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const middleFrame = frameStoreRef.current.get(middle);
      if (!middleFrame || middleFrame.timeMs < timeMs) low = middle + 1;
      else high = middle;
    }
    seekFrame(low, true);
  }, [seekFrame]);

  const imbalance = frame ? (() => {
    const bid = frame.bids.reduce((total, level) => total + level.size, 0);
    const ask = frame.asks.reduce((total, level) => total + level.size, 0);
    return bid + ask === 0 ? undefined : (bid - ask) / (bid + ask);
  })() : undefined;
  const bestBid = frame?.bids[0];
  const bestAsk = frame?.asks[0];
  const frameStatus = !frame ? 'empty' : !frame.complete ? 'incomplete' : frame.crossed ? 'crossed' : 'ready';
  const frameStatusLabel = frameStatus === 'empty'
    ? '等待帧'
    : frameStatus === 'incomplete'
      ? '预热中'
      : frameStatus === 'crossed' ? '交叉盘' : '完整';
  const currentPrice = frame?.complete && !frame.crossed && bestBid && bestAsk
    ? (nanoPrice(bestBid.priceNano) + nanoPrice(bestAsk.priceNano)) / 2
    : undefined;
  const currentBar = useMemo(() => {
    if (!frame || !session?.bars.length) return undefined;
    let result: ReplaySession['bars'][number] | undefined;
    for (const bar of session.bars) {
      if (bar.timeMs > frame.timeMs) break;
      result = bar;
    }
    return result;
  }, [frame, session?.bars]);
  const currentPriceColor = currentBar
    ? currentBar.closeNano > currentBar.openNano
      ? '#168a32'
      : currentBar.closeNano < currentBar.openNano ? '#20252b' : '#667085'
    : undefined;
  const spread = frame?.complete && !frame.crossed && bestBid && bestAsk
    ? nanoPrice(bestAsk.priceNano - bestBid.priceNano)
    : undefined;
  const microPrice = frame?.complete && !frame.crossed && bestBid && bestAsk && bestBid.size + bestAsk.size > 0
    ? (nanoPrice(bestAsk.priceNano) * bestBid.size + nanoPrice(bestBid.priceNano) * bestAsk.size)
      / (bestBid.size + bestAsk.size)
    : undefined;
  const bidDepth = frame?.bids.reduce((total, level) => total + level.size, 0);
  const askDepth = frame?.asks.reduce((total, level) => total + level.size, 0);
  const bidLevelCount = frame?.bids.length ?? 0;
  const askLevelCount = frame?.asks.length ?? 0;
  const netAdded = frame ? frame.addedSize - frame.cancelledSize : undefined;
  const cancelRate = frame && frame.addedSize + frame.cancelledSize > 0
    ? frame.cancelledSize / (frame.addedSize + frame.cancelledSize) * 100
    : undefined;
  const previousFrame = cursor > 0 ? frameStoreRef.current.get(cursor - 1) : undefined;
  const priceRange = useMemo(() => {
    if (!frame) return undefined;
    const levels = [...frame.bids, ...frame.asks];
    if (levels.length === 0) return undefined;
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (const level of levels) {
      const price = nanoPrice(level.priceNano);
      min = Math.min(min, price);
      max = Math.max(max, price);
    }
    return { min, max };
  }, [frame]);
  const pricePadding = priceRange
    ? Math.max((priceRange.max - priceRange.min) * 0.06, 0.25)
    : 0;
  const viewDepthLabel = `当前快照最多${session?.depth ?? 100}档`;

  return (
    <main className="replay-terminal">
          <header className="terminal-toolbar">
        <div className="terminal-toolbar__instrument">
          <span className="terminal-brand">MBO REPLAY</span>
          <label htmlFor="replay-contract">合约</label>
          <select
            id="replay-contract"
            value={selectedKey}
            disabled={loading || contractOptions.length === 0}
            onChange={(event) => setSelectedKey(event.target.value)}
            aria-label="回放合约"
          >
            {contractOptions.map((entry) => (
              <option value={identityKey(entry)} key={identityKey(entry)}>
                {entry.symbol} ({entry.instrumentId})
              </option>
            ))}
          </select>
          <form className="replay-query" onSubmit={(event) => {
            event.preventDefault();
            void queryReplay();
          }} noValidate>
            <label htmlFor="replay-query-start">开始 <small>纽约 ET</small></label>
            <input
              id="replay-query-start"
              type="datetime-local"
              value={queryStartTime}
              step="1"
              required
              title="按纽约当地时间输入。冬令时为 EST（UTC−5），夏令时为 EDT（UTC−4）。"
              aria-description="按纽约当地时间输入；冬令时为 EST，夏令时为 EDT。"
              aria-invalid={queryStartTime !== '' && queryEndMs !== undefined && queryStartMs !== undefined && queryStartMs > queryEndMs}
              onChange={(event) => {
                setQueryStartTime(event.target.value);
                setError(undefined);
              }}
            />
            <label htmlFor="replay-query-end">结束 <small>纽约 ET</small></label>
            <input
              id="replay-query-end"
              type="datetime-local"
              value={queryEndTime}
              step="1"
              required
              title="按纽约当地时间输入。冬令时为 EST（UTC−5），夏令时为 EDT（UTC−4）。"
              aria-description="按纽约当地时间输入；冬令时为 EST，夏令时为 EDT。"
              aria-invalid={queryEndTime !== '' && queryStartMs !== undefined && queryEndMs !== undefined && queryStartMs > queryEndMs}
              onChange={(event) => {
                setQueryEndTime(event.target.value);
                setError(undefined);
              }}
            />
            <label htmlFor="replay-kline-interval">K线</label>
            <select
              id="replay-kline-interval"
              value={barIntervalMs}
              disabled={Boolean(session)}
              onChange={(event) => setBarIntervalMs(Number(event.target.value))}
              aria-label="K线周期"
            >
              <option value={1000}>1秒</option>
              <option value={5000}>5秒</option>
              <option value={15000}>15秒</option>
              <option value={30000}>30秒</option>
              <option value={60000}>60秒</option>
            </select>
            <button type="submit" disabled={!queryRangeValid || loading}>确定</button>
          </form>
        </div>
        <div className="terminal-toolbar__actions">
          <span className="terminal-interval">{session ? `${session.barIntervalMs / 1000}s K` : 'K 线'}</span>
          <button type="button" onClick={togglePlayback} disabled={!session || loading || replayFinished}>
            {playing ? '暂停' : '播放'}
          </button>
          <button type="button" onClick={() => seekFrame(cursor - 1, true)} disabled={!frame}>上一步</button>
          <button type="button" onClick={() => seekFrame(cursor + 1, true)} disabled={!frame}>下一步</button>
          <div className="speed-control" aria-label="回放倍速">
            {SPEEDS.map((value) => (
              <button type="button" className={speed === value ? 'is-active' : ''} onClick={() => changeSpeed(value)} key={value}>
                {value}×
              </button>
            ))}
          </div>
        </div>
      </header>

      {error && <div className="replay-error-status" role="status">{error}</div>}

      <section className="replay-timeline">
        <input
          className="replay-timeline__range"
          type="range"
          min={0}
          max={Math.max(0, receivedFrameCountRef.current - 1)}
          value={Math.min(cursor, Math.max(0, receivedFrameCountRef.current - 1))}
          disabled={receivedFrameCountRef.current === 0}
          onChange={(event) => seekFrame(Number(event.target.value), true)}
          aria-label="回放帧位置"
        />
        <time>{frame ? formatNewYorkDateTime(frame.timeMs) : '等待数据'}</time>
      </section>

      <section className="terminal-workspace">
        <article className="terminal-chart">
          <div className="terminal-chart__heading">
            <div><span>{session?.symbol ?? selected?.symbol ?? '中间价 K 线'}</span><small>点击 K 线可跳转回放位置 · 纽约时间 ET</small></div>
            <span>{frame ? `ordinal ${frame.sourceOrdinal.toLocaleString()} · seq ${frame.sequence.toLocaleString()}` : '等待快照'}</span>
          </div>
          <ReplayCandleChart
            bars={session?.bars ?? []}
            onSeek={seekTime}
            queryStartMs={queryStartMs}
            currentPrice={currentPrice}
            currentPriceStartMs={currentBar?.timeMs}
            currentPriceColor={currentPriceColor}
            priceRange={priceRange ? { min: priceRange.min - pricePadding, max: priceRange.max + pricePadding } : undefined}
          />
        </article>

        <aside className="depth-sidebar">
          <div className="depth-controls">
            <label className="depth-search">
              <span className="sr-only">搜索价格</span>
              <input
                type="search"
                value={priceSearch}
                onChange={(event) => setPriceSearch(event.target.value)}
                placeholder="搜索价格"
                inputMode="decimal"
              />
            </label>
          </div>
          <div className="depth-status">
            <span className={`replay-frame-state replay-frame-state--${frameStatus}`}>
              状态 <strong>{frameStatusLabel}</strong>
            </span>
            <span>当前视图 <strong>{viewDepthLabel}</strong></span>
            <span title="当前回放帧的实际价位数；服务端每侧最多返回400档">
              买 <strong>{bidLevelCount}</strong> 档 · 卖 <strong>{askLevelCount}</strong> 档
            </span>
          </div>
          {frameStatus !== 'empty' && frameStatus !== 'ready' && (
            <div className={`replay-frame-note replay-frame-note--${frameStatus}`} role="status">
              {frameStatus === 'crossed'
                ? '交叉盘保留原始深度，仅供诊断；中间价、Spread、Microprice 与 K 线标记已屏蔽。'
                : '首个 Clear 尚未到达，当前盘口仍处于预热状态。'}
            </div>
          )}
          <DomLadder
            frame={frame}
            previousFrame={previousFrame}
            searchPrice={priceSearch}
            priceRange={priceRange ?? undefined}
          />
          <div className="depth-totals">
            <span>买方深度 <strong>{formatQuantity(bidDepth)}</strong></span>
            <span>卖方深度 <strong>{formatQuantity(askDepth)}</strong></span>
          </div>
          <section className="feature-grid" aria-label="当前实时特征">
            <div className="feature-cell"><span>Spread</span><strong>{formatDecimal(spread)}</strong><small>ask − bid</small></div>
            <div className="feature-cell"><span>Microprice</span><strong>{formatDecimal(microPrice)}</strong><small>按 BBO 数量加权</small></div>
            <div className="feature-cell"><span>Imbalance</span><strong>{imbalance === undefined ? '—' : imbalance.toFixed(3)}</strong><small>400档买卖比</small></div>
            <div className="feature-cell"><span>净挂单量</span><strong className={netAdded !== undefined && netAdded < 0 ? 'feature-negative' : 'feature-positive'}>{formatSignedQuantity(netAdded)}</strong><small>A − C</small></div>
            <div className="feature-cell"><span>撤单率</span><strong>{formatRate(cancelRate)}</strong><small>C / (A + C)</small></div>
            <div className="feature-cell"><span>OFI</span><strong className="feature-pending">—</strong><small>待逐价位事件数据</small></div>
          </section>
          <p className="feature-caption">盘口价格范围与 K 线价格轴同步。</p>
        </aside>
      </section>

      <footer className="terminal-footer">
        <span>回放位置 {receivedFrameCountRef.current ? `${cursor + 1} / ${receivedFrameCountRef.current}` : '—'}</span>
        <span>数据源 {session?.fileSha256 === 'STREAM' ? '按时间范围流式回放' : '—'}</span>
        <span>{loading ? '加载中…' : replayTruncated ? '已到单次回放上限，请缩小时间范围' : '就绪'}</span>
      </footer>
    </main>
  );
}
