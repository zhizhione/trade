import { useEffect, useRef } from 'react';
import {
  CandlestickSeries,
  createChart,
  LineStyle,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type MouseEventParams,
  type Time,
  type UTCTimestamp,
} from 'lightweight-charts';
import { nanoPrice, type ReplayBar } from '../domain/replay';
import { formatNewYorkChartTime } from '../time';

interface ReplayCandleChartProps {
  bars: ReplayBar[];
  onSeek: (timeMs: number) => void;
  /** 所选查询起点的绝对 Unix 时间，用于保留真实存在的无行情空档。 */
  queryStartMs?: number;
  /** 当前有效 BBO 的中间价，已转换为前端展示使用的价格单位。 */
  currentPrice?: number;
  /** 当前 K 线的起始时刻，使用绝对 Unix 毫秒表示。 */
  currentPriceStartMs?: number;
  /** 当前 K 线颜色，同时复用于最新价格线以维持视觉关联。 */
  currentPriceColor?: string;
  priceRange?: { min: number; max: number };
}

type PriceRange = { min: number; max: number };
type CurrentPriceState = {
  price: number;
  startMs: number;
  color: string;
};

function validRange(range: PriceRange | undefined): range is PriceRange {
  return Boolean(range && Number.isFinite(range.min) && Number.isFinite(range.max) && range.max > range.min);
}

function toChartTime(timeMs: number): UTCTimestamp {
  return Math.floor(timeMs / 1000) as UTCTimestamp;
}

function toEpochMs(time: Time): number | undefined {
  if (typeof time === 'number') return time * 1000;
  if (typeof time === 'object' && time !== null && 'year' in time) {
    return Date.UTC(time.year, time.month - 1, time.day);
  }
  return undefined;
}

function formatChartTime(time: Time): string {
  const milliseconds = toEpochMs(time);
  return milliseconds === undefined ? '' : formatNewYorkChartTime(milliseconds);
}

function nearestBar(bars: ReplayBar[], time: Time | undefined): ReplayBar | undefined {
  if (time === undefined || typeof time !== 'number' || bars.length === 0) return undefined;
  let low = 0;
  let high = bars.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (bars[middle].timeMs / 1000 < time) low = middle + 1;
    else high = middle;
  }
  const right = bars[low];
  const left = bars[Math.max(0, low - 1)];
  return Math.abs(left.timeMs / 1000 - time) <= Math.abs(right.timeMs / 1000 - time) ? left : right;
}

/**
 * TradingView Lightweight Charts 原生回放图。
 *
 * K 线时间轴拖动、时间轴缩放、右侧价格轴拖动和价格轴缩放全部由
 * Lightweight Charts 的 handleScroll / handleScale 原生实现处理。
 * 双击右侧价格轴会让当前可见 K 线重新自动适配价格范围。
 */
export function ReplayCandleChart({
  bars,
  onSeek,
  queryStartMs,
  currentPrice,
  currentPriceStartMs,
  currentPriceColor,
  priceRange,
}: ReplayCandleChartProps) {
  const elementRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick', Time> | null>(null);
  const barsRef = useRef(bars);
  const onSeekRef = useRef(onSeek);
  const priceRangeRef = useRef(priceRange);
  const initializedRef = useRef(false);
  const firstBarTimeRef = useRef<number | undefined>(undefined);
  const initialPriceRangeAppliedRef = useRef(false);
  const userInteractedRef = useRef(false);
  const currentPriceLineRef = useRef<IPriceLine | null>(null);
  const currentPriceSegmentRef = useRef<HTMLSpanElement>(null);
  const currentPriceStateRef = useRef<CurrentPriceState | undefined>(undefined);
  const updateCurrentPriceSegmentRef = useRef<(() => void) | null>(null);

  useEffect(() => { barsRef.current = bars; }, [bars]);
  useEffect(() => { onSeekRef.current = onSeek; }, [onSeek]);
  useEffect(() => { priceRangeRef.current = priceRange; }, [priceRange]);

  // 用户手动拖动、缩放或滚动图表后，不再因新回放 K 线到达而移动其指针下方的可见时间窗口。
  useEffect(() => {
    const element = elementRef.current;
    if (!element) return;
    const markUserInteraction = () => { userInteractedRef.current = true; };
    element.addEventListener('pointerdown', markUserInteraction, { passive: true });
    element.addEventListener('wheel', markUserInteraction, { passive: true });
    return () => {
      element.removeEventListener('pointerdown', markUserInteraction);
      element.removeEventListener('wheel', markUserInteraction);
    };
  }, []);

  // K 线数据只有在加载新窗口/切换数据流时变化；回放 cursor 变化不应重写全部数据。
  useEffect(() => {
    const element = elementRef.current;
    if (!element) return;

    const chart = createChart(element, {
      autoSize: true,
      layout: { background: { color: '#ffffff' }, textColor: '#475467', attributionLogo: false },
      grid: { vertLines: { color: '#f2f4f7' }, horzLines: { color: '#eef0f2' } },
      rightPriceScale: { visible: true, borderColor: '#d0d5dd', scaleMargins: { top: 0.08, bottom: 0.08 } },
      leftPriceScale: { visible: false },
      localization: {
        locale: 'en-US',
        timeFormatter: formatChartTime,
      },
      timeScale: {
        borderColor: '#d0d5dd',
        timeVisible: true,
        secondsVisible: true,
        rightOffset: 4,
        barSpacing: 8,
        minBarSpacing: 2,
        tickMarkFormatter: formatChartTime,
      },
      crosshair: { vertLine: { color: '#98a2b3', style: 2 }, horzLine: { color: '#98a2b3', style: 2 } },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: true,
      },
      handleScale: {
        mouseWheel: true,
        pinch: true,
        axisPressedMouseMove: { time: true, price: true },
        // 时间轴和价格轴都使用 Lightweight Charts 原生双击复位；价格轴复位
        // 会按照当前可见 K 线的高低点自动计算价格范围。
        axisDoubleClickReset: { time: true, price: true },
      },
    });
    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#eaf8ed', downColor: '#20252b', borderUpColor: '#168a32', borderDownColor: '#20252b',
      wickUpColor: '#168a32', wickDownColor: '#20252b', priceLineVisible: false, lastValueVisible: false,
      // NQ 最小跳动为 0.25，价格轴因此只会落在 .00/.25/.50/.75。
      priceFormat: { type: 'price', precision: 2, minMove: 0.25 },
    });
    chartRef.current = chart;
    seriesRef.current = series;

    const updateCurrentPriceSegment = () => {
      const state = currentPriceStateRef.current;
      const segment = currentPriceSegmentRef.current;
      if (!segment || !state) {
        if (segment) segment.style.display = 'none';
        return;
      }
      const rawStartX = chart.timeScale().timeToCoordinate(toChartTime(state.startMs));
      const priceY = series.priceToCoordinate(state.price);
      // timeScale().width() 不包含右侧价格轴宽度。将线段限制在时间图窗内，避免绘制到
      // 价格轴标签下方。
      const rightEdge = Math.min(
        chart.timeScale().width(),
        element.clientWidth - chart.priceScale('right').width(),
      );
      if (rawStartX === null || priceY === null || rightEdge <= 0) {
        segment.style.display = 'none';
        return;
      }
      // timeToCoordinate 指向 K 线中心。以当前柱间距的一半作为起点，可使线段从当前 K 线
      // 右侧开始，而不会穿过实体或上下影线。
      const halfBarSpacing = Math.max(1, chart.timeScale().options().barSpacing / 2);
      const startX = Math.max(0, Math.min(rightEdge, rawStartX + halfBarSpacing));
      if (rightEdge <= startX) {
        segment.style.display = 'none';
        return;
      }
      segment.style.display = 'block';
      segment.style.left = `${startX}px`;
      segment.style.top = `${Math.round(priceY) + 0.5}px`;
      segment.style.width = `${rightEdge - startX}px`;
      segment.style.borderTopColor = state.color;
    };
    let frameId: number | undefined;
    const scheduleCurrentPriceSegmentUpdate = () => {
      if (frameId !== undefined) return;
      frameId = window.requestAnimationFrame(() => {
        frameId = undefined;
        updateCurrentPriceSegment();
      });
    };
    updateCurrentPriceSegmentRef.current = scheduleCurrentPriceSegmentUpdate;
    chart.timeScale().subscribeVisibleTimeRangeChange(scheduleCurrentPriceSegmentUpdate);
    chart.timeScale().subscribeVisibleLogicalRangeChange(scheduleCurrentPriceSegmentUpdate);
    chart.timeScale().subscribeSizeChange(scheduleCurrentPriceSegmentUpdate);
    chart.subscribeCrosshairMove(scheduleCurrentPriceSegmentUpdate);
    element.addEventListener('pointermove', scheduleCurrentPriceSegmentUpdate, { passive: true });
    const resizeObserver = typeof ResizeObserver === 'undefined'
      ? undefined
      : new ResizeObserver(scheduleCurrentPriceSegmentUpdate);
    resizeObserver?.observe(element);

    const click = (params: MouseEventParams<Time>) => {
      const bar = nearestBar(barsRef.current, params.time);
      if (bar) onSeekRef.current(bar.timeMs);
    };
    /** 双击价格轴时，让当前可见 K 线自动适配价格范围。 */
    const fitPriceScaleOnAxis = (params: MouseEventParams<Time>) => {
      const point = params.point;
      if (!point) return;
      const axisStart = element.clientWidth - chart.priceScale('right').width();
      if (point.x < axisStart) return;
      // 重新开启自动缩放；Lightweight Charts 会基于当前可见时间窗口内的
      // K 线 high/low 计算上下边界，并保留右侧价格轴的 scaleMargins 留白。
      series.priceScale().setAutoScale(true);
    };

    chart.subscribeClick(click);
    chart.subscribeDblClick(fitPriceScaleOnAxis);
    return () => {
      chart.unsubscribeClick(click);
      chart.unsubscribeDblClick(fitPriceScaleOnAxis);
      chart.timeScale().unsubscribeVisibleTimeRangeChange(scheduleCurrentPriceSegmentUpdate);
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(scheduleCurrentPriceSegmentUpdate);
      chart.timeScale().unsubscribeSizeChange(scheduleCurrentPriceSegmentUpdate);
      chart.unsubscribeCrosshairMove(scheduleCurrentPriceSegmentUpdate);
      element.removeEventListener('pointermove', scheduleCurrentPriceSegmentUpdate);
      resizeObserver?.disconnect();
      if (frameId !== undefined) window.cancelAnimationFrame(frameId);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      currentPriceLineRef.current = null;
      updateCurrentPriceSegmentRef.current = null;
      currentPriceStateRef.current = undefined;
      initializedRef.current = false;
      initialPriceRangeAppliedRef.current = false;
    };
  }, []);

  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    if (
      currentPrice === undefined
      || !Number.isFinite(currentPrice)
      || currentPriceStartMs === undefined
      || !Number.isFinite(currentPriceStartMs)
    ) {
      currentPriceStateRef.current = undefined;
      if (currentPriceLineRef.current) {
        series.removePriceLine(currentPriceLineRef.current);
        currentPriceLineRef.current = null;
      }
      currentPriceSegmentRef.current?.style.setProperty('display', 'none');
      return;
    }
    currentPriceStateRef.current = {
      price: currentPrice,
      startMs: currentPriceStartMs,
      color: currentPriceColor ?? '#667085',
    };
    updateCurrentPriceSegmentRef.current?.();
    if (!currentPriceLineRef.current) {
      currentPriceLineRef.current = series.createPriceLine({
        price: currentPrice,
        color: currentPriceColor ?? '#667085',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        lineVisible: false,
        axisLabelVisible: true,
        title: '',
        axisLabelColor: currentPriceColor ?? '#667085',
      });
    } else {
      currentPriceLineRef.current.applyOptions({
        price: currentPrice,
        color: currentPriceColor ?? '#667085',
        axisLabelColor: currentPriceColor ?? '#667085',
        lineVisible: false,
      });
    }
  }, [currentPrice, currentPriceColor, currentPriceStartMs]);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) return;
    const firstBarTime = bars[0]?.timeMs;
    // 切换回放文件时恢复新数据自己的自动范围；追加下一窗口不重置用户视窗。
    if (firstBarTime !== firstBarTimeRef.current) {
      firstBarTimeRef.current = firstBarTime;
      initializedRef.current = false;
      initialPriceRangeAppliedRef.current = false;
      userInteractedRef.current = false;
    }
    const data = bars
      .filter((bar, index) => index === 0 || bar.timeMs > bars[index - 1].timeMs)
      .map((bar) => ({
        time: toChartTime(bar.timeMs),
        open: nanoPrice(bar.openNano),
        high: nanoPrice(bar.highNano),
        low: nanoPrice(bar.lowNano),
        close: nanoPrice(bar.closeNano),
      }));
    series.setData(data);
    if (data.length > 0 && !userInteractedRef.current && queryStartMs !== undefined) {
      // 即使市场连续数小时没有有效 BBO，也要保留所选起点。空白区间是真实数据空档，
      // 不能伪造 K 线；在用户拖动、缩放或滚动前，右边界始终跟随最新收到的 K 线。
      const first = bars[0].timeMs;
      const last = bars.at(-1)?.timeMs ?? first;
      const fromMs = Math.min(queryStartMs, first);
      const spanMs = Math.max(60_000, last - fromMs);
      const paddingMs = Math.max(60_000, Math.min(30 * 60_000, Math.round(spanMs * 0.08)));
      chart.timeScale().setVisibleRange({
        from: toChartTime(fromMs),
        to: toChartTime(last + paddingMs),
      });
      initializedRef.current = true;
    } else if (!initializedRef.current && data.length > 0) {
      chart.timeScale().fitContent();
      initializedRef.current = true;
    }
    // setData/fitContent 可能在价格线 effect 执行后才确定可见范围，因此需以最终右边界
    // 刷新该线段。
    updateCurrentPriceSegmentRef.current?.();
  }, [bars, queryStartMs]);

  // 盘口价格范围只作为新数据流的初始窗口，之后完全交给 TradingView 原生价格轴。
  useEffect(() => {
    const series = seriesRef.current;
    if (!series || initialPriceRangeAppliedRef.current || !validRange(priceRange)) return;
    series.priceScale().setVisibleRange({ from: priceRange.min, to: priceRange.max });
    initialPriceRangeAppliedRef.current = true;
    // setVisibleRange 会改变纵坐标映射但不改变时间范围；初始按盘口深度设置价格范围后，
    // 必须显式刷新 DOM 线段位置。
    updateCurrentPriceSegmentRef.current?.();
  }, [priceRange]);

  return (
    <div className="chart chart--replay" ref={elementRef} aria-label="TradingView 历史中间价K线；可拖动时间轴和价格轴">
      <span ref={currentPriceSegmentRef} className="replay-current-price-segment" aria-hidden="true" />
    </div>
  );
}
