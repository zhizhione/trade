import { useEffect } from 'react';
import type { DepthLevel } from '../domain/market';
import { useEChart } from '../hooks/useEChart';

interface DepthChartProps {
  bids: DepthLevel[];
  asks: DepthLevel[];
}

export function DepthChart({ bids, asks }: DepthChartProps) {
  const { elementRef, chartRef } = useEChart();

  useEffect(() => {
    // 买盘按高到低、卖盘按低到高传入；买盘反转后两侧在图上围绕最优价连续排列。
    const orderedBids = [...bids].reverse();
    const orderedAsks = [...asks];
    const rows = [
      ...orderedBids.map((level) => ({ ...level, side: 'bid' as const })),
      ...orderedAsks.map((level) => ({ ...level, side: 'ask' as const })),
    ];
    chartRef.current?.setOption({
      animationDuration: 180,
      grid: { left: 72, right: 26, top: 12, bottom: 28 },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#70849f' },
        splitLine: { lineStyle: { color: '#172a43' } },
      },
      yAxis: {
        type: 'category',
        data: rows.map((row) => row.price.toFixed(4)),
        axisLabel: { color: '#8da2bd', fontSize: 10 },
        axisLine: { lineStyle: { color: '#223551' } },
      },
      series: [
        {
          type: 'bar',
          data: rows.map((row) => ({
            value: row.quantity,
            itemStyle: { color: row.side === 'bid' ? '#23b99a' : '#eb6676' },
          })),
          barMaxWidth: 12,
        },
      ],
    });
  }, [asks, bids]);

  return <div className="chart" ref={elementRef} aria-label="盘口深度图" />;
}
