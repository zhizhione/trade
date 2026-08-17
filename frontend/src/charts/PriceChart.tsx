import { useEffect } from 'react';
import * as echarts from 'echarts';
import { useEChart } from '../hooks/useEChart';
import { formatNewYorkTime } from '../time';

export interface PricePoint {
  time: string;
  value: number;
}

interface PriceChartProps {
  points: PricePoint[];
}

export function PriceChart({ points }: PriceChartProps) {
  const { elementRef, chartRef } = useEChart();

  useEffect(() => {
    // x 轴仅作可读时间标签；点的真实顺序由 App 按服务端 snapshot 到达顺序维护。
    chartRef.current?.setOption({
      animationDuration: 180,
      grid: { left: 56, right: 18, top: 20, bottom: 34 },
      tooltip: { trigger: 'axis' },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: points.map((point) => formatNewYorkTime(point.time)),
        axisLabel: { color: '#70849f', hideOverlap: true },
        axisLine: { lineStyle: { color: '#223551' } },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: '#70849f' },
        splitLine: { lineStyle: { color: '#172a43' } },
      },
      series: [
        {
          type: 'line',
          data: points.map((point) => point.value),
          showSymbol: false,
          smooth: 0.16,
          lineStyle: { color: '#40e0c1', width: 2 },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(64, 224, 193, 0.32)' },
              { offset: 1, color: 'rgba(64, 224, 193, 0.01)' },
            ]),
          },
        },
      ],
    });
  }, [points]);

  return <div className="chart" ref={elementRef} aria-label="价格走势图" />;
}
