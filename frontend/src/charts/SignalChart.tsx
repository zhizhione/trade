import { useEffect } from 'react';
import { formatNewYorkTime } from '../time';
import { useEChart } from '../hooks/useEChart';

export interface SignalPoint {
  time: string;
  orderFlow: number;
  signal: number;
}

interface SignalChartProps {
  points: SignalPoint[];
}

export function SignalChart({ points }: SignalChartProps) {
  const { elementRef, chartRef } = useEChart();

  useEffect(() => {
    // 订单流与信号单位不同，信号固定映射到右轴 [-1, 1]，避免被成交量级压平。
    chartRef.current?.setOption({
      animationDuration: 180,
      legend: {
        data: ['订单流', '信号'],
        textStyle: { color: '#8da2bd' },
        right: 12,
      },
      grid: { left: 54, right: 42, top: 38, bottom: 34 },
      tooltip: { trigger: 'axis' },
      xAxis: {
        type: 'category',
        data: points.map((point) => formatNewYorkTime(point.time)),
        axisLabel: { color: '#70849f', hideOverlap: true },
        axisLine: { lineStyle: { color: '#223551' } },
      },
      yAxis: [
        {
          type: 'value',
          scale: true,
          axisLabel: { color: '#70849f' },
          splitLine: { lineStyle: { color: '#172a43' } },
        },
        {
          type: 'value',
          min: -1,
          max: 1,
          axisLabel: { color: '#70849f' },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: '订单流',
          type: 'bar',
          data: points.map((point) => point.orderFlow),
          itemStyle: { color: '#4388eb' },
          barMaxWidth: 8,
        },
        {
          name: '信号',
          type: 'line',
          yAxisIndex: 1,
          data: points.map((point) => point.signal),
          showSymbol: false,
          lineStyle: { color: '#f4bd50', width: 2 },
        },
      ],
    });
  }, [points]);

  return <div className="chart" ref={elementRef} aria-label="订单流与信号图" />;
}
