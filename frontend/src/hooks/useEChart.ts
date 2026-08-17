import { useEffect, useRef } from 'react';
import * as echarts from 'echarts';
import type { EChartsType } from 'echarts';

/**
 * 管理 ECharts 实例的创建、窗口尺寸更新与销毁。图表组件只负责设置业务 option，避免
 * 每个组件重复注册 resize 监听器或遗漏 dispose。
 */
export function useEChart(renderer: 'canvas' | 'svg' = 'canvas') {
  const elementRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<EChartsType | null>(null);

  useEffect(() => {
    const element = elementRef.current;
    if (!element) return undefined;
    const chart = echarts.init(element, undefined, { renderer });
    chartRef.current = chart;
    const resize = () => chart.resize();
    window.addEventListener('resize', resize);
    return () => {
      window.removeEventListener('resize', resize);
      chart.dispose();
      chartRef.current = null;
    };
  }, [renderer]);

  return { elementRef, chartRef };
}
