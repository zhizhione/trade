import type { ConnectionStatus, MarketMessage } from '../domain/market';

export type { ConnectionStatus, MarketMessage } from '../domain/market';

function defaultWebSocketUrl(): string {
  // 页面若通过 HTTPS 打开，必须使用 WSS，否则浏览器会拦截混合内容连接。
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.hostname}:8080/ws/market`;
}

/**
 * 建立带指数退避的实时行情连接。返回的清理函数必须在 React 组件卸载时调用，才能停止
 * 重连定时器并关闭旧连接，避免同一页面存在多个并发订阅者。
 */
export function connectMarketWebSocket(
  onMessage: (message: MarketMessage) => void,
  onStatus: (status: ConnectionStatus) => void,
): () => void {
  // 环境变量可覆盖本地后端地址，便于开发、反向代理和生产部署共用同一前端构建。
  const url = import.meta.env.VITE_WS_URL ?? defaultWebSocketUrl();
  let socket: WebSocket | undefined;
  let retryTimer: number | undefined;
  let retryCount = 0;
  let stopped = false;

  const connect = () => {
    if (stopped) return;
    onStatus('connecting');
    socket = new WebSocket(url);

    socket.onopen = () => {
      // 只有真正建立连接才清零退避计数，连续失败会逐步延长重试间隔。
      retryCount = 0;
      onStatus('connected');
    };

    socket.onmessage = (event) => {
      try {
        onMessage(JSON.parse(event.data) as MarketMessage);
      } catch (error) {
        console.warn('忽略无法解析的 WebSocket 消息', error);
      }
    };

    socket.onerror = () => socket?.close();
    socket.onclose = () => {
      onStatus('disconnected');
      if (stopped) return;
      // 指数退避上限 15 秒，防止后端不可用时前端产生高频重连风暴。
      const delay = Math.min(1_000 * 2 ** retryCount, 15_000);
      retryCount += 1;
      retryTimer = window.setTimeout(connect, delay);
    };
  };

  connect();

  return () => {
    // React 卸载时取消定时器并关闭 socket，避免旧页面实例在后台继续重连。
    stopped = true;
    if (retryTimer !== undefined) window.clearTimeout(retryTimer);
    socket?.close();
  };
}
