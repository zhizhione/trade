import type {
  ReplayStreamConnection,
  ReplayStreamMessage,
  ReplayStreamStatus,
} from '../domain/replay';

function defaultReplayWebSocketUrl(): string {
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.hostname}:8080/ws/replay`;
}

/**
 * 建立单次历史回放 WebSocket 连接。命令在握手完成前会暂存，避免目录加载完成后立即发起
 * replay_start 时因连接仍处于 CONNECTING 状态而丢失请求。
 */
export function connectReplayWebSocket(
  onMessage: (message: ReplayStreamMessage) => void,
  onStatus: (status: ReplayStreamStatus) => void,
): ReplayStreamConnection {
  const url = import.meta.env.VITE_REPLAY_WS_URL ?? defaultReplayWebSocketUrl();
  const socket = new WebSocket(url);
  let closed = false;
  const queue: string[] = [];

  const flush = () => {
    if (socket.readyState !== WebSocket.OPEN) return;
    while (queue.length > 0) socket.send(queue.shift()!);
  };

  onStatus('connecting');
  socket.onopen = () => {
    onStatus('connected');
    flush();
  };
  socket.onmessage = (event) => {
    try {
      onMessage(JSON.parse(event.data) as ReplayStreamMessage);
    } catch (reason) {
      console.warn('忽略无法解析的回放 WebSocket 消息', reason);
    }
  };
  socket.onerror = () => onStatus('disconnected');
  socket.onclose = () => onStatus('disconnected');

  return {
    send(type, payload = {}) {
      if (closed) return;
      const message = JSON.stringify({ type, payload });
      if (socket.readyState === WebSocket.OPEN) socket.send(message);
      else queue.push(message);
    },
    close() {
      closed = true;
      queue.length = 0;
      socket.close();
    },
  };
}
