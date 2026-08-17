import { DepthChart } from './charts/DepthChart';
import { PriceChart } from './charts/PriceChart';
import { SignalChart } from './charts/SignalChart';
import { useMarketDashboard } from './features/market/useMarketDashboard';
import { formatNumber } from './shared/format';
import { formatNewYorkTime } from './time';

export default function App() {
  const { status, snapshot, events, prices, signals, spread } = useMarketDashboard();

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">REALTIME MARKET DATA</p>
          <h1>实时行情数据台</h1>
        </div>
        <div className={`connection connection--${status}`}>
          <span className="connection__dot" />
          {status === 'connected' ? '实时连接' : status === 'connecting' ? '正在连接' : '等待重连'}
        </div>
      </header>

      <section className="ticker-strip">
        <div className="symbol-block">
          <span className="metric-label">交易标的</span>
          <strong>{snapshot?.symbol ?? '等待数据'}</strong>
        </div>
        <div>
          <span className="metric-label">最新价</span>
          <strong className="price-value">{formatNumber(snapshot?.lastPrice)}</strong>
        </div>
        <div>
          <span className="metric-label">买卖价差</span>
          <strong>{formatNumber(spread, 6)}</strong>
        </div>
        <div>
          <span className="metric-label">订单流</span>
          <strong className={(snapshot?.orderFlow ?? 0) >= 0 ? 'positive' : 'negative'}>
            {formatNumber(snapshot?.orderFlow, 2)}
          </strong>
        </div>
        <div>
          <span className="metric-label">信号值</span>
          <strong>{formatNumber(snapshot?.signalValue, 4)}</strong>
        </div>
      </section>

      <section className="dashboard-grid">
        <article className="panel panel--wide">
          <div className="panel__heading">
            <div>
              <span className="panel__kicker">TICK STREAM</span>
              <h2>价格走势</h2>
            </div>
            <span className="panel__meta">{prices.length} 点</span>
          </div>
          <PriceChart points={prices} />
        </article>

        <article className="panel">
          <div className="panel__heading">
            <div>
              <span className="panel__kicker">ORDER BOOK</span>
              <h2>盘口深度</h2>
            </div>
          </div>
          <DepthChart bids={snapshot?.bids ?? []} asks={snapshot?.asks ?? []} />
        </article>

        <article className="panel panel--wide">
          <div className="panel__heading">
            <div>
              <span className="panel__kicker">FLOW &amp; SIGNAL</span>
              <h2>订单流 / MBO 信号</h2>
            </div>
          </div>
          <SignalChart points={signals} />
        </article>

        <article className="panel event-panel">
          <div className="panel__heading">
            <div>
              <span className="panel__kicker">EVENT FEED</span>
              <h2>实时事件</h2>
            </div>
          </div>
          <div className="event-list">
            {events.length === 0 && <p className="empty-state">Kafka 事件到达后将在此显示</p>}
            {events.map((event) => (
              <div className="event-row" key={event.eventId}>
                <span className={`event-tag event-tag--${event.eventType}`}>{event.eventType}</span>
                <div>
                  <strong>{event.symbol}</strong>
                  <small>{formatNewYorkTime(event.eventTime)}</small>
                </div>
                <span className={event.side === 'SELL' ? 'negative' : 'positive'}>
                  {event.side ?? ''} {formatNumber(event.quantity, 3)}
                </span>
                <span>{formatNumber(event.price)}</span>
              </div>
            ))}
          </div>
        </article>
      </section>
    </main>
  );
}
