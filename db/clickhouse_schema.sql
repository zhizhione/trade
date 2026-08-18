-- 行情系统唯一的 ClickHouse schema：Databento 历史 MBO 与 ATAS 实时数据各自保留
-- 原始表，只有成交和盘口等下游分析表才做统一。重放、审计和特征计算必须以原始表
-- 为准，不能从前端快照或展示字段反推。
--
-- 新数据卷由 Docker Compose 自动执行本文件。已有数据卷可手动重复执行本文件来补齐
-- 新增表和下方的兼容列；本文件不会重建或删除已有原始行情表。

CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.instruments
(
    canonical_id UInt64 COMMENT '系统内部的合约唯一标识；同一个具体到期合约在 Databento、ATAS 和派生表中必须使用相同值',
    root_symbol LowCardinality(String) COMMENT '期货品种根代码，例如 NQ、ES、GC；不能单独用于区分不同到期月份',
    contract_symbol LowCardinality(String) COMMENT '具体到期合约代码，例如 NQU6、ESU6、GCQ6；换月后必须创建新的映射记录',
    exchange LowCardinality(String) COMMENT '合约所属交易所或市场名称，例如 CME、COMEX 或 Chicago Mercantile Exchange',
    expiry_date Nullable(Date) COMMENT '具体合约到期日；未知时为 NULL，用于换月、历史归档和连续合约处理',
    databento_dataset LowCardinality(String) COMMENT '历史兼容字段；运行时不用于来源身份映射',
    databento_publisher_id Nullable(UInt16) COMMENT '历史兼容字段；运行时不用于来源身份映射',
    databento_instrument_id Nullable(UInt32) COMMENT '历史兼容字段；运行时不用于来源身份映射',
    atas_instrument Nullable(String) COMMENT '历史兼容字段；运行时使用上游事件中的明确 canonical_id',
    tick_size_nano Int64 COMMENT '合约最小价格跳动乘以 10^9 后的整数，例如 0.25 存为 250000000、0.10 存为 100000000',
    contract_multiplier Decimal64(6) COMMENT '合约乘数，用于把价格变化换算为合约盈亏和名义价值，例如 NQ 为 20、ES 为 50',
    currency LowCardinality(String) COMMENT '合约计价货币代码，例如 USD',
    exchange_timezone LowCardinality(String) DEFAULT 'America/Chicago' COMMENT '交易所业务时区；用于计算交易日和交易时段，行情时间本身统一存储为 UTC',
    is_active UInt8 DEFAULT 1 COMMENT '映射是否仍在使用：1 表示有效，0 表示停用；历史记录不应物理删除',
    version UInt64 COMMENT 'ReplacingMergeTree 的版本号；同一 canonical_id 保留 version 最大的最新映射记录',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '映射记录最后更新时间，UTC 毫秒精度'
)
ENGINE = ReplacingMergeTree(version)
ORDER BY canonical_id;

-- 每次状态变化都插入一条完整的新版本；读取当前任务状态必须使用 FINAL。文件哈希
-- 是导入幂等身份，路径只是审计信息；同名文件内容变化会得到新的任务而不是被误跳过。
CREATE TABLE IF NOT EXISTS market_data.dbn_import_jobs
(
    file_sha256 FixedString(64) COMMENT 'DBN 文件内容的 SHA-256 小写十六进制值，作为幂等任务标识',
    source_path String COMMENT '提交任务时的源文件路径，仅用于读取和审计，不作为任务标识',
    display_name String COMMENT '用于日志和界面展示的文件名',
    file_size UInt64 COMMENT '源文件大小，单位字节',
    status LowCardinality(String) DEFAULT 'pending' COMMENT '任务状态，例如 pending、claimed、staging、committing、completed 或 failed',
    expected_rows Nullable(UInt64) COMMENT '预扫描得到的预计记录数；尚未统计时为 NULL',
    staged_rows UInt64 DEFAULT 0 COMMENT '已完成解码并进入暂存阶段的记录数',
    committed_rows UInt64 DEFAULT 0 COMMENT '已确认写入正式原始表的记录数',
    error_message String DEFAULT '' COMMENT '最近一次失败的错误信息；无错误时为空字符串',
    attempt UInt32 DEFAULT 0 COMMENT '任务领取或重试次数',
    claimed_by String DEFAULT '' COMMENT '当前领取任务的 worker 标识；未领取时为空字符串',
    claim_token Nullable(UUID) COMMENT '本次领取的唯一令牌；worker 写入前应通过 FINAL 查询确认令牌仍有效',
    lease_expires_at Nullable(DateTime64(3, 'UTC')) COMMENT '当前领取租约的 UTC 到期时间；到期任务允许重新领取',
    started_at Nullable(DateTime64(3, 'UTC')) COMMENT '首次开始处理的 UTC 时间',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '该状态版本写入时的 UTC 时间',
    completed_at Nullable(DateTime64(3, 'UTC')) COMMENT '任务成功完成的 UTC 时间；未完成时为 NULL',
    version UInt64 COMMENT '单调递增的状态版本；ReplacingMergeTree 保留 file_sha256 对应的最大版本'
)
ENGINE = ReplacingMergeTree(version)
ORDER BY file_sha256
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.databento_mbo_file_catalog
(
    file_sha256 FixedString(64) COMMENT '源文件内容 SHA-256，目录主身份；路径变化不改变身份',
    publisher_id UInt16 COMMENT 'Databento 发布商 ID；文件多身份时每个身份单独一行',
    instrument_id UInt32 COMMENT 'Databento 合约 ID；文件多身份时每个身份单独一行',
    source_path String COMMENT '最近一次导入或回填使用的绝对路径，仅用于审计',
    display_name String COMMENT '源文件名，用于日志和界面展示',
    trading_date Nullable(Date) COMMENT '仅从标准 DBN 文件名解析；无法识别时为 NULL',
    file_order UInt32 DEFAULT 0 COMMENT '同一交易日多文件时的显式顺序；默认 0',
    file_size UInt64 COMMENT '压缩源文件字节数',
    decoded_rows UInt64 DEFAULT 0 COMMENT 'DBN 解码流中的全部记录数',
    mbo_rows UInt64 DEFAULT 0 COMMENT '此文件内当前发布商/合约组合的 MBO 行数',
    skipped_rows UInt64 DEFAULT 0 COMMENT '非 MBO、未写入 raw 的记录数',
    first_ts_event Nullable(UInt64) COMMENT '源顺序第一条 MBO 的 ts_event，不是最小时间',
    last_ts_event Nullable(UInt64) COMMENT '源顺序最后一条 MBO 的 ts_event，不是最大时间',
    min_ts_event Nullable(UInt64) COMMENT '文件内 MBO 的最小 ts_event，用于时间范围筛选',
    max_ts_event Nullable(UInt64) COMMENT '文件内 MBO 的最大 ts_event，用于时间范围筛选',
    first_source_ordinal Nullable(UInt64) COMMENT '第一条 MBO 在完整 DBN 解码流中的位置',
    last_source_ordinal Nullable(UInt64) COMMENT '最后一条 MBO 在完整 DBN 解码流中的位置',
    status LowCardinality(String) DEFAULT 'completed' COMMENT '目录可用状态；事件提交成功后写 completed',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '目录版本写入时间',
    version UInt64 COMMENT 'ReplacingMergeTree 版本；同一文件/身份保留最大版本'
)
ENGINE = ReplacingMergeTree(version)
PRIMARY KEY file_sha256
ORDER BY (file_sha256, publisher_id, instrument_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.databento_mbo_raw
(
    file_sha256 FixedString(64) COMMENT '原始 DBN 文件字节 SHA-256；与 source_ordinal 共同构成事件身份',
    source_ordinal UInt64 COMMENT '该记录在 DBN 解码流中的零基位置，禁止重排后重新生成',
    ts_recv UInt64 COMMENT 'Databento 接收时间戳，自 UNIX 纪元起的纳秒数',
    ts_event UInt64 COMMENT '撮合引擎接收事件的时间戳，自 UNIX 纪元起的纳秒数',
    rtype UInt8 COMMENT 'DBN 记录类型哨兵值；MBO 固定为 160',
    publisher_id UInt16 COMMENT 'Databento 分配的发布商 ID，用于标识数据集和发布平台',
    instrument_id UInt32 COMMENT 'Databento 合约数字 ID',
    action FixedString(1) COMMENT '订单簿操作代码：A=新增、C=取消、M=修改、R=清空、T=成交、F=填充、N=无操作',
    side FixedString(1) COMMENT '事件方向代码：A=卖方、B=买方、N=无方向',
    price Int64 COMMENT '订单价格的固定点整数，1 单位等于 10^-9；无价格使用 DBN 未定义值',
    size UInt32 COMMENT '订单数量',
    channel_id UInt8 COMMENT 'Databento 分配的行情频道 ID',
    order_id UInt64 COMMENT '交易所分配的订单编号',
    flags UInt8 COMMENT '事件结束、消息特征和数据质量的位字段',
    ts_in_delta Int32 COMMENT '撮合引擎发送时间相对 ts_recv 的纳秒差',
    sequence UInt32 COMMENT '会话分配的消息序列号'
)
ENGINE = ReplacingMergeTree
PARTITION BY file_sha256
ORDER BY (file_sha256, source_ordinal)
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS market_data.atas_mbo_raw
(
    schema_version UInt16 COMMENT 'ATAS 采集消息的 JSON 结构版本；对应样本中的 schema_version',
    source_stream_id UUID COMMENT '采集程序每次启动或重连时生成的流会话 ID；用于隔离 sequence 重置和不同连接的订单簿状态',
    source_sequence UInt64 COMMENT 'ATAS 消息顺序号；同一 source_stream_id 内按此字段严格处理 MBO 与 Trade 消息',
    received_utc DateTime64(7, 'UTC') COMMENT '采集程序收到 ATAS 消息的 UTC 时间，保留原始 100 纳秒精度',
    event_time_utc DateTime64(7, 'UTC') COMMENT '经过时区确认和标准化后的交易所事件 UTC 时间；订单簿排序仍优先使用 source_sequence',
    event_time_raw String COMMENT 'ATAS 原始 event_time 文本；由于样本没有 Z 且 Kind=Unspecified，必须原样保留供时区核验',
    event_time_kind LowCardinality(String) COMMENT 'ATAS 时间类型标记，例如 Unspecified、Utc、Local；用于判断 event_time_raw 的时区语义',
    canonical_id UInt64 COMMENT '上游明确提供的系统内具体合约 ID；不通过本地来源映射表补充',
    root_symbol LowCardinality(String) COMMENT 'ATAS 消息中的品种根代码，例如 NQ、ES、GC',
    contract_symbol LowCardinality(String) COMMENT '实际订阅的具体到期合约，例如 NQU6；不能只保存 NQ，否则换月后数据会混合',
    exchange LowCardinality(String) COMMENT 'ATAS 消息中的交易所名称，例如 Chicago Mercantile Exchange',
    update_type LowCardinality(String) COMMENT 'ATAS MBO 动作类型：New=新增、Change=修改、Delete=删除',
    side LowCardinality(String) COMMENT '挂单方向：Bid=买方订单、Ask=卖方订单',
    priority UInt64 COMMENT '交易所订单优先级或队列排序值；Change 即使价格数量不变但 priority 变化也必须更新',
    exchange_order_id UInt64 COMMENT '交易所订单 ID；New 插入、Change 更新、Delete 删除均以此字段定位订单',
    price Decimal64(9) COMMENT 'ATAS 提供的十进制委托价格，统一保留 9 位小数精度',
    price_nano Int64 COMMENT '委托价格乘以 10^9 后的整数；供精确比较、聚合及与 Databento price_nano 对齐',
    volume UInt64 COMMENT '当前 MBO 事件携带的订单数量；具体为新状态还是变化量按 ATAS New/Change/Delete 规则解释',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '该消息写入 ClickHouse 的 UTC 时间，用于监控采集延迟和入库进度',
    INDEX idx_atas_mbo_event_time event_time_utc TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_utc)
ORDER BY (canonical_id, source_stream_id, source_sequence)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.atas_trade_raw
(
    schema_version UInt16 COMMENT 'ATAS 采集消息的 JSON 结构版本；对应样本中的 schema_version',
    source_stream_id UUID COMMENT '采集程序连接会话 ID；与 ATAS MBO 表使用同一 ID 时可按 sequence 合并还原事件顺序',
    source_sequence UInt64 COMMENT 'ATAS 消息顺序号；Trade 与 MBO 共用该序列时可恢复它们在同一数据流中的先后关系',
    received_utc DateTime64(7, 'UTC') COMMENT '采集程序收到成交消息的 UTC 时间，保留原始 100 纳秒精度',
    event_time_utc DateTime64(7, 'UTC') COMMENT '经过时区确认后的成交事件 UTC 时间，用于成交查询、K 线和回测',
    event_time_raw String COMMENT 'ATAS 原始 event_time 文本；保留以验证无时区标记时间的转换结果',
    event_time_kind LowCardinality(String) COMMENT 'ATAS 原始时间类型标记，例如 Unspecified、Utc 或 Local',
    canonical_id UInt64 COMMENT '系统内部具体合约 ID；必须与对应 ATAS MBO 消息使用相同映射',
    root_symbol LowCardinality(String) COMMENT 'ATAS 消息中的品种根代码，例如 NQ、ES、GC',
    contract_symbol LowCardinality(String) COMMENT '实际成交所属的具体到期合约代码，例如 NQU6',
    exchange LowCardinality(String) COMMENT '成交所属交易所名称，例如 Chicago Mercantile Exchange',
    direction LowCardinality(String) COMMENT '主动成交方向：Buy 表示主动买入，Sell 表示主动卖出；写入 trades 时映射为 aggressor_side',
    data_type LowCardinality(String) COMMENT 'ATAS 数据类型名称，当前样本为 Trade；保留用于输入验证和未来类型扩展',
    price Decimal64(9) COMMENT '成交十进制价格，统一保留 9 位小数精度',
    price_nano Int64 COMMENT '成交价格乘以 10^9 后的整数；用于与 Databento 和盘口价格精确对齐',
    volume UInt64 COMMENT '本次成交数量，单位为合约张数',
    passive_exchange_order_id UInt64 COMMENT '成交中提供流动性的被动挂单交易所订单 ID',
    aggressor_exchange_order_id UInt64 COMMENT '发起本次成交的主动订单交易所订单 ID',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '该成交消息写入 ClickHouse 的 UTC 时间，用于采集延迟和数据完整性监控',
    INDEX idx_atas_trade_event_time event_time_utc TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(received_utc)
ORDER BY (canonical_id, source_stream_id, source_sequence)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.trades
(
    source LowCardinality(String) COMMENT '标准化成交来源，例如 databento 或 atas；用于历史/实时切换和质量对比',
    source_event_id String COMMENT '来源内稳定的成交事件唯一键；重跑导入时用于幂等检查，不能只用时间价格数量代替',
    source_stream_id Nullable(UUID) COMMENT '实时采集连接会话 ID；ATAS 实时成交填写，离线 Databento 文件没有流会话时为 NULL',
    source_sequence UInt64 COMMENT '来源消息序列号；来源没有可靠序列时填 0，并依赖 source_event_id 保证事件身份',
    canonical_id UInt64 COMMENT '系统内部具体合约 ID；用于跨 Databento、ATAS 统一查询同一到期合约',
    ts_event DateTime64(9, 'UTC') COMMENT '标准化后的交易所成交时间，UTC 纳秒精度；K 线、成交分析和回测的主要时间字段',
    ts_recv Nullable(DateTime64(9, 'UTC')) COMMENT '来源接收成交消息的 UTC 时间；缺失或被标记为不可信时存 NULL',
    aggressor_side LowCardinality(String) COMMENT '主动成交方向，统一使用 Buy、Sell 或 Unknown；不要与盘口 Bid、Ask 混用',
    price Decimal64(9) COMMENT '标准化成交价格，精确保留 9 位小数',
    price_nano Int64 COMMENT '成交价格乘以 10^9 后的整数，供精确匹配和高性能整数计算',
    size UInt64 COMMENT '标准化成交数量，单位为合约张数',
    passive_order_id Nullable(UInt64) COMMENT '被动挂单订单 ID；来源不提供或无法确定时为 NULL',
    aggressor_order_id Nullable(UInt64) COMMENT '主动订单 ID；来源不提供或无法确定时为 NULL',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '标准化成交写入派生表的 UTC 时间，用于追踪处理延迟和重算批次',
    INDEX idx_trade_source_event_id source_event_id TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts_event)
ORDER BY (canonical_id, ts_event, source, source_sequence)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS market_data.book_snapshots
(
    source LowCardinality(String) COMMENT '盘口快照的数据来源，例如 databento 或 atas；两个来源的盘口状态必须独立重建',
    canonical_id UInt64 COMMENT '系统内部具体合约 ID；标识该快照对应的真实到期合约',
    ts_snapshot DateTime64(9, 'UTC') COMMENT '生成盘口快照时对应的市场 UTC 时间，纳秒精度',
    source_stream_id Nullable(UUID) COMMENT '实时采集连接会话 ID；ATAS 快照填写，离线 Databento 重建没有会话时可为 NULL',
    last_source_sequence UInt64 COMMENT '构建该快照时已处理的最后一个来源序列号；用于断点恢复和判断快照新旧',
    depth UInt16 COMMENT '快照保存的最大盘口档数，例如 10 表示最多保存十档买卖盘',
    bid_prices_nano Array(Int64) COMMENT '买盘价格数组，按价格从高到低排列；每个价格均为真实价格乘以 10^9',
    bid_sizes Array(UInt64) COMMENT '买盘各档总数量数组；数组下标必须与 bid_prices_nano 一一对应',
    bid_order_counts Array(UInt32) COMMENT '买盘各档挂单笔数数组；数组下标必须与 bid_prices_nano 一一对应',
    ask_prices_nano Array(Int64) COMMENT '卖盘价格数组，按价格从低到高排列；每个价格均为真实价格乘以 10^9',
    ask_sizes Array(UInt64) COMMENT '卖盘各档总数量数组；数组下标必须与 ask_prices_nano 一一对应',
    ask_order_counts Array(UInt32) COMMENT '卖盘各档挂单笔数数组；数组下标必须与 ask_prices_nano 一一对应',
    is_complete UInt8 COMMENT '盘口是否完整：1 表示从有效快照或完整起点重建，0 表示可能缺少初始订单或存在序列缺口',
    build_version UInt32 COMMENT '订单簿重建算法版本；算法规则调整后可区分并重新生成历史快照',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT '盘口快照写入 ClickHouse 的 UTC 时间，用于监控重建处理延迟'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_snapshot)
ORDER BY (canonical_id, ts_snapshot, source)
SETTINGS index_granularity = 8192;
