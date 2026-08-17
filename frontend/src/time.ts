/**
 * Web 端统一使用纽约时间显示和输入行情时间。
 *
 * 后端传来的时间仍然是绝对 Unix 时间（毫秒或带 Z 的 ISO 字符串）。
 * 只有展示层按 America/New_York 转换，避免把数据库里的事件时间改成
 * 浏览器所在机器的本地时间。
 */
export const NEW_YORK_TIME_ZONE = 'America/New_York';

type DateTimeParts = {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
};

const dateTimeFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: NEW_YORK_TIME_ZONE,
  calendar: 'gregory',
  numberingSystem: 'latn',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
});

function pad(value: number): string {
  return String(value).padStart(2, '0');
}

function partsAt(value: number): DateTimeParts {
  const parts = dateTimeFormatter.formatToParts(new Date(value));
  const get = (type: string) => Number(parts.find((part) => part.type === type)?.value);
  return {
    year: get('year'),
    month: get('month'),
    day: get('day'),
    hour: get('hour'),
    minute: get('minute'),
    second: get('second'),
  };
}

function partsAsUtcMs(parts: DateTimeParts): number {
  return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
}

/** 指定绝对时刻的时区偏移量，数值等于当地墙上时间减去 UTC。 */
function newYorkOffsetMs(value: number): number {
  return partsAsUtcMs(partsAt(value)) - value;
}

function epochMs(value: number | string | Date): number | undefined {
  let milliseconds: number;
  if (typeof value === 'number') {
    milliseconds = value;
  } else if (value instanceof Date) {
    milliseconds = value.getTime();
  } else {
    // Java Instant 通常带有 Z。没有时区的值统一按 UTC 处理，不能让浏览器所在机器的
    // 本地时区改变解析结果。
    const text = value.trim();
    const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(text);
    milliseconds = new Date(hasZone ? text : `${text}Z`).getTime();
  }
  return Number.isFinite(milliseconds) ? milliseconds : undefined;
}

/** 完整展示时间，并显式附加 ET 后缀以消除时区歧义。 */
export function formatNewYorkDateTime(value: number | string | Date): string {
  const milliseconds = epochMs(value);
  if (milliseconds === undefined) return '—';
  const parts = partsAt(milliseconds);
  return `${parts.year}/${pad(parts.month)}/${pad(parts.day)} ${pad(parts.hour)}:${pad(parts.minute)}:${pad(parts.second)} ET`;
}

/** 非 Lightweight Charts 的 ECharts 视图使用的紧凑时间标签。 */
export function formatNewYorkTime(value: number | string | Date): string {
  const milliseconds = epochMs(value);
  if (milliseconds === undefined) return '—';
  const parts = partsAt(milliseconds);
  return `${pad(parts.hour)}:${pad(parts.minute)}:${pad(parts.second)} ET`;
}

/** Lightweight Charts 坐标刻度和十字光标使用的时间标签。 */
export function formatNewYorkChartTime(value: number): string {
  const parts = partsAt(value);
  return `${pad(parts.month)}/${pad(parts.day)} ${pad(parts.hour)}:${pad(parts.minute)}:${pad(parts.second)}`;
}

/** datetime-local 输入框使用的值，以纽约当地墙上时间表示。 */
export function toNewYorkDateTimeInput(value: number): string {
  const parts = partsAt(value);
  return `${parts.year}-${pad(parts.month)}-${pad(parts.day)}T${pad(parts.hour)}:${pad(parts.minute)}:${pad(parts.second)}`;
}

/**
 * 将 datetime-local 值按纽约时间解析，并返回绝对 Unix 毫秒时间戳。
 * 通过迭代 IANA 时区偏移可同时处理 EST 与 EDT。秋季回拨时出现的歧义时间固定解析为
 * 第一次出现；春季跳时中不存在的本地时间会被拒绝，不能静默改写用户的查询区间。
 */
export function parseNewYorkDateTimeInput(value: string): number | undefined {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
  if (!match) return undefined;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText = '0'] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const wallMs = Date.UTC(year, month - 1, day, hour, minute, second);
  if (!Number.isFinite(wallMs)) return undefined;
  const wallCheck = new Date(wallMs);
  if (
    wallCheck.getUTCFullYear() !== year
    || wallCheck.getUTCMonth() !== month - 1
    || wallCheck.getUTCDate() !== day
    || wallCheck.getUTCHours() !== hour
    || wallCheck.getUTCMinutes() !== minute
    || wallCheck.getUTCSeconds() !== second
  ) return undefined;

  let candidate = wallMs;
  for (let index = 0; index < 4; index += 1) {
    candidate = wallMs - newYorkOffsetMs(candidate);
  }
  const expected = `${yearText}-${monthText}-${dayText}T${hourText}:${minuteText}:${pad(second)}`;
  return toNewYorkDateTimeInput(candidate) === expected
    ? candidate
    : undefined;
}
