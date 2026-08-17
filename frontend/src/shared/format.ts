/** 将可选数值格式化为仪表盘文本；缺失值统一显示为长破折号。 */
export function formatNumber(value: number | undefined, digits = 4): string {
  return value === undefined ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: digits });
}
