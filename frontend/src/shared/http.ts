/** 统一处理 REST 响应和错误文本，避免各 API 模块重复处理非 2xx 响应。 */
export async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}
