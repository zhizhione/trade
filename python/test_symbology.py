#!/usr/bin/env python3
"""将 Databento 原始合约代码解析为 instrument_id 的命令行连通性检查。

该文件不是 pytest 单元测试，而是人工核对数据集、合约代码和映射配置的小工具。网络
请求只在 ``main`` 中发起，确保被导入或被测试收集时不会意外消耗 API 配额。
"""

from __future__ import annotations

import os
import sys

# 750=NQH4
# 13743=NQM4
# 4358=NQU4
# 106364=NQZ4
# 42288528=NQH5
# 42005804=NQM5
# 42008487=NQU5
# 158704=NQZ5
# 42002475=NQH6
# 42004058=NQM6
# 42004177=NQU6

# 17077=ESH4
# 5602=ESM4
# 118=ESU4
# 183748=ESZ4
# 5002=ESH5
# 4916=ESM5
# 14160=ESU5
# 294973=ESZ5
# 42140878=ESH6
# 42140864=ESM6
# 42140870=ESU6


def resolve_symbol(
    api_key: str,
    *,
    dataset: str = "GLBX.MDP3",
    symbols: list[str] | None = None,
    start_date: str = "2025-09-20",
    end_date: str = "2025-12-20",
):
    """调用 Databento symbology 接口解析原始合约代码，并返回供应商原始响应。"""
    try:
        import databento as db_client
    except ImportError as exception:
        raise RuntimeError(
            "未安装 databento，请先运行 `python3 -m pip install -r requirements.txt`"
        ) from exception

    if not api_key.strip():
        raise ValueError("未设置 DATABENTO_API_KEY 环境变量")

    historical_client = db_client.Historical(api_key)
    return historical_client.symbology.resolve(
        dataset=dataset,
        symbols=symbols or ["NQZ5"],
        stype_in="raw_symbol",
        stype_out="instrument_id",
        start_date=start_date,
        end_date=end_date,
    )


def main() -> int:
    # 安装 python-dotenv 时加载仓库根目录的 .env；即使未安装该依赖，显式导出的环境变量
    # 仍然可用，便于 CI 或密钥管理环境运行本脚本。
    try:
        from dotenv import load_dotenv
    except ImportError:
        pass
    else:
        load_dotenv()

    api_key = os.getenv("DATABENTO_API_KEY", "")
    try:
        result = resolve_symbol(api_key)
    except (RuntimeError, ValueError) as exception:
        print(f"错误: {exception}", file=sys.stderr)
        return 2
    except Exception as exception:  # 供应商 API 或网络异常也转换为便于命令行阅读的错误信息。
        print(f"Databento 请求失败: {exception}", file=sys.stderr)
        return 1

    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
