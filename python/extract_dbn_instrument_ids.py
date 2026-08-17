#!/usr/bin/env python3
"""从 DBN 文件开头快速抽取 instrument_id，用于核对数据文件身份。

每个文件默认只读取前两条数据记录，因此它不是完整元数据扫描工具；它的用途是
在大量历史文件导入前，低成本确认文件中出现的 Databento 合约数字 ID。
"""

from __future__ import annotations

import argparse
import glob
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any


DBN_SUFFIXES = (".dbn", ".dbn.zst")


def _is_dbn_file(path: Path) -> bool:
    """判断路径是否具有受支持的 DBN 文件后缀，不负责确认文件内容是否可解码。"""

    name = path.name.lower()
    return name.endswith(DBN_SUFFIXES)


def expand_input_paths(inputs: Sequence[str]) -> list[Path]:
    """将文件、目录和 glob 展开为去重且排序后的绝对文件路径。"""

    paths: list[Path] = []
    seen: set[Path] = set()

    for raw_input in inputs:
        matches = [Path(raw_input)]
        if any(character in raw_input for character in "*?["):
            matches = [Path(match) for match in glob.glob(raw_input, recursive=True)]
        if not matches:
            raise FileNotFoundError(f"输入路径不存在或没有匹配文件: {raw_input}")

        for match in matches:
            if match.is_dir():
                candidates: Iterable[Path] = (
                    path for path in match.rglob("*") if path.is_file() and _is_dbn_file(path)
                )
            elif match.is_file():
                candidates = (match,)
            else:
                raise FileNotFoundError(f"输入路径不存在: {match}")

            for candidate in sorted(candidates):
                resolved = candidate.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    paths.append(resolved)

    if not paths:
        raise FileNotFoundError("没有找到 DBN 文件")
    return paths


def first_instrument_ids(path: Path, limit: int = 2) -> list[int]:
    """最多读取 ``limit`` 条数据记录并返回其中存在的 instrument_id。"""

    try:
        import databento as db
    except ImportError as exception:
        raise RuntimeError(
            "未安装 databento，请先运行 `python3 -m pip install -r requirements.txt`"
        ) from exception

    instrument_ids: list[int] = []
    store = db.DBNStore.from_file(path)
    for record_number, record in enumerate(store):
        if record_number >= limit:
            break
        value: Any = getattr(record, "instrument_id", None)
        if value is None:
            continue
        try:
            instrument_ids.append(int(value))
        except (TypeError, ValueError) as exception:
            raise ValueError(
                f"{path} 第 {record_number + 1} 条记录的 instrument_id 无法转换为整数: {value!r}"
            ) from exception
    return instrument_ids


def collect_instrument_ids(paths: Iterable[Path], limit: int = 2) -> list[int]:
    """跨文件去重但保留首次出现顺序，便于与人工维护的映射逐项比对。"""

    unique_ids: list[int] = []
    seen: set[int] = set()
    for path in paths:
        for instrument_id in first_instrument_ids(path, limit=limit):
            if instrument_id not in seen:
                seen.add(instrument_id)
                unique_ids.append(instrument_id)
    return unique_ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="+",
        help="DBN 文件、包含 DBN 文件的目录，或 glob 模式",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2,
        help="每个文件读取的前 N 条数据记录（默认: 2）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit < 1:
        print("错误: --limit 必须是正整数", file=sys.stderr)
        return 2

    try:
        paths = expand_input_paths(args.paths)
        instrument_ids = collect_instrument_ids(paths, limit=args.limit)
    except (FileNotFoundError, RuntimeError, ValueError, OSError) as exception:
        print(f"错误: {exception}", file=sys.stderr)
        return 1

    for instrument_id in instrument_ids:
        print(instrument_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
