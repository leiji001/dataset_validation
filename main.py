"""Alpaca 数据集格式校验工具，支持 JSON 和 JSONL 格式。"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


def _is_interactive() -> bool:
    """判断是否应在退出前暂停（拖拽到 exe 上运行时需要暂停）。"""
    return getattr(sys, "frozen", False) or os.environ.get("PAUSE_ON_EXIT") == "1"

# ── Alpaca 格式定义 ──────────────────────────────────────────────────────────

REQUIRED_FIELDS = {"instruction", "output"}
OPTIONAL_FIELDS = {"input"}
ALL_KNOWN_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS


# ── 校验结果 ─────────────────────────────────────────────────────────────────

@dataclass
class ValidationError:
    """单条校验问题。"""
    index: int          # 样本在数据集中的位置（从 0 开始）
    field: str          # 相关字段；文件级别问题为空字符串
    message: str


@dataclass
class ValidationResult:
    """校验汇总。"""
    file: str
    total_samples: int = 0
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines: list[str] = []
        lines.append(f"文件: {self.file}")
        lines.append(f"样本总数: {self.total_samples}")
        lines.append(f"错误数: {len(self.errors)}")
        lines.append(f"警告数: {len(self.warnings)}")

        if self.errors:
            lines.append("\n── 错误 ──")
            for e in self.errors:
                loc = f"样本 #{e.index}" if e.index >= 0 else "文件"
                lines.append(f"  [{loc}] {e.field}: {e.message}" if e.field else f"  [{loc}] {e.message}")

        if self.warnings:
            lines.append("\n── 警告 ──")
            for w in self.warnings:
                loc = f"样本 #{w.index}" if w.index >= 0 else "文件"
                lines.append(f"  [{loc}] {w.field}: {w.message}" if w.field else f"  [{loc}] {w.message}")

        status = "✅ 校验通过" if self.is_valid else "❌ 校验未通过"
        lines.append(f"\n{status}")
        return "\n".join(lines)


# ── 单条样本校验 ─────────────────────────────────────────────────────────────

def _validate_sample(
    sample: dict, index: int, result: ValidationResult
) -> None:
    """校验单条 Alpaca 样本，将问题写入 *result*。"""

    # 类型检查
    if not isinstance(sample, dict):
        result.errors.append(
            ValidationError(index, "", f"样本应为 JSON 对象，实际类型为 {type(sample).__name__}")
        )
        return

    keys = set(sample.keys())

    # 必填字段缺失
    for f in REQUIRED_FIELDS:
        if f not in keys:
            result.errors.append(ValidationError(index, f, "缺少必填字段"))

    # 未知字段
    unknown = keys - ALL_KNOWN_FIELDS
    if unknown:
        result.warnings.append(
            ValidationError(index, "", f"包含未知字段: {', '.join(sorted(unknown))}")
        )

    # 字段值类型 & 内容检查
    for f in ALL_KNOWN_FIELDS & keys:
        val = sample[f]
        if not isinstance(val, str):
            result.errors.append(
                ValidationError(index, f, f"字段值应为字符串，实际类型为 {type(val).__name__}")
            )
        elif f in REQUIRED_FIELDS and val.strip() == "":
            result.errors.append(
                ValidationError(index, f, "必填字段不能为空字符串")
            )


# ── 文件加载与校验 ────────────────────────────────────────────────────────────

def _load_json(path: Path) -> list[dict] | None:
    """加载 .json 文件，返回样本列表或 None（表示格式错误）。"""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return None


def _load_jsonl(path: Path) -> list[dict] | None:
    """加载 .jsonl 文件，返回样本列表或 None（表示格式错误）。"""
    samples: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"第 {lineno} 行 JSON 解析失败: {exc}") from exc
            samples.append(obj)
    return samples


def validate(file_path: str | Path) -> ValidationResult:
    """校验指定文件，返回 :class:`ValidationResult`。

    支持 ``.json`` 和 ``.jsonl`` 两种格式。
    """
    path = Path(file_path)
    result = ValidationResult(file=str(path))

    # 文件存在性
    if not path.exists():
        result.errors.append(ValidationError(-1, "", f"文件不存在: {path}"))
        return result

    suffix = path.suffix.lower()
    if suffix not in (".json", ".jsonl"):
        result.errors.append(ValidationError(-1, "", f"不支持的文件格式: {suffix}，仅支持 .json 和 .jsonl"))
        return result

    # 解析
    try:
        if suffix == ".json":
            samples = _load_json(path)
        else:
            samples = _load_jsonl(path)
    except (json.JSONDecodeError, ValueError) as exc:
        result.errors.append(ValidationError(-1, "", f"文件解析失败: {exc}"))
        return result

    if samples is None:
        result.errors.append(ValidationError(-1, "", "JSON 文件顶层应为数组 (list)"))
        return result

    if len(samples) == 0:
        result.warnings.append(ValidationError(-1, "", "数据集为空，没有任何样本"))

    result.total_samples = len(samples)

    for idx, sample in enumerate(samples):
        _validate_sample(sample, idx, result)

    return result


# ── 文件夹扫描 ────────────────────────────────────────────────────────────────

def collect_files(paths: list[str | Path]) -> list[Path]:
    """将路径列表展开：文件直接保留，文件夹递归收集其中的 .json/.jsonl 文件。"""
    result: list[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_dir():
            found = sorted(
                f for f in path.rglob("*") if f.suffix.lower() in (".json", ".jsonl")
            )
            if not found:
                print(f"警告: 文件夹 {path} 中未找到 .json 或 .jsonl 文件")
            result.extend(found)
        else:
            result.append(path)
    return result


# ── CLI ──────────────────────────────────────────────────────────────────────

def _pause() -> None:
    """打包后运行时暂停，让用户看到结果。"""
    if _is_interactive():
        print("─" * 40)
        input("按回车键退出...")


def main() -> None:
    if len(sys.argv) < 2:
        print("Alpaca 数据集格式校验工具")
        print("─" * 40)
        print("用法: 将 .json / .jsonl 文件或文件夹拖拽到本程序上即可校验")
        print("也可通过命令行: dataset_validation <文件或文件夹路径> [路径 ...]")
        print("传入文件夹时会递归扫描其中所有 .json 和 .jsonl 文件")
        _pause()
        sys.exit(1)

    files = collect_files(sys.argv[1:])
    if not files:
        print("未找到任何可校验的文件")
        _pause()
        sys.exit(1)

    all_valid = True
    for filepath in files:
        result = validate(filepath)
        print(result.summary())
        print()
        if not result.is_valid:
            all_valid = False

    _pause()
    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
