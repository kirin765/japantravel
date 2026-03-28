"""Parse Google Search Console coverage exports from Archive.zip bundles."""

from __future__ import annotations

import argparse
import csv
import io
import json
from dataclasses import asdict, dataclass
from pathlib import Path
import zipfile


@dataclass(frozen=True)
class CoverageIssueRecord:
    issue_type: str
    status_bucket: str
    url: str
    recommended_action: str
    requires_wp_plugin: bool = False
    requires_server_change: bool = False


ISSUE_STATUS_BUCKETS = {
    "찾을 수 없음(404)": "404",
    "크롤링됨 - 현재 색인이 생성되지 않음": "crawled_not_indexed",
    "발견됨 - 현재 색인이 생성되지 않음": "discovered_not_indexed",
    "‘NOINDEX’ 태그에 의해 제외되었습니다.": "noindex_excluded",
    "'NOINDEX' 태그에 의해 제외되었습니다.": "noindex_excluded",
}


def parse_coverage_export(source: str | Path) -> list[CoverageIssueRecord]:
    records: list[CoverageIssueRecord] = []
    source_path = Path(source)
    if source_path.is_dir():
        for path in sorted(source_path.rglob("*.zip")):
            records.extend(_parse_zip_bytes(path.read_bytes(), path.name))
        return records
    return _parse_zip_bytes(source_path.read_bytes(), source_path.name)


def _parse_zip_bytes(raw_bytes: bytes, name: str) -> list[CoverageIssueRecord]:
    records: list[CoverageIssueRecord] = []
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
        metadata_name = ""
        table_name = ""
        nested_names: list[str] = []
        for member in archive.namelist():
            lower_name = member.lower()
            if lower_name.endswith(".zip") and not member.startswith("__MACOSX/"):
                nested_names.append(member)
            elif lower_name.endswith("메타데이터.csv"):
                metadata_name = member
            elif lower_name.endswith("테이블.csv"):
                table_name = member

        for nested_name in nested_names:
            records.extend(_parse_zip_bytes(archive.read(nested_name), nested_name))

        if not metadata_name or not table_name:
            return records

        metadata_rows = _read_csv_rows(archive.read(metadata_name))
        table_rows = _read_csv_rows(archive.read(table_name))
        issue_type = _extract_issue_type(metadata_rows)
        status_bucket = ISSUE_STATUS_BUCKETS.get(issue_type, "unknown")
        for row in table_rows:
            url = (row[0] if row else "").strip()
            if not url or url.lower() in {"url", "페이지"}:
                continue
            action = _recommend_action(url, status_bucket)
            records.append(
                CoverageIssueRecord(
                    issue_type=issue_type,
                    status_bucket=status_bucket,
                    url=url,
                    recommended_action=action["action"],
                    requires_wp_plugin=action["requires_wp_plugin"],
                    requires_server_change=action["requires_server_change"],
                )
            )
    return records


def _read_csv_rows(raw_bytes: bytes) -> list[list[str]]:
    text = ""
    for encoding in ("utf-8-sig", "cp949", "utf-8"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if not text:
        return []
    return [row for row in csv.reader(io.StringIO(text)) if row]


def _extract_issue_type(rows: list[list[str]]) -> str:
    for row in rows:
        if len(row) < 2:
            continue
        key = row[0].strip()
        value = row[1].strip()
        if key == "문제" and value:
            return value
    return ""


def _recommend_action(url: str, status_bucket: str) -> dict[str, bool | str]:
    lowered = url.lower()
    if status_bucket == "noindex_excluded":
        return {
            "action": "keep_noindex_expected",
            "requires_wp_plugin": False,
            "requires_server_change": False,
        }
    if "/wp-content/uploads/" in lowered:
        return {
            "action": "disable_directory_indexing",
            "requires_wp_plugin": False,
            "requires_server_change": True,
        }
    if "/wp-includes/" in lowered:
        return {
            "action": "add_x_robots_noindex",
            "requires_wp_plugin": False,
            "requires_server_change": True,
        }
    if "/feed/" in lowered:
        return {
            "action": "disable_feed_discovery_and_noindex_feed",
            "requires_wp_plugin": True,
            "requires_server_change": False,
        }
    if "/category/" in lowered or "/tag/" in lowered or "/author/" in lowered:
        return {
            "action": "noindex_archive_and_remove_from_sitemap",
            "requires_wp_plugin": True,
            "requires_server_change": False,
        }
    if any(
        token in lowered
        for token in (
            "/hello-world/",
            "/sample-page/",
            "/smoke-test",
            "/안녕하세요/",
            "/예제-페이지/",
            "/%ec%95%88%eb%85%95",
            "/%ec%98%88%ec%a0%9c",
        )
    ):
        return {
            "action": "privatize_placeholder_content",
            "requires_wp_plugin": False,
            "requires_server_change": False,
        }
    return {
        "action": "privatize_or_remove_from_sitemap",
        "requires_wp_plugin": False,
        "requires_server_change": False,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse Search Console coverage export archives.")
    parser.add_argument("source")
    parser.add_argument("--json-out")
    parser.add_argument("--csv-out")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    records = parse_coverage_export(args.source)
    payload = [asdict(record) for record in records]

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.csv_out:
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "issue_type",
                "status_bucket",
                "url",
                "recommended_action",
                "requires_wp_plugin",
                "requires_server_change",
            ],
        )
        writer.writeheader()
        writer.writerows(payload)
        Path(args.csv_out).write_text(output.getvalue(), encoding="utf-8")

    summary: dict[str, int] = {}
    for record in records:
        summary[record.status_bucket] = summary.get(record.status_bucket, 0) + 1
    print(json.dumps({"total": len(records), "summary": summary, "records": payload}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
