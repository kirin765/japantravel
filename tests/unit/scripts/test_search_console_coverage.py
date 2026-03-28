import io
import zipfile

from japantravel.scripts.parse_search_console_coverage import parse_coverage_export


def test_parse_coverage_export_reads_nested_search_console_archives(tmp_path):
    nested_buffer = io.BytesIO()
    with zipfile.ZipFile(nested_buffer, "w") as nested:
        nested.writestr("메타데이터.csv", "대상,예시\n문제,발견됨 - 현재 색인이 생성되지 않음\n")
        nested.writestr(
            "테이블.csv",
            "URL\nhttps://www.japantravel.co.kr/category/japan/\nhttps://www.japantravel.co.kr/안녕하세요/\n",
        )

    archive_path = tmp_path / "Archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested.zip", nested_buffer.getvalue())

    records = parse_coverage_export(archive_path)

    assert len(records) == 2
    assert records[0].status_bucket == "discovered_not_indexed"
    assert records[0].recommended_action == "noindex_archive_and_remove_from_sitemap"
    assert records[1].recommended_action == "privatize_placeholder_content"
