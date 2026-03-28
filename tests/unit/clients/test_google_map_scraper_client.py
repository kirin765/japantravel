from __future__ import annotations

from pathlib import Path
import stat

import pytest

from japantravel.clients.google_map_scraper_client import GoogleMapScraperClient
from japantravel.shared.exceptions import ExternalServiceError


def _write_fake_scraper(path: Path, json_payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import pathlib, sys",
                "args = sys.argv[1:]",
                "result_path = pathlib.Path(args[args.index('-results') + 1])",
                f"result_path.write_text({json_payload!r}, encoding='utf-8')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def test_scrape_places_normalizes_json_output(tmp_path: Path):
    scraper_path = tmp_path / "bin" / "google-map-scraper"
    _write_fake_scraper(
        scraper_path,
        '[{"title":"Tokyo Tower","cid":"cid-1","address":"Tokyo","latitude":35.6586,"longitude":139.7454,"review_rating":4.6,"review_count":1200,"thumbnail":"https://example.com/a.jpg","link":"https://maps.example.com/place/1","category":"attraction"}]',
    )

    client = GoogleMapScraperClient(
        scraper_path=str(scraper_path),
        todo_dir=str(tmp_path / "google-map-scraper"),
    )

    result = client.scrape_places(
        location_query="Tokyo,Japan",
        search_strings=["attraction"],
        max_results_per_search=5,
        language="ko",
    )

    assert result["meta"]["result_count"] == 1
    assert result["meta"]["queries"] == ["attraction Tokyo,Japan"]
    assert result["items"][0]["id"] == "cid-1"
    assert result["items"][0]["name"] == "Tokyo Tower"
    assert result["items"][0]["rating"] == 4.6
    assert result["items"][0]["review_count"] == 1200
    assert result["items"][0]["image_urls"] == ["https://example.com/a.jpg"]


def test_scrape_places_supports_project_directory_output(tmp_path: Path):
    project_dir = tmp_path / "google-map-scraper"
    entrypoint = project_dir / "dist" / "src" / "index.js"
    entrypoint.parent.mkdir(parents=True, exist_ok=True)
    entrypoint.write_text(
        "\n".join(
            [
                "import fs from 'node:fs';",
                "import path from 'node:path';",
                "const args = process.argv.slice(2);",
                "const outputDir = args[args.indexOf('--output') + 1];",
                "fs.mkdirSync(outputDir, { recursive: true });",
                "fs.writeFileSync(path.join(outputDir, 'run.json'), JSON.stringify({",
                "  searchResults: [{",
                "    id: 'sr-1',",
                "    title: 'Project Place',",
                "    placeUrl: 'https://maps.example.com/place-1!3d35.1!4d139.1'",
                "  }],",
                "  places: [{",
                "    id: 'place-1',",
                "    searchResultId: 'sr-1',",
                "    name: 'Google 지도',",
                "    address: 'Tokyo',",
                "    sourceUrl: 'https://maps.example.com/place-1!3d35.1!4d139.1'",
                "  }],",
                "  reviews: [{ placeId: 'place-1' }, { placeId: 'place-1' }],",
                "  photos: [{ placeId: 'place-1', imageUrl: 'https://example.com/p1.jpg' }]",
                "}));",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    client = GoogleMapScraperClient(
        scraper_path=str(project_dir),
        todo_dir=str(project_dir),
    )

    result = client.scrape_places(
        location_query="Tokyo,Japan",
        search_strings=["cafe"],
        max_results_per_search=3,
        language="ko",
    )

    assert result["meta"]["result_count"] == 1
    assert result["items"][0]["name"] == "Project Place"
    assert result["items"][0]["latitude"] == 35.1
    assert result["items"][0]["longitude"] == 139.1
    assert result["items"][0]["googleMapsUrl"] == "https://maps.example.com/place-1!3d35.1!4d139.1"
    assert result["items"][0]["review_count"] == 2
    assert result["items"][0]["image_urls"] == ["https://example.com/p1.jpg"]


def test_scrape_places_creates_todo_for_insufficient_output(tmp_path: Path):
    scraper_path = tmp_path / "bin" / "google-map-scraper"
    todo_dir = tmp_path / "google-map-scraper"
    _write_fake_scraper(scraper_path, '[{"title":"Incomplete Place"}]')

    client = GoogleMapScraperClient(
        scraper_path=str(scraper_path),
        todo_dir=str(todo_dir),
    )

    with pytest.raises(ExternalServiceError):
        client.scrape_places(
            location_query="Tokyo,Japan",
            search_strings=["restaurant"],
            max_results_per_search=5,
            language="ko",
        )

    todo_path = todo_dir / "TODO.md"
    assert todo_path.exists()
    todo_text = todo_path.read_text(encoding="utf-8")
    assert "identity/location" in todo_text
    assert "rating/review_count" in todo_text
