"""Refresh pipeline for published articles.

Determines whether a published article should be regenerated/updated based on
data freshness and quality signals.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional


@dataclass
class RefreshIssue:
    criterion: str
    severity: str
    reason: str
    suggestion: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "criterion": self.criterion,
            "severity": self.severity,
            "reason": self.reason,
            "suggestion": self.suggestion,
        }


@dataclass
class RefreshDecision:
    needs_refresh: bool
    score: int
    issues: List[RefreshIssue]

    def to_payload(self) -> Dict[str, Any]:
        return {
            "needs_refresh": self.needs_refresh,
            "score": self.score,
            "issues": [issue.to_dict() for issue in self.issues],
        }


class RefreshPipeline:
    """Evaluate refresh necessity for published post contents."""

    SEASONAL_KEYWORDS = {
        "spring": ["봄", "벚꽃", "개화", "사쿠라", "춘천", "한강나들이"],
        "summer": ["여름", "휴가", "바다", "워터파크", "피서", "폭염", "장마"],
        "autumn": ["단풍", "가을", "추석", "단풍시즌", "감성", "단풍축제"],
        "winter": ["겨울", "눈", "스키", "온천", "크리스마스", "연말", "연휴"],
    }

    DEFAULT_SOURCE_REQUIRED_FIELDS = {"place_id", "name", "rating", "review_count", "business_status"}

    def __init__(
        self,
        stale_days: int = 30,
        rating_delta_abs: float = 0.35,
        rating_delta_ratio: float = 0.12,
        review_count_abs_delta: int = 500,
        review_count_ratio: float = 0.25,
        seasonal_refresh_extra_days: int = 14,
    ):
        self.stale_days = stale_days
        self.rating_delta_abs = rating_delta_abs
        self.rating_delta_ratio = rating_delta_ratio
        self.review_count_abs_delta = review_count_abs_delta
        self.review_count_ratio = review_count_ratio
        self.seasonal_refresh_extra_days = seasonal_refresh_extra_days

    def evaluate(
        self,
        article: Mapping[str, Any],
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        issues: List[RefreshIssue] = []

        checked = self._check_last_verified(article, now)
        if checked is not None:
            issues.append(checked)

        checked = self._check_business_status(article)
        if checked is not None:
            issues.append(checked)

        issues.extend(self._check_data_drift(article))
        issues.extend(self._check_source_data_missing(article))
        issues.extend(self._check_seasonality(article, now))

        score = self._score(issues)
        return RefreshDecision(needs_refresh=score < 100, score=score, issues=issues).to_payload()

    def _check_last_verified(self, article: Mapping[str, Any], now: datetime) -> Optional[RefreshIssue]:
        raw = article.get("last_data_verified_at")
        if not raw:
            return RefreshIssue(
                criterion="last_data_verified_at",
                severity="high",
                reason="last_data_verified_at 값이 없습니다.",
                suggestion="원본 데이터 검증 시각을 즉시 기록하고 갱신 기준을 적용하세요.",
            )

        verified_at = self._parse_datetime(raw)
        if verified_at is None:
            return RefreshIssue(
                criterion="last_data_verified_at",
                severity="high",
                reason=f"last_data_verified_at 파싱 실패: {raw}",
                suggestion="ISO8601 형식(YYYY-MM-DDTHH:MM:SS)으로 보정 후 평가하세요.",
            )

        if verified_at.tzinfo is None:
            verified_at = verified_at.replace(tzinfo=timezone.utc)

        age_days = (now - verified_at).days
        if age_days > self.stale_days:
            return RefreshIssue(
                criterion="last_data_verified_at",
                severity="high",
                reason=f"마지막 검증일로부터 {age_days}일 경과 (임계값 {self.stale_days}일 초과).",
                suggestion="최근 장소 영업/리뷰/가격/혼잡도 데이터를 재조회하고 초안을 갱신하세요.",
            )
        return None

    def _check_business_status(self, article: Mapping[str, Any]) -> Optional[RefreshIssue]:
        status = article.get("business_status")
        if not status:
            return RefreshIssue(
                criterion="business_status",
                severity="critical",
                reason="business_status가 비어있거나 unknown 취급됩니다.",
                suggestion="장소 영업 상태를 API로 재조회해 확정 상태로 업데이트하세요.",
            )

        if str(status).strip().lower() == "unknown":
            return RefreshIssue(
                criterion="business_status",
                severity="critical",
                reason="business_status가 unknown입니다.",
                suggestion="장소 영업 상태를 최신 값으로 덮어쓰기하세요.",
            )
        return None

    def _check_data_drift(self, article: Mapping[str, Any]) -> List[RefreshIssue]:
        issues: List[RefreshIssue] = []
        previous_places = self._index_places(article.get("place_snapshots", []))
        current_places = self._index_places(article.get("places", []))

        if not previous_places and not current_places:
            return issues

        for place_id in current_places.keys() & previous_places.keys():
            prev = previous_places[place_id]
            curr = current_places[place_id]

            prev_rating = self._to_float(prev.get("rating"))
            curr_rating = self._to_float(curr.get("rating"))
            if prev_rating is not None and curr_rating is not None:
                delta = abs(curr_rating - prev_rating)
                ratio = delta / max(0.01, prev_rating) if prev_rating > 0 else 1.0
                if delta >= self.rating_delta_abs or ratio >= self.rating_delta_ratio:
                    issues.append(
                        RefreshIssue(
                            criterion="rating/review_count",
                            severity="medium",
                            reason=f"place_id={place_id}: rating 급변 감지(이전 {prev_rating} -> 현재 {curr_rating})",
                            suggestion="평점 변화 원인을 검토하고 근거 기반으로 문구를 조정하세요.",
                        )
                    )

            prev_count = self._to_int(prev.get("review_count"))
            curr_count = self._to_int(curr.get("review_count"))
            if prev_count is not None and curr_count is not None:
                delta = abs(curr_count - prev_count)
                ratio = delta / max(1, prev_count)
                if delta >= self.review_count_abs_delta or ratio >= self.review_count_ratio:
                    issues.append(
                        RefreshIssue(
                            criterion="rating/review_count",
                            severity="medium",
                            reason=f"place_id={place_id}: 리뷰 수 급변 감지(이전 {prev_count} -> 현재 {curr_count})",
                            suggestion="리뷰 수 변화 반영 시 추천 포인트(인기/이슈)를 갱신하세요.",
                        )
                    )

        if not issues:
            return issues
        if len(issues) >= 2:
            return issues
        # single high drift still meaningful for refresh
        issues[0].severity = "high"
        return issues

    def _check_source_data_missing(self, article: Mapping[str, Any]) -> List[RefreshIssue]:
        issues: List[RefreshIssue] = []
        required = self.DEFAULT_SOURCE_REQUIRED_FIELDS

        places = article.get("places")
        if places is None:
            return [
                RefreshIssue(
                    criterion="source_data_missing",
                    severity="critical",
                    reason="places(원본 장소 데이터) 자체가 없습니다.",
                    suggestion="모든 추천 장소의 원본 피드를 재수집하고 누락 필드를 보완하세요.",
                )
            ]

        if not isinstance(places, list):
            return [
                RefreshIssue(
                    criterion="source_data_missing",
                    severity="critical",
                    reason="places 타입이 list가 아닙니다.",
                    suggestion="places는 배열 형태의 장소 객체여야 합니다.",
                )
            ]

        for idx, place in enumerate(places, start=1):
            if not isinstance(place, Mapping):
                issues.append(
                    RefreshIssue(
                        criterion="source_data_missing",
                        severity="high",
                        reason=f"places[{idx}]가 객체 형식이 아님.",
                        suggestion="places 항목을 place_id/name/rating/review_count/business_status 포맷으로 정규화하세요.",
                    )
                )
                continue

            missing = [field for field in required if not place.get(field)]
            if missing:
                issues.append(
                    RefreshIssue(
                        criterion="source_data_missing",
                        severity="high",
                        reason=f"places[{idx}]({place.get('place_id', '?')})에 필수 필드 누락: {', '.join(missing)}",
                        suggestion="누락 필드를 API 재조회로 채우고 publish 전에 다시 반영하세요.",
                    )
                )

        return issues

    def _check_seasonality(self, article: Mapping[str, Any], now: datetime) -> List[RefreshIssue]:
        issues: List[RefreshIssue] = []
        raw_topics = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary", "")),
                str(article.get("region", "")),
                " ".join(map(str, article.get("tags", []))) if isinstance(article.get("tags"), list) else "",
            ]
        )

        season = self._season_from_text(raw_topics)
        if season is None:
            return issues

        if not article.get("last_data_verified_at"):
            return [
                RefreshIssue(
                    criterion="seasonality_topic",
                    severity="medium",
                    reason="시즌성 주제가 포함되었으나 검증 시각이 없습니다.",
                    suggestion="시즌성 주제 글은 기간 기반 재검증 정책을 적용하세요.",
                )
            ]

        verified = self._parse_datetime(article["last_data_verified_at"])
        if verified is None:
            return []

        age_days = (now - verified).days
        if age_days > (self.stale_days + self.seasonal_refresh_extra_days):
            issues.append(
                RefreshIssue(
                    criterion="seasonality_topic",
                    severity="high",
                    reason=f"시즌성 주제('{season}') 글이 {age_days}일 경과되어 추천 포인트가 시즌 변동을 반영하지 못할 수 있습니다.",
                    suggestion="해당 시즌 일정(행사/성수기/휴장일) 기준 최신 데이터로 갱신하세요.",
                )
            )
            return issues

        current_season = self._season_by_month(now.month)
        if season != current_season and season != "all":
            issues.append(
                RefreshIssue(
                    criterion="seasonality_topic",
                    severity="low",
                    reason=f"글 주제 시즌('{season}')과 현재 시즌('{current_season}')이 다릅니다.",
                    suggestion="시즌 종료 후 리드문구를 과거형 또는 다음 시즌 정보로 조정하세요.",
                )
            )

        return issues

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str):
            return None
        raw = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    def _index_places(self, items: Any) -> Dict[str, Dict[str, Any]]:
        indexed: Dict[str, Dict[str, Any]] = {}
        if not isinstance(items, list):
            return indexed
        for item in items:
            if not isinstance(item, Mapping):
                continue
            pid = item.get("place_id") or item.get("id")
            if pid is None:
                continue
            indexed[str(pid)] = dict(item)
        return indexed

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _season_from_text(self, text: str) -> Optional[str]:
        for season, keywords in self.SEASONAL_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    return season
        return None

    @staticmethod
    def _season_by_month(month: int) -> str:
        if month in (3, 4, 5):
            return "spring"
        if month in (6, 7, 8):
            return "summer"
        if month in (9, 10, 11):
            return "autumn"
        return "winter"

    def _score(self, issues: List[RefreshIssue]) -> int:
        score = 100
        penalty_by_severity = {"low": 3, "medium": 12, "high": 22, "critical": 35}
        for issue in issues:
            score -= penalty_by_severity.get(issue.severity, 5)
        return max(0, score)


def should_refresh(article: Mapping[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    """Convenience helper returning JSON-compatible payload."""
    return RefreshPipeline().evaluate(article, now=now)
