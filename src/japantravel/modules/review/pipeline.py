"""Review pipeline for generated travel article drafts.

The pipeline combines:
1) Rule-based checks (deterministic)
2) Optional LLM-based checks (quality/governance sanity)

Output format:
{
  "pass": bool,
  "score": int,
  "issues": [ ... ],
  "required_actions": [ ... ],
}
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ...clients.openai_client import OpenAIClient
from ...shared.exceptions import ExternalServiceError


@dataclass
class ReviewIssue:
    section: str
    severity: str
    message: str
    action: str
    source: str = "rule"

    def to_dict(self) -> Dict[str, str]:
        return {
            "section": self.section,
            "severity": self.severity,
            "message": self.message,
            "action": self.action,
            "source": self.source,
        }


@dataclass
class ReviewResult:
    passed: bool
    score: int
    naturalness_score: int
    issues: List[ReviewIssue]
    required_actions: List[str]

    def to_payload(self) -> Dict[str, Any]:
        return {
            "pass": self.passed,
            "score": self.score,
            "naturalness_score": self.naturalness_score,
            "issues": [issue.to_dict() for issue in self.issues],
            "required_actions": self.required_actions,
        }


class ReviewPipeline:
    """Validate article candidates with hybrid rule + LLM review."""

    SYSTEM_PROMPT = (
        "너는 한국어 여행 콘텐츠 검수자이다. "
        "입력된 글을 검토해 다음 JSON 형식으로만 응답한다.\n"
        "{"
        "\"overall_pass\": bool, "
        "\"score_adjustment\": int, "
        "\"issues\": [{\"section\": str, \"severity\": \"critical|high|medium|low\", \"message\": str, \"action\": str}], "
        "\"required_actions\": [str]"
        "}"
    )

    LLM_USER_PROMPT = (
        "아래 기사 초안을 구조적으로 검수한다.\n"
        "- 한국어 톤 일관성, 과장/불명확 표현, 사실성에 대한 의심 문장, 문단 간 연결성을 점검한다.\n"
        "- 위 규칙 위반이 있으면 issue 항목으로 남겨라.\n"
        "- score_adjustment는 rule 점수(0~100) 대비 -30~+10 범위의 보정값\n"
        "입력 구조(JSON):\n{article_json}\n"
    )

    SCORE_BASE = 100
    SECTION_MIN_LEN = {
        "title": 6,
        "summary": 40,
        "intro": 60,
        "route_suggestion": 80,
        "conclusion": 40,
    }
    PENALTIES = {
        "critical": 35,
        "high": 20,
        "medium": 10,
        "low": 5,
    }

    def __init__(
        self,
        openai_client: Optional[OpenAIClient] = None,
        allow_place_issues: bool = True,
        max_score: int = 100,
    ) -> None:
        self.openai_client = openai_client
        self.allow_place_issues = allow_place_issues
        self.max_score = max_score
        self.logger = logging.getLogger(self.__class__.__name__)

    def review(self, article: Any) -> Dict[str, Any]:
        payload = self._to_payload(article)
        issues = self._rule_checks(payload)
        rule_score = self._calculate_score(issues)

        llm_score_adjustment = 0
        if self.openai_client is not None:
            llm_issues, llm_score_adjustment = self._llm_review(payload)
            issues.extend(llm_issues)

        score = max(0, min(self.max_score, rule_score + llm_score_adjustment))
        naturalness_score = self._naturalness_score(payload, issues)
        required_actions = self._build_required_actions(issues)
        passed = score >= 75 and not any(issue.severity == "critical" for issue in issues)

        result = ReviewResult(
            passed=passed,
            score=score,
            naturalness_score=naturalness_score,
            issues=issues,
            required_actions=required_actions,
        )
        return result.to_payload()

    def _to_payload(self, article: Any) -> Dict[str, Any]:
        if isinstance(article, Mapping):
            return dict(article)

        if hasattr(article, "to_payload") and callable(getattr(article, "to_payload")):
            return article.to_payload()

        if hasattr(article, "__dict__"):
            return dict(vars(article))

        raise ValueError("Article input must be mapping or have to_payload/__dict__.")

    def _rule_checks(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        issues.extend(self._check_presence(payload))
        issues.extend(self._check_lengths(payload))
        issues.extend(self._check_place_sections(payload))
        issues.extend(self._check_lists(payload))
        issues.extend(self._check_placeholder_tokens(payload))
        return issues

    def _check_presence(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        required = [
            "title",
            "summary",
            "intro",
            "place_sections",
            "route_suggestion",
            "checklist",
            "faq",
            "conclusion",
        ]
        for key in required:
            value = payload.get(key)
            if value is None or (isinstance(value, (str, list, tuple, dict)) and len(value) == 0):
                issues.append(
                    ReviewIssue(
                        section=key,
                        severity="critical",
                        message=f"{key} is missing or empty.",
                        action="필수 섹션을 생성해 채워주세요.",
                    )
                )
        return issues

    def _check_lengths(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        for section, min_len in self.SECTION_MIN_LEN.items():
            text = self._to_text(payload.get(section))
            if len(text) < min_len:
                issues.append(
                    ReviewIssue(
                        section=section,
                        severity="high",
                        message=f"{section} is too short ({len(text)} < {min_len}).",
                        action="해당 섹션을 더 구체적이고 풍부하게 작성하세요.",
                    )
                )
        return issues

    def _check_place_sections(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        if not self.allow_place_issues:
            return issues

        raw_sections = payload.get("place_sections", [])
        if not isinstance(raw_sections, list):
            issues.append(
                ReviewIssue(
                    section="place_sections",
                    severity="critical",
                    message="place_sections has invalid type.",
                    action="장소별 섹션을 배열 형태로 구성하세요.",
                )
            )
            return issues

        if len(raw_sections) < 2:
            issues.append(
                ReviewIssue(
                    section="place_sections",
                    severity="medium",
                    message="place_sections should contain at least 2 places.",
                    action="2개 이상 장소 섹션을 구성해 추천 근거를 확장하세요.",
                )
            )

        if len(raw_sections) > 12:
            issues.append(
                ReviewIssue(
                    section="place_sections",
                    severity="low",
                    message="place_sections may be too many and harm readability.",
                    action="핵심 장소만 6~9곳으로 압축해 핵심성을 높이세요.",
                )
            )

        for idx, sec in enumerate(raw_sections, start=1):
            if not isinstance(sec, Mapping):
                issues.append(
                    ReviewIssue(
                        section=f"place_sections[{idx}]",
                        severity="high",
                        message="place section must be an object with title/body.",
                        action="장소 섹션을 title/body 구조로 맞추세요.",
                    )
                )
                continue
            title = self._to_text(sec.get("title"))
            body = self._to_text(sec.get("body"))
            if not title:
                issues.append(
                    ReviewIssue(
                        section=f"place_sections[{idx}]",
                        severity="medium",
                        message="place section title is missing.",
                        action="장소 추천 이유를 담은 제목을 추가하세요.",
                    )
                )
            if len(body) < 50:
                issues.append(
                    ReviewIssue(
                        section=f"place_sections[{idx}]",
                        severity="medium",
                        message="place section body is too short.",
                        action="방문 포인트, 동선 이유, 주의사항을 보강하세요.",
                    )
                )
        return issues

    def _check_lists(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        checklist = payload.get("checklist", [])
        faq = payload.get("faq", [])

        if not isinstance(checklist, list) or len(checklist) == 0:
            issues.append(
                ReviewIssue(
                    section="checklist",
                    severity="high",
                    message="checklist is missing or empty.",
                    action="체크리스트를 5~10개로 구성하세요.",
                )
            )
        elif not all(isinstance(item, str) and item.strip() for item in checklist):
            issues.append(
                ReviewIssue(
                    section="checklist",
                    severity="medium",
                    message="checklist contains invalid items.",
                    action="각 항목을 문자열로 정리하세요.",
                )
            )

        if not isinstance(faq, list) or len(faq) == 0:
            issues.append(
                ReviewIssue(
                    section="faq",
                    severity="high",
                    message="faq is missing or empty.",
                    action="Q/A 형식의 FAQ를 3개 이상 생성하세요.",
                )
            )
        else:
            for idx, item in enumerate(faq, start=1):
                question = None
                answer = None
                if isinstance(item, Mapping):
                    question = self._to_text(item.get("question") or item.get("q"))
                    answer = self._to_text(item.get("answer") or item.get("a"))
                elif isinstance(item, str):
                    text = self._to_text(item)
                    if re.search(r"^Q\\s*:\\s*", text):
                        split = re.split(r"A\\s*:\\s*", text, maxsplit=1)
                        if len(split) > 1:
                            question = split[0]
                            answer = split[1]
                    else:
                        question = text
                        answer = ""
                else:
                    question = ""

                if not question or not answer:
                    issues.append(
                        ReviewIssue(
                            section=f"faq[{idx}]",
                            severity="low",
                            message="faq item should include explicit Q and A.",
                            action="Q와 A를 명확히 작성하거나 question/answer 필드로 분리하세요.",
                        )
                    )
        return issues

    def _check_placeholder_tokens(self, payload: Dict[str, Any]) -> List[ReviewIssue]:
        issues: List[ReviewIssue] = []
        banned_patterns = [
            r"\{region\}",
            r"\{scenario\}",
            r"\{place_count\}",
            r"TODO",
            r"FIXME",
            r"{{",
            r"}}",
        ]
        combined_text = self._collect_text(payload)
        for pattern in banned_patterns:
            if re.search(pattern, combined_text, flags=re.IGNORECASE):
                issues.append(
                    ReviewIssue(
                        section="content",
                        severity="high",
                        message=f"Template token or placeholder found: {pattern}",
                        action="검수 전 템플릿 토큰을 제거하고 실제 콘텐츠로 치환하세요.",
                    )
                )
        return issues

    def _llm_review(self, payload: Dict[str, Any]) -> tuple[list[ReviewIssue], int]:
        if self.openai_client is None:
            return [], 0

        user_prompt = self.LLM_USER_PROMPT.format(article_json=json.dumps(payload, ensure_ascii=False, indent=2))
        try:
            response = self.openai_client.generate(
                system_prompt=self.SYSTEM_PROMPT,
                user_prompt=user_prompt,
                context={"mode": "review"},
            )
            parsed = self._extract_json(response)
            llm_issues: List[ReviewIssue] = []
            adjustment = int(self._as_int(parsed.get("score_adjustment", 0), 0))
            adjustment = max(-30, min(10, adjustment))

            for item in parsed.get("issues", []):
                if not isinstance(item, Mapping):
                    continue
                severity = str(item.get("severity", "low")).lower()
                if severity not in ("critical", "high", "medium", "low"):
                    severity = "low"
                llm_issues.append(
                    ReviewIssue(
                        section=str(item.get("section", "llm")),
                        severity=severity,
                        message=str(item.get("message", "")),
                        action=str(item.get("action", "")),
                        source="llm",
                    )
                )

            required = [str(x) for x in parsed.get("required_actions", []) if x]
            llm_issues.extend(
                ReviewIssue(
                    section="llm",
                    severity="low",
                    message="llm reported overall pass=false",
                    action="llm 결과에서 제시한 조치를 우선 반영하세요.",
                ) for _ in ([] if parsed.get("overall_pass", True) else [0])
            )

            llm_adjusted_actions = required
            return llm_issues, adjustment

        except ExternalServiceError as exc:
            self.logger.warning("LLM review failed: %s", exc)
            issue = ReviewIssue(
                section="llm",
                severity="low",
                message="LLM 검수 호출 실패. 규칙 기반 결과만 반환합니다.",
                action="LLM 키/네트워크 상태 확인 후 재검수하세요.",
                source="llm",
            )
            return [issue], 0
        except (json.JSONDecodeError, ValueError):
            issue = ReviewIssue(
                section="llm",
                severity="low",
                message="LLM 응답을 JSON으로 파싱하지 못했습니다.",
                action="LLM 응답 포맷을 JSON-only로 고정하고 재실행하세요.",
                source="llm",
            )
            return [issue], -5

    def _extract_json(self, raw: str) -> Dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            raise ValueError("empty llm response")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise
            return json.loads(text[start : end + 1])

    @staticmethod
    def _build_required_actions(issues: Iterable[ReviewIssue]) -> List[str]:
        actions = []
        for issue in issues:
            if issue.action and issue.action not in actions:
                actions.append(issue.action)
        return actions

    @staticmethod
    def _to_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        try:
            if isinstance(value, bool):
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _collect_text(self, payload: Mapping[str, Any]) -> str:
        chunks: List[str] = []
        for value in payload.values():
            if isinstance(value, str):
                chunks.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        chunks.append(item)
                    elif isinstance(item, Mapping):
                        chunks.append(self._to_text(item.get("title")))
                        chunks.append(self._to_text(item.get("body")))
                        chunks.append(self._to_text(item.get("question")))
                        chunks.append(self._to_text(item.get("answer")))
        return "\n".join([chunk for chunk in chunks if chunk])

    def _calculate_score(self, issues: Sequence[ReviewIssue]) -> int:
        score = self.SCORE_BASE
        for issue in issues:
            score -= self.PENALTIES.get(issue.severity, self.PENALTIES["low"])
        return max(0, min(self.max_score, score))

    @staticmethod
    def _naturalness_score(payload: Mapping[str, Any], issues: Sequence[ReviewIssue]) -> int:
        score = 100
        text = (payload.get("summary", "") + " " + payload.get("intro", "") + " " + payload.get("conclusion", "")).strip()
        if not text:
            return 0

        overclaim_words = ["최고", "최강", "꼭", "완벽", "반드시", "무조건", "단 하나", "천국", "필수", "절대"]
        for word in overclaim_words:
            if word in text:
                score -= 6

        soft_connectors = ["그러나", "또한", "반면", "다만", "그래도", "이때", "예상치 못한"]
        for word in soft_connectors:
            if word in text:
                score += 1

        if len(text) < 350:
            score -= 10

        faq = payload.get("faq", [])
        if isinstance(faq, list):
            if len(faq) < 3:
                score -= 12
            if len(faq) > 10:
                score -= 8

        if any(issue.severity in {"high", "critical"} for issue in issues):
            score -= 10

        for issue in issues:
            if issue.source == "llm" and issue.severity == "low":
                score -= 1

        return max(0, min(100, score))


def review_article(article: Any, openai_client: Optional[OpenAIClient] = None) -> Dict[str, Any]:
    """Convenience wrapper for quick usage."""
    return ReviewPipeline(openai_client=openai_client).review(article)
