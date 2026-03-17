# PostgreSQL DDL 스키마

자동화 파이프라인에서 사용할 DB 스키마 초안입니다.

- 대상 엔터티: Place, ArticleCandidate, PublishedArticle, publish_logs, error_logs
- 설계 포인트:
  - 장소 후보(`place`)와 생성 후보(`article_candidate`) 분리
  - 후보-발행본 매핑(`published_article.article_candidate_id`)
  - 발행 이력(`publish_logs`)과 공통 오류 로그(`error_logs`) 분리 수집

## 적용 순서
1. `travel_blog_schema.sql`을 PostgreSQL에서 실행
2. 필요 시 `country/region`/`category` 조건에 맞는 추가 인덱스 추가
3. 운영 환경에서는 마이그레이션 도구로 버전 관리
