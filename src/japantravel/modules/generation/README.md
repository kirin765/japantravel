# Generation Pipeline

입력: 선정된 장소 후보 리스트
출력: 아래 구조를 갖는 한국어 초안
- title
- summary
- intro
- place_sections
- route_suggestion
- checklist
- faq
- conclusion

`GenerationPipeline.generate_article` 를 통해 생성하고,
`formatter.format_markdown` 으로 마크다운 렌더용 문자열 변환이 가능합니다.
