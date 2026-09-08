---
title: "Backtest data foundation adoption"
status: accepted
date: 2026-09-05
---

# 백테스트 데이터 기반

종목 이력·원본 출처·데이터 버전·시점·feature·사용 자격은 독립 AAS의 필수 기반이다.
가격 테이블과 실행 JSON만으로 축소한 병렬 데이터 모델은 만들지 않는다.

2026-09-07: [0014](0014-local-embedded-databases.md)가 저장 기술 선택을 대체한다.
이 결정의 데이터 의미·시점·계보·사용 자격 요구는 계속 적용한다.

- 종목 식별·원장·카탈로그·메타데이터·실행 영수증은 SQLite에 둔다.
- 비공개 전략은 별도 SQLite, 대량 관측·수정 이력·typed 실행 결과는 DuckDB에 둔다.
- 원본은 출처와 해시가 있는 파일로 보존한다. Parquet를 기본 저장 형식으로 요구하지 않는다.
- 세 모듈은 버전·시점이 고정된 공통 reader와 소유자별 writer를 사용한다.
- 입력 snapshot, knowledge cutoff, identity/universe, 가격 basis, 시장 관례,
  전략·환경 버전과 실행 영수증을 명시한다. 누락을 추정값으로 채우지 않는다.

상세 엔티티·키·제약·남은 공백은
[백테스트 데이터 설계](../design/backtest-data-foundation.md)가 소유한다.
전체 미국 주식 데이터 기반의 준비 여부를 확인하며 특정 전략의 자산 목록으로 구축
범위를 축소하지 않는다. 실제 전략의 실행에는 해당 입력의 품질·시점·권한 검증이 필요하다.

현재 DB 설치·검증된 기존 데이터 채택·카탈로그와 가격 조회는 CLI에 연결돼 있다.
전체 실행 bundle, 과거 revision 재생과 전략 실행 연결의 완성을 뜻하지 않는다.
기존 migration은 새 DB 경로 전환 전까지 현재 구현으로 유지한다. 데이터 의미를 보존하면서
[0014](0014-local-embedded-databases.md)의 전환 검증 후 옛 저장 adapter·SQL·의존성을 제거한다.
구버전 실행 도구는 [0012](0012-retire-legacy-runtime.md)의 폐기 범위를 따른다.
새 migration, 실제 데이터 복원·적재·수집 재개는 각각 범위를 정하고 검증한다.
