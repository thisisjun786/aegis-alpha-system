---
title: "Local SQLite and DuckDB storage"
status: accepted
date: 2026-09-07
---

# 로컬 내장 DB로 재설계

2026-09-08: [0015](0015-research-engine-product-boundary.md)의 엔진 목표에서도 이 저장 계약을
유지한다. 아래의 상시 앱 명령 통로는 승인됐지만 미구현인 설계다. 현재 CLI는 명령별로
설치 잠금을 잡고 DB를 열며, 소켓으로 요청하지 않는다.

오너는 별도 데이터 배포·공유 요구가 없으며, SQLite와 DuckDB를 바탕으로 DB를 완전히
재설계하도록 지시했다. 신규 설치는 한 사용자·한 컴퓨터의 로컬 저장소를 기준으로 한다.
DB 서버 관리, PostgreSQL과 Parquet를 기본 설치에서 제외한다.

이 결정은 내장 저장소의 기준이다. 현재 `init`·`doctor`·`db`·`strategy`·`data` 명령은
새 `storage/` 구현을 사용한다. 기존 실제 DB를 이전한 기록은 아니다. 물리 테이블·조회·복구 계약은 [데이터 설계](../design/backtest-data-foundation.md),
설치 흐름은 [0013](0013-first-install-workspace.md)이 소유한다.

## 저장소

| 파일 | 정본 |
| --- | --- |
| `state.sqlite3` | 종목·출처·카탈로그·권한·작업 상태·입력 pin·실행 영수증 |
| `strategies.sqlite3` | 별도 비공개 전략 원문·버전·규칙·계보·원래 성과와 비교 조건 |
| `market.duckdb` | 가격·재무·거시·corporate action·파생 feature·대량 실행 결과 |

기본 루트는 `~/.aas`이며 설정으로 위치를 지정한다. 원본 응답 bytes는 `raw/`에 해시로
보존한다. SQLite와 DuckDB는 AAS에 내장하고 사용자가 별도로 설치·설정하지 않는다.
Parquet는 필요 시 입출력 adapter가 지원할 수 있는 형식이다. 기본 저장, 백업, 데이터
publication 또는 첫 설치의 선행 조건으로 두지 않고 미리 export 체계를 만들지 않는다.

SQLite는 작은 상태 갱신과 참조 무결성, DuckDB는 대량 저장·분석을 맡는다.
파일별 책임을 고정하고 같은 데이터를 두 저장소의 정본으로 유지하지 않는다.
DB 세 개 사이에는 공통 transaction이나 FK가 없으므로 durable intent와 완료 영수증으로
부분 저장을 복구한다. 그 비용은 전략 DB의 독립 소유와 분석 저장소 분리를 위해 수용한다.

## 실행과 보존

- 한 AAS 프로세스가 `market.duckdb`의 읽기·쓰기를 소유한다. 프로세스 내부의 조회와
  계산은 병렬로 실행할 수 있다. 다른 프로세스는 같은 파일을 직접 열어 쓰지 않는다.
- CLI의 일회 실행도 같은 설치 잠금을 따른다. 상시 앱이 있으면 로컬 명령 통로로 요청한다.
  DB를 소유하는 앱은 사용자 요청으로 시작하며 설치만으로 자동 작업을 켜지 않는다.
- 시장 정본은 버전별 append-only 관측·수정·철회 기록이다. Parquet 파일의 불변성 대신
  generation ID·논리 내용 해시·수정 이력·고정 조회 조건으로 재현성을 보존한다.
- 전략 정의는 새 버전으로 등록한다. 실행 기록에는 ID·버전·해시와 필요한 실행 입력을
  고정한다. 공개 레포·패키지·이미지·CI에 실제 전략 DB나 데이터가 들어가지 않는다.
- 파일 권한과 명시적 읽기·쓰기 API를 사용한다. SQLite 파일 분리가 PostgreSQL 역할 기반
  권한이나 OS 소유자의 변조 방지를 제공한다고 주장하지 않는다.
- 백업은 세 DB와 원본·결과 파일이 함께 일관된 시점에 맞도록 수행한다. 실행 중인 DB
  파일을 단순 복사하지 않는다. 복원은 새 루트에 완료한 뒤 전환한다.

## 기존 결정과 코드의 처리

[0010](0010-backtest-data-foundation.md)의 종목 이력·원본·시점·불변 버전·자격·실행 계보
요구는 유지한다. 그 결정의 PostgreSQL·Parquet 지정과 DB 역할 기반 실행 경계는 이 결정이
대체한다. [0012](0012-retire-legacy-runtime.md)의 구버전 SQLite 제거는 당시 폐기한 구현을
가리킨다. 새 SQLite 설계가 그 코드를 복원하거나 동일한 schema를 사용한다는 뜻은 아니다.

PostgreSQL adapter·Alembic chain·Parquet reader는 전환 기간의 `legacy` 추가 구현이다. 새 경로의
동작과 실패 조건을 합성 입력으로 검증한 뒤 호출자·CLI·CI를 함께 전환하고, 사용하지 않는
옛 adapter·dependency·migration·테스트를 공개 후보에서 제거한다. 옛 SQL을 SQLite 문법으로
자동 번역하지 않는다. 실제 보관 데이터의 채택·복원은 별도로 범위를 정한다.

여러 DB backend를 동시에 영구 지원하는 범용 추상화, 외부 DB 옵션, 분산 작업 큐,
DuckLake나 DB 서버를 이번 설계에 추가하지 않는다. 네트워크 파일시스템을 여러 컴퓨터가
공유하는 사용도 지원 범위에 두지 않는다.

## 근거와 검증 범위

DuckDB는 자체 파일에 영구 저장하며, 내장 모드는 한 프로세스의 읽기·쓰기와 그 안의
동시성을 지원한다. SQLite는 로컬 앱의 상태 저장에 적합하다. 이는 선택의 기술적 근거이며
AAS의 처리량·재시작·복원 성공 증거는 아니다.
[DuckDB 저장](https://duckdb.org/docs/current/connect/overview#persistence),
[DuckDB 동시성](https://duckdb.org/docs/current/connect/concurrency),
[SQLite 용도](https://www.sqlite.org/whentouse.html).

설계 완료 기준은 도메인별 정본·키·시점·변경 규칙·복구 순서와 전환 검사가 정의되는 것이다.
구현 완료는 빈 설치, 계약별 거부 사례, 중간 장애·재시작, 백업·새 루트 복원,
실제 설치 artifact에서의 합성 실행까지 통과해야 별도로 선언한다.
