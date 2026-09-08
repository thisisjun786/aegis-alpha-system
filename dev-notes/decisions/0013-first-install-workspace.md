---
title: "User workspace and embedded installation"
status: accepted
date: 2026-09-07
---

# 처음 설치하는 사용자의 저장소와 실행 환경

오너는 별도 PostgreSQL 서버와 데이터셋 배포 요구 없이 SQLite·DuckDB로 재설계하도록
지시했다. 이전의 Compose·PostgreSQL 볼륨 초안은 이 방향으로 교체한다. 저장 방향은
[0014](0014-local-embedded-databases.md)에 따른다. 네이티브 초기화·진단·전략 등록·백업·새 루트
복원은 `storage/`와 CLI에 연결돼 있다. 상시 앱의 소켓·예약 실행은 후속 구현이다.

파일별 테이블·시점·복구의 정본은 [데이터 설계](../design/backtest-data-foundation.md)다.
[현재 구조](../architecture.md), [실행 가능한 명령](../operations.md),
[공개·비공개 경계](0011-public-engine-private-strategies.md)는 해당 문서가 소유한다.

## 기본 설치

사용자는 AAS 앱 하나를 설치한다. SQLite·DuckDB는 앱에 포함하며 DB 서버·계정·별도 DB
볼륨을 만들지 않는다. 네이티브 앱과 선택적 단일 Docker 이미지가 같은 저장 계약을 사용한다.
실행기·이미지·설치 manifest 배포는 후속 구현이며 현재 release artifact의 존재를 주장하지 않는다.

최초 지원은 로컬 Linux 파일시스템이다. 원격 Docker daemon, 네트워크 공유 DB 파일,
Docker Desktop·rootless 환경은 실제 경로·권한·잠금 검증 전 지원으로 표시하지 않는다.
Aegis·Alpha·Hedge는 같은 앱의 모듈이다. DB 서버, Redis, 별도 worker 서비스를 추가하지 않는다.

## 저장 위치

```text
~/.aas/
  runtime.json         # 사용자가 정하는 경로·자원 한도·작업 설정
  installation.json    # 설치 정체성·버전·초기화 단계 영수증
  state.sqlite3        # 앱 상태·종목·출처·카탈로그·실행 영수증
  strategies.sqlite3   # 사용자 소유의 비공개 전략 원문·버전·계보
  market.duckdb        # 시장 이력·재무·거시·feature·대량 실행 결과
  raw/                 # 해시 경로의 원본 응답 bytes
  runs/                # 실행별 보고서·추가 산출물
  secrets/             # 사용자가 나중에 등록하는 공급자 키
  backups/             # 일관된 DB·원본·결과 백업 세트
  runtime/             # OS 잠금과 활성 프로세스의 로컬 소켓
```

루트 선택 순서는 `--home`, `AAS_HOME`, 기본 `~/.aas`다. 상대 경로 설정은 현재 작업
디렉터리가 아니라 선택한 루트에서 해석한다. `runtime.json`이 사용자 설정의 단일 원본이고,
installation.json은 실제 설치·재개를 검증하는 영수증이다. 동일 설정을 양쪽에서 편집하지 않는다.

큰 market DB·raw와 별도 전략 DB의 위치를 바꿀 수 있지만 모두 한 설치가 소유한다.
다른 설치와 같은 파일을 공유하지 않는다. 파일 정체성·store ID·설치 ID와 외부 경로의
잠금도 검증한다. 영구 루트는 Git checkout 밖에 둔다. 호스트의 절대 경로는 사용자 설정에만
있으며 전략 ID·데이터 내용 hash·공개 패키지에 섞이지 않는다.

## 실행 과정

한 프로세스가 market.duckdb의 읽기·쓰기를 소유한다. 다른 명령은 실행 중인 앱의 로컬
소켓으로 요청하고 같은 파일을 직접 열지 않는다. 앱이 없으면 일회 CLI가 설치 잠금을 얻어
같은 저장 계층을 열었다 닫는다. 기본 초기화가 상시 앱이나 수집을 자동으로 시작하지 않는다.
예약 실행이 필요할 때 같은 앱을 상시 실행하며, 별도 스케줄러 서비스를 요구하지 않는다.

로컬 소켓은 사용자 권한·설치 ID를 검증하고 구조화한 앱 명령만 받는다. 소켓 연결 실패가
새 DuckDB writer를 시작할 이유가 되지 않는다. 파일 잠금은 DB 연결 수명 전체에 유지하고
PID나 만료 시각만 보고 인계하지 않는다. 프로세스 내부의 여러 작업은 독립적으로 계산할 수
있으며 게시·상태 확정만 직렬화한다. [DuckDB 동시성](https://duckdb.org/docs/current/connect/concurrency).

Docker에서는 같은 앱 한 개를 실행하고 사용자 루트를 `/state/aas`에 연결한다. 별도 DB
서비스·named DB volume·호스트 공개 포트·Docker 소켓 마운트는 없다. 외부 경로를 선택했다면
실행기가 그 명시적 경로만 추가 연결한다. 컨테이너는 비-root로 실행하며 앱 이미지와 사용자
자료의 수명을 분리한다. 키와 DB 파일은 이미지 빌드에 포함하지 않는다.

Docker 모드의 호스트 실행기는 `docker exec`로 컨테이너 안의 CLI를 호출해 같은 로컬
소켓을 사용한다. Docker Desktop에서도 호스트가 컨테이너 Unix 소켓을 바로 열 수 있다고
가정하지 않는다. 컨테이너가 없거나 중지됐다면 상태를 알리고 명시적 시작을 요구한다.

## 처음 쓰는 사람의 흐름

현재 실행 가능한 명령의 인자는 [operations](../operations.md)를 따른다.

1. `aas init`: 디렉터리·파일 권한·세 빈 DB·schema·설정·설치 영수증을 준비한다.
   전략이나 공급자 키는 필요하지 않다. 기존 DB를 발견하면 소유자·버전을 확인한다.
2. `aas doctor`: 실제 루트·세 DB 위치·schema·잠금 상태를 표시한다. 설치 성공과
   전략 없음·시장 데이터 없음·권한 미설정·예약 실행 꺼짐을 구분한다.
3. 합성 예제로 엔진 입출력을 확인한다. 실제 전략·공급자 접속이나 주문을 실행하지 않는다.
4. `aas strategy import <파일> --id <ID> --version <버전> --sha256 <해시>`: 원문·ID·버전·hash·요구사항을 검증해 비공개 DB에 등록한다.
   원문은 DB 안에 보존하므로 가져온 파일이 사라져도 등록 내용은 남는다.
5. 사용자가 공급자를 설정하고 수집·실행을 요청한다. 설치나 재시작이 유료 호출이나
   데이터 복원·주문을 자동으로 시작하지 않는다.

`init` 재실행은 기존 자료를 덮어쓰지 않고 불완전한 초기화만 검증 후 재개한다.
동시 초기화는 하나만 허용한다. 다른 설치의 DB는 이름이 같아도 인수하지 않는다.
실제 전략이 비어 있는 상태를 오류로 숨기거나 내장 기본 전략으로 채우지 않는다.
키는 사용자 전용 파일로 관리하고 진단 출력에서 노출하지 않는다.

## 백업·업데이트·제거

백업은 같은 앱에 유지보수를 요청한다. 새 쓰기를 막고 수집·전략 등록·분석 연결과
미완료 저장을 정리한다. 전체 백업 동안 모든 writer와 설치 잠금을 통제한다.
SQLite는 backup API, DuckDB는 CHECKPOINT·모든 연결 종료 후 복사를 사용한다.
살아 있는 DB 파일을 일반 파일처럼 복사하거나 각 DB snapshot이 자동으로 일치한다고
가정하지 않는다. [SQLite 백업](https://www.sqlite.org/backup.html).

세 DB·참조 raw·runs 파일·비밀값 없는 설정·schema/engine 버전·크기·hash manifest를 묶는다.
별도 디스크를 지정한 파일도 포함한다. 공급자 키는 기본 제외해 복원 후 다시 등록한다.
DB가 포함된 백업은 비공개이며 같은 디스크의 사본이 디스크 장애를 보호한다고 안내하지 않는다.

복원은 새 루트에서 파일·논리 hash·FK·시점 조회·전략과 결과 참조를 확인한 뒤 전환한다.
업데이트는 잠금·백업 후 schema별 migration을 수행하고 일부만 성공하면 기동을 차단한다.
이미지 rollback이 DB schema rollback을 해결하지 않는다. 평소 중지·재시작·앱 제거는
사용자 자료를 유지하며, 데이터 삭제는 소유 자원과 범위를 확인하는 별도 동작이다.

## Vibe-Trading에서 참고한 점

2026-09-07 확인한 upstream
[a4f06a29f61f2dec3764da294db77478521de259](https://github.com/HKUDS/Vibe-Trading/commit/a4f06a29f61f2dec3764da294db77478521de259)의
소스를 참고했다. MIT 코드의 구조를 검토했으며 이번 작업에 코드를 복사하지 않았다.

VT는 `~/.vibe-trading` 아래 SQLite와 파일을 사용하고 `init`에서 설정을 만든다.
온보딩은 `.env.partial` 저장 후 원자적으로 교체한다. 기본 Docker 앱에는 백엔드와
빌드된 UI가 함께 있고, named volume 다섯 개와 레포의 `.env` bind mount로 자료를 보존한다.
[경로](https://github.com/HKUDS/Vibe-Trading/blob/a4f06a29f61f2dec3764da294db77478521de259/agent/src/config/paths.py#L13),
[초기화](https://github.com/HKUDS/Vibe-Trading/blob/a4f06a29f61f2dec3764da294db77478521de259/agent/cli/onboard.py#L121),
[전략 DB](https://github.com/HKUDS/Vibe-Trading/blob/a4f06a29f61f2dec3764da294db77478521de259/agent/src/strategy_store/sqlite_store.py#L130),
[Compose](https://github.com/HKUDS/Vibe-Trading/blob/a4f06a29f61f2dec3764da294db77478521de259/docker-compose.yml#L1).

AAS는 사용자 루트·초기화·데이터 보존을 참고하되 모든 기본 경로를 하나의 설정에서 결정한다.
VT의 기본 홈 직접 계산과 레포 안 `.env` 경로를 새 설치에 가져오지 않는다.
통합 백업과 복원은 별도로 검증하며 VT의 볼륨 보존을 AAS 복구 증거로 사용하지 않는다.

## 구현 전환

현재 PostgreSQL 설치·Parquet reader와 예전 설정 경로는 아직 코드에 남아 있다.
0014의 L1~L6 전환을 마친 뒤 옛 adapter·schema·의존성을 제거한다. 기존 사용자 자료는
자동 이동하지 않으며 실제 데이터 채택은 별도 범위에서 검증한다.
이 문서는 문서·계약 검토 결과다. 초기화·재시작·컨테이너·백업·복원 성공은 구현 후 입증한다.
