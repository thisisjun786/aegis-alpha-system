# AAS architecture

AAS의 목표는 외부 앱과 에이전트가 사용하는 데이터·연구 엔진이다. AAS가 데이터 수집·저장·
조회와 계산을 맡고 외부 도구가 화면과 작업 진행을 맡는다. [0015](decisions/0015-research-engine-product-boundary.md)가
제품 경계와 기존 결정의 적용 범위를 소유한다. 외부 플랫폼이나 에이전트를 필수 설치하지 않는다.

현재 구현은 Python 패키지와 CLI다. 아래 표는 소스의 연결 상태이며 전체 백테스트나
실제 공급자 호출 성공을 뜻하지 않는다. `aas status`의 `provider_collection`·
`daily_collection`은 전환 도구의 존재를 나타낸다. 내장 DB 수집 완료나 자동 예약 활성화로
해석하지 않으며 함께 출력되는 `provider_storage`를 확인한다.

| 현재 진입점 | 연결된 동작 | 연결되지 않은 동작 |
| --- | --- | --- |
| `aas preview` | `application/portfolio.py`의 명시적 세 모듈 비중 합성 | 모듈별 전략 실행·DB 읽기·주문 |
| `engine.load_bundle` / `engine.replay` | 외부 bundle 검증과 호출자가 주입한 입력 계산 | DB에서 전략·시장 입력 자동 선택 |
| `aas init/doctor/db/strategy/data` | `storage/`의 내장 DB 설치·등록·조회·복구 | 등록 전략을 실행하고 전체 결과를 확정하는 경로 |
| `aas providers/collect` | 기존 공급자·예산·실행 영수증 도구 | 수집기의 내장 DB 이식·스케줄러 자동 활성화 |

외부 도구는 현재 Python 계산 API 또는 CLI를 재사용할 수 있다. 모든 저장·수집 기능이
하나의 안정된 외부 API로 통합됐다는 뜻은 아니다. 안정된 외부 HTTP/MCP API는 제공하지 않는다. 선택형 [로컬 리소스 콘솔](design/local-resource-console.md)은 같은 프로세스에서 Python 저장 API를 호출하고 루프백 HTTP로 브라우저 화면만 제공한다.

## 모듈과 소유 범위

세 이름은 연구 영역을 구분한다. 현재 preview의 필수 입력 계약은 유지하지만 모든 외부
계산 API에 세 모듈을 강제하는 제품 요구로 확대하지 않는다. 장기 구조 변경은 후속 설계다.

| 모듈 | 책임 | 향후 입력 | 출력 경계 |
| --- | --- | --- | --- |
| 이지스 (`aegis`) | ETF 중심 자산배분 | 가격·유니버스·정규화한 배분 전략 | 모듈 내부 목표 비중 |
| 알파 (`alpha`) | 개별 종목·팩터 신호와 배분 | 시점이 고정된 재무·가격·전략 | 모듈 내부 목표 비중과 연구 근거 |
| 헷지 (`hedge`) | 방어 배분·헷지 판단 | 시장·위험 관측과 헷지 전략 | 보호 목적의 배분; 파생상품 계약은 후속 설계 |
| 포트폴리오 | 모듈 예산 배분과 결과 합성 | 세 모듈 결과 + 명시적 모듈 예산 | 자산별 총 비중·현금·모듈별 출처 |

모듈끼리 직접 호출하거나 서로의 저장 영역을 수정하지 않는다. 공통 데이터는
읽기 계약으로 공급하며, 전체 포트폴리오에서 모듈에 얼마를 배정할지는 포트폴리오
계층이 소유한다. 헷지 전략의 시장 판단과 주문 실행의 위험 제한은 별개 책임이다.

## 지금 실행되는 것

`application/contracts.py`의 입력과 `application/portfolio.py`의 순수 합성 함수가
CLI에 연결돼 있다. `modules/`는 각 모듈의 책임을 선언하며 전략 알고리즘을 구현한
것으로 표시하지 않는다. `aas status`가 구현 상태를 반환한다.

- 입력은 버전·기준일·모듈별 예산·세 모듈의 목표 비중을 명시한다.
- 같은 기준일의 세 모듈만 합성한다. 사용하지 않는 모듈도 예산 0과 빈 목표로
  명시하며, 누락을 현금으로 추정하지 않는다.
- 비중은 모듈에 배정된 자본 기준이다. 모듈 비중에 전체 포트폴리오 예산을 곱한
  뒤 같은 instrument ID끼리 합친다. 사용하지 않은 예산과 모듈 내부 잔여분은 현금이다.
- 음수·비유한 수·과도한 합계·중복·알 수 없는 필드는 거부한다.
- instrument ID는 불투명 식별자다. 티커 연결·상품 유형·거래 가능성은 검증하지
  않는다. 옵션 델타·선물 증거금·공매도 노출을 자본 비중으로 해석하지 않는다.
- preview는 입력을 읽고 JSON을 출력한다. DB 기록·외부 호출·실주문은 없다.

범용 계산은 `engine/`에 두고 전략 정의는 외부 비공개 저장소에서 공급한다.
[0011](decisions/0011-public-engine-private-strategies.md)이 공개·비공개 경계를 소유한다.
엔진은 명시한 자산·계산 기간·가중치·비교 조건·선택·전환 규칙을 검증하고 계산한다.
실제 전략 조합, 임계값, 성과와 보유 내역은 코드나 테스트에 내장하지 않는다.

입력 bundle은 ID·버전·원본 해시를 검증한 뒤 파싱한다. 실행 결과는 사용한 bundle과
계약 해시를 남긴다. 비공개 DB 연결이나 전략 전용 코드 실행을 자동 수행하지 않는다.
현재 CLI의 `preview`는 기존 비중 합성을 유지하며 `replay`를 호출하지 않는다.
`strategy import`는 `load_bundle`을 사용해 검증·저장하지만 전략 계산은 시작하지 않는다.
새 전략 DB의 정리 중인 export를 원본으로 자동 채택하지 않는다.

## 데이터와 DB

신규 DB는 [0014](decisions/0014-local-embedded-databases.md)에 따라 **SQLite 두 파일과
DuckDB 한 파일**을 기본으로 사용한다. PostgreSQL과 Parquet를 기본 설치에서 제외한다.
종목 이력·시점 재생·원본 출처·버전·사용 자격 모델은 유지한다. 상세 테이블·키·시점·게시·복구 기준은
[백테스트 데이터 설계](design/backtest-data-foundation.md)가 소유한다.

```text
목표 연결: 외부 앱·에이전트
  → AAS의 데이터·연구 요청 경계 (전체 연결 미구현)
  → 공통 reader · 계산 엔진 · 소유자별 writer
      ├─ state.sqlite3: identity·catalog·권한·작업·입력 pin·실행 영수증
      ├─ strategies.sqlite3: 별도 비공개 전략 원문·버전·계보·원래 성과
      ├─ market.duckdb: 시장·재무·거시·feature·대량 실행 결과
      └─ raw/ · runs/: 원본 bytes와 보고서
```

모듈은 파일 glob이나 최신 DB head를 직접 선택하지 않는다. 실행 입력 bundle은 데이터·identity·
universe·전략·시장 관례의 exact version/hash를 묶고, 각 의사결정 시점에서 가용했던 관측만
조회해야 한다. DuckDB 파일의 물리 hash와 dataset의 논리 내용 hash를 구분한다.
공통 transaction이 없는 파일 간 저장은 durable intent와 대상 완료 영수증을 대조해 확정한다.
내장 저장 경로는 `storage/`와 CLI `init`·`doctor`·`db`·`strategy`·`data`에 연결돼 있다.
전체 백테스트 입력 조합과 공급자 수집기의 전환은 별도로 검증한다.

**남아 있는 전환 전 코드**는 `legacy-db`·`legacy-data`와 일부 수집기다. 이 경로에만
`legacy` 추가 의존성과 PostgreSQL이 필요하다. 기본 설치는 SQLite·DuckDB를 사용한다.
전환 전 코드는 PostgreSQL DB 설치·기존 데이터 채택·카탈로그·가격 조회를 지원한다.
`metadata/runtime_install.py`가 빈 전용 template에 migration을 적용하고 일반 이름의
앱 DB를 생성한다. 관리 계정과 수집용 제한 계정을 분리한다. `snapshot_import.py`는
지원하는 이전 schema의 원본 열·생성 열·해시·행 수·수집 이력을 검증하고 한 transaction으로
채택 영수증까지 기록한다. 기존 DB를 테스트 이름으로 바꾸거나 원본 cluster에 덮어쓰지 않는다.

`data/catalog_access.py`와 `data/pinned_prices.py`는 명시한 dataset/version의 파일을 검증하고
가격 기준·기간·instrument·가용 시각으로 조회한다. 자격 없는 데이터는 `--inspect`로만
열람하며 백테스트 가능 상태로 승격하지 않는다. 실행 원장의 모듈 결합과 완료 기록 불변성은
DB에서 제한하지만, 전략 실행 엔진이나 전체 입력 bundle 검증을 완성한 것은 아니다.
앱 이미지와 DB 수명주기는 분리되고, CLI 미리보기는 DB 없이 실행된다.

`application/provider_config.py`는 공급자별 경로·한도·private 파일을 명시하고,
`provider_cli.py`가 기존 수집기를 연결한다. FMP는 완료 파일을 검증해 카탈로그를 등록하며,
FRED는 응답 페이지별 출처와 사용량 정산을, SEC는 응답 출처와 실행별 publication을 유지한다.
공급자가 코드에 있다는 사실과 실제 호출 가능한 상태는 다르다. 현재 정책·CIK·서명·gate가
부족한 공급자는 차단 이유를 반환하며 자격을 자동 승격하지 않는다.

`daily_collection.py`는 외부의 불변 시작·결과 영수증과 잠금으로 자동 실행을 조정한다.
같은 service day의 불확실한 실행을 유료 재호출하지 않는다. `compute_resources.py`는
호스트·컨테이너·운영자 상한에서 CPU와 메모리 예산을 정하고, 가격 조회의 파일 검증과
DuckDB 계산에 적용한다. 이 코드가 systemd 타이머의 설치·활성화까지 의미하지는 않는다.

후속 이식은 현재 데이터 계약·identity·metadata·collection의 의미를 보존해야 한다. Alembic chain은
전환 전 구현이며 새 SQLite/DuckDB migration으로 재사용하지 않는다. 새 경로가 같은 실패
조건을 검증하고 호출자를 대체하면 옛 저장 adapter·SQL·의존성을 제거한다. 과거 전략 oracle은 비공개 보관한다.
전체 미국 주식 데이터 기반의 준비 여부를 확인하고 특정 DAA 자산 목록으로 구축 범위를
축소하지 않는다. 개별 연구 입력의 품질·시점·권한과 전체 시장 coverage는 별도로 판정한다.

구버전 SQLite·VT 환경과 특정 운영 기록에 묶인 게시·복원 도구는 제거했다.
가격 파일 스키마는 `data/price_schema.py`, 현재 SEC 정책 pin은 `data/sec_policy.py`가
소유한다. 유지되는 데이터 모델과 제거 범위는 [0012](decisions/0012-retire-legacy-runtime.md)를 따른다.

내장 DB 백업·복원 절차는 [operations](operations.md#검사복구백업)가 소유한다.
PostgreSQL archive·dump는 별도 [구형 백업 검토 기준](design/standalone-backup-inventory.md)을 따른다. 실제 백업 선택·복원·schema 이식·수집 재개는
각각 검증을 거쳐야 한다. DB 채택 명령이 수집 재개까지 자동 수행하지는 않는다.

## 설치 경계

기본 설치는 네이티브 CLI다. `aas init`으로 `~/.aas`에 빈 SQLite 두 개와 DuckDB를 만들고,
`aas doctor`로 위치와 상태를 확인한다. `--home`·`AAS_HOME`으로 루트를 지정할 수 있다.
DB 서버·별도 Docker·DB 계정이 필요 없다. Dockerfile과 기본 Compose는 AAS 한 개를 포장하는
선택사항이며 사용자 자료는 외부 디렉터리에 둔다. release artifact 게시·이미지 배포는 아직 없다.

`legacy-db`·`legacy-data`와 전환 전 수집기·별도 데이터 Compose는 이전 계약을 검증하는 코드다.
기본 앱 설치에는 PostgreSQL·Parquet 의존성을 설치하지 않는다. 이 전환용 경로는 `legacy`
추가 패키지가 필요하며 수집기 이식 후 제거한다.

## 검증과 다음 구현 순서

`./scripts/verify`는 신규 앱과 보존 코드의 회귀를 검사하며 기존 CI gate 이름을 유지한다.
실제 DB 복원 성공·전략 수익성·주문 안전성을 이 테스트 결과로 주장하지 않는다.

후속 구현은 [L1~L6 상태와 남은 작업](design/backtest-data-foundation.md#전환-계획과-기존-코드)을 따른다.
이미 연결된 설치·등록·조회·복구를 바탕으로 수집기와 실행 경로를 이식한 뒤 구경로를 제거한다.
실제 공급자 연결 검증과 스케줄러 활성화는 별도다. 알파·헷지는 같은 모듈 계약 아래
검토한다. [0013](decisions/0013-first-install-workspace.md)의 상시 프로세스·로컬 소켓은
승인됐지만 미구현인 설계다. 외부 소비자·HTTP/MCP 선택과 장기 모듈 구조는 별도 후속 판단이다. 실주문은 전략 연구와 별도 실행 adapter에서 검증한 뒤 연다.

SEC 공식 bulk 원본 확보는 `data/sec_bulk.py`가 담당한다. 원본 압축본의 무결성 영수증과
DB 카탈로그 등록은 구분한다. `data/sec_periods.py`는 VT에서 이식한 기간 시작·끝 기준의
분기/누적 구분이며, 기존 SEC 정규화 버전에 자동 적용하지 않는다. 이식 출처와 변경은
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)에 기록한다.
