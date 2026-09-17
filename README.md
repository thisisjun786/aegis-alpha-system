# Aegis Alpha System

AAS는 외부 앱과 에이전트가 사용하는 데이터·투자 연구 엔진을 목표로 한다. AAS가 데이터와
계산을 제공하고 외부 도구가 화면과 작업 진행을 맡는다. 현재는 범용 계산 Python API와
데이터·전략 저장, 저장 입력의 백테스트 준비, 비중 합성 CLI를 제공하며, 수집기는 전환 중인
PostgreSQL 저장 경로를 사용한다.
공개 레포에는 범용 엔진과 입력·저장 계약을 두고, 전략 정의와 사용자 데이터는 별도 비공개 저장소에서 관리한다.
라이선스는 Apache-2.0이다.

## 설치와 첫 실행

로컬 소스에서 앱을 설치한다. Python 환경은 `uv`가 분리해서 관리한다.

```bash
uv tool install .
aas init
aas doctor
aas strategy list
```

기본 경로는 `~/.aas`다. SQLite와 DuckDB가 앱 안에서 작동하므로 PostgreSQL 서버나
별도 Docker는 필요 없다. 다른 로컬 디스크를 쓰려면 `aas --home /path/to/aas init` 또는
`AAS_HOME`을 지정한다. 설치는 전략·공급자 키·예약 수집을 자동으로 추가하지 않는다.

```text
~/.aas/
  runtime.json         경로·자원 설정
  installation.json    설치·스키마 정체성
  state.sqlite3        상태·출처·데이터 카탈로그
  strategies.sqlite3   비공개 전략 원문·버전·참조 성과
  market.duckdb        가격·재무·거시·기업행동·분석 데이터
  raw/                 해시로 보존한 원본
  runs/                실행 산출물
  secrets/             공급자 키
  backups/             비공개 백업
  runtime/             실행 상태
```

지원 대상은 로컬 Linux 파일시스템이다. 영구 저장소는 Git checkout 밖에 둔다.
공유 네트워크 파일시스템이나 여러 컴퓨터가 동시에 여는 DB는 지원하지 않는다.

## 전략·데이터·백업

```bash
aas strategy import /path/to/bundle.json --id ID --version VERSION --sha256 SHA256
aas data datasets
aas db verify
aas db backup --output /path/to/new-backup
aas --home /path/to/new-home db restore --backup /path/to/new-backup
```

등록은 원문 bytes와 정확한 ID·버전·해시를 함께 검증한다. 원본 파일을 지워도 등록 내용은
전략 DB에 남는다. 같은 버전에 다른 내용을 덮어쓰지 않는다. 기본 전략은 없다.
백업은 SQLite snapshot과 연결을 닫은 DuckDB, 원본·산출물을 묶으며 공급자 키는 제외한다.
복원은 새 디렉터리에서 검증하며 기존 데이터를 덮어쓰지 않는다.

`aas data import`는 명시한 로컬 typed JSON envelope를 검증해 버전으로 게시한다.
`aas data read --dataset ID --version VERSION --cutoff-us UTC_MICROSECONDS`는 그 시점에
알려진 수정 이력을 읽는다. 데이터 조회를 전체 백테스트나 실거래 성공으로 표시하지 않는다.
자세한 인자와 한계는 [운영 안내](dev-notes/operations.md)에 있다.

## 엔진과 실행 범위

`aas preview --input examples/portfolio-preview.json`은 합성 모듈 비중을 결합한다.
`aegis_alpha.engine.load_bundle`과 `replay`는 명시한 외부 전략과 검증한 입력을 계산하는
Python API다. `aas prepare --request FILE --sha256 SHA256 --output ENVELOPE`는 등록한
전략과 고정한 관례·세션·가격·membership pin만 읽어 판단별 목표 비중을 `aas backtest` 봉투와
출처 sidecar로 내보내며, 체결 계산과 run 등록은 하지 않는다. 봉투 하나만 따로 계산하려면
`aas backtest`를 쓴다. 상시 앱과 실주문은 아직 구현 전이다.
요청 형식과 등록 선행 조건은 [운영 안내](dev-notes/operations.md#저장한-전략과-입력의-준비)에 있다.

준비부터 결과 저장까지 한 번에 하려면 `aas run`을 쓴다. 같은 요청 문서로 준비하고,
요청을 등록하고, 잠금 없이 계산한 다음 결과를 run으로 확정한다. run 저장 표는 기본
설치에 없으므로 `aas db run-install`을 먼저 한 번 실행한다. 설치 전에 실행하면 계산하기
전에 그 명령을 알려주며 실패한다.

```bash
aas db run-install
aas run execute --request request.json --sha256 SHA256
aas run show --run-id run-0123456789abcdef
aas run list
```

`aas run execute`는 결과가 저장된 뒤에만 영수증을 돌려준다. 영수증에는 준비 hash,
판단별 목표 비중, 체결·NAV가 담긴 `backtest`, 그리고 run ID·결과 hash·표별 행 수와
지표가 담긴 `run`이 들어 있다. `--envelope-output PATH`를 주면 봉투와 출처 sidecar를
파일로도 남긴다.

```json
{"executed": true, "request_hash": "3d4bd0e5...", "bundle_id": "inputs-9f2c...",
 "target_weights": {"2026-01-29": {"ASSET_A": 1.0}, "2026-02-26": {"ASSET_B": 1.0}},
 "backtest": {"module": "aegis", "live_orders": false,
              "result": {"nav": [{"date": "2026-03-30", "equity": 80.0, "cash": 0.0}]}},
 "run": {"run_id": "run-0123456789abcdef", "status": "SUCCESS",
         "result_hash": "8c68a7c3...", "metrics": {"final_equity": {"value": "80.000000000000"}}},
 "certified": false}
```

`aas run show --run-id ID`는 기록·표시자·봉인 파일이 여전히 서로 맞을 때만 그 run을
돌려준다. 같은 요청을 CLI로 돌리든 `aegis_alpha.application.run_backtest.run_backtest`
Python API로 돌리든 run ID만 다르고 준비 hash·목표 비중·체결·NAV·비교 상태는 같다.
계산 도중 프로세스가 죽으면 run은 RUNNING으로 남고 `aas db recover`가 기존 계약대로
INTERRUPTED로 끝낸다. 회복은 다시 계산하지 않는다.

이지스·알파·헷지는 자산배분·개별 종목·방어 배분의 연구 영역이다. 현재 preview 입력은
세 모듈을 모두 명시해야 하지만, 각 모듈이 전략을 실행하는 것은 아니다. 전략 등록은
bundle을 검증·저장하며 실행을 시작하지 않는다. `aas run`의 통합 실행은 이지스 봉투를
내보내는 등록 전략에만 해당하고, 알파·헷지 전략 자료나 실주문을 뜻하지 않는다.
HTTP/MCP 서버와 상시 daemon은 제공하지 않는다. [제품 경계 결정](dev-notes/decisions/0015-research-engine-product-boundary.md)에
목표와 현재 구현의 차이를 기록했다.

기존 공급자 수집기는 전환 중이다. `legacy-db`·`legacy-data`와 해당 수집기, 그리고
`storage.source_library.import_arrow`의 Arrow 적재는 `legacy` 추가 의존성이 필요하다.
새 기본 설치에는 PostgreSQL 드라이버·Alembic·PyArrow를 설치하지 않는다. 이 전환용 경로는
`uv sync --locked --dev`의 잠긴 개발 환경에서만 확인했고 기본 설치로는 검증하지 않았다.
native 경로는 다르다. 등록부터 run 실행·재조회·백업·새 루트 복원·중단 복구까지는
`scripts/verify-lane-build`가 깨끗한 환경에 설치한 wheel로 직접 돌려 확인한다.
의존성은 `pyproject.toml`과 `uv.lock`이 정본이다. 수집기 이식 후 전환용 경로를 제거한다.

Docker는 앱 하나를 포장하는 선택사항이다. 기본 Compose에는 DB 서비스가 없다.
컨테이너 사용 절차는 [운영 안내](dev-notes/operations.md)를 따른다.

## 개발과 문서

```bash
uv sync --locked --dev
uv run --no-sync pytest -m 'not database'
uv run --no-sync ruff check .
uv run --no-sync ty check src tests
```

- [구조](dev-notes/architecture.md), [DB 설계](dev-notes/design/backtest-data-foundation.md)
- [설치 결정](dev-notes/decisions/0013-first-install-workspace.md), [내장 DB 결정](dev-notes/decisions/0014-local-embedded-databases.md)
- [공개 엔진·비공개 전략 경계](dev-notes/decisions/0011-public-engine-private-strategies.md)
- [개발·CI 정책](POLICY.md), [기여 안내](CONTRIBUTING.md), [보안](SECURITY.md)
- [Apache-2.0](LICENSE), [외부 코드 고지](THIRD_PARTY_NOTICES.md)

실제 전략·자산 구성·성과·수집 데이터·키·운영 경로는 패키지와 CI에 포함하지 않는다.
