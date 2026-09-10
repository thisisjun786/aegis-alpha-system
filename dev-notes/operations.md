# AAS operations

현재 운영 진입점은 네이티브 CLI다. 외부 도구도 이를 호출할 수 있으며, Python 계산 API의
역할과 연결 한계는 [구조](architecture.md)가 설명한다. SQLite와 DuckDB는 앱 내부에서 파일을 열며 별도 DB 서버나
Docker가 필요 없다. [설계](design/backtest-data-foundation.md)와 [설치 결정](decisions/0013-first-install-workspace.md)이 저장 계약을 소유한다.

## 처음 설치

```bash
uv tool install .
aas init
aas doctor
```

소스 대신 로컬 wheel을 설치할 수도 있다. 원격 release가 게시됐다는 뜻은 아니다.
기본 위치는 `~/.aas`, 선택 순서는 `--home`, `AAS_HOME`, 기본값이다.
`runtime.json`의 상대 경로는 이 루트에서 해석한다. 사용자 설정 파일과 DB는 0600,
디렉터리는 0700으로 유지한다. 자료는 Git checkout 밖의 로컬 Linux 파일시스템에 둔다.

`init`은 빈 상태·전략 SQLite와 시장 DuckDB, raw/runs/secrets/backups/runtime 디렉터리를
만든다. 재실행은 저장 내용을 유지하고 중단된 초기화를 재개한다. 다른 설치의 DB는 자동
채택하지 않는다. 전략 없음·데이터 없음은 정상 초기 상태이며 provider나 scheduler를 시작하지 않는다.
`doctor`는 경로·DB 종류·등록 수를 표시한다. 키나 전략 원문은 출력하지 않는다.

## 전략과 데이터

```bash
aas strategy import /path/to/bundle.json --id ID --version VERSION --sha256 SHA256
aas strategy list
aas data datasets
aas data import /path/to/typed-data.json --sha256 SHA256
aas data inspect --dataset ID --version VERSION
aas data read --dataset ID --version VERSION --cutoff-us UTC_MICROSECONDS
```

전략은 `engine.bundle`의 envelope를 검증하고 원문과 계약 해시를 저장한다. 원래 성과를
추정해 채우지 않는다. 현재 bundle은 실행 계약이며 별도 원본 성과 입력 UI는 제공하지 않는다.
`data import`의 스키마는 [import_document.py](../src/aegis_alpha/storage/import_document.py),
도메인별 필드는 [market_schema.py](../src/aegis_alpha/storage/market_schema.py)에 있다.
가격·재무·거시·기업행동·종목 상태·추정치·FX·calendar·feature를 typed 테이블로 저장한다.

데이터 버전과 부모 generation을 고정하며 수정·철회는 새 revision이다. cutoff 없이 읽으면
검사 목적이고, cutoff가 있으면 공개·수정 인지 시각이 모두 알려진 행만 읽는다.
`--ingestion-cutoff-us`는 실제 수집 당시 재생 조건을 추가한다. 로컬 import는 AAS가 가져온
시각을 기록하며 파일에 적힌 수집 시각을 시스템 관측으로 인정하지 않는다. 공급자의 조정 가격은 참고값이며
PIT 입력으로 승격하지 않는다. 조회·게시만으로 coverage·backtest 자격이 확보됐다고 표시하지 않는다.

## 원본 자료 이전과 조회

```bash
aas db source-import /path/to/private-snapshot.sqlite3 --id SOURCE_ID --sha256 SHA256
aas db sources
aas db source-tables --source SOURCE_ID
aas db source-read --source SOURCE_ID --table TABLE_NAME --limit 20
```

`source-import`는 원본 SQLite 테이블을 비공개 원본 자료실에 보존한다. 먼저 읽기 전용
연결의 SQLite backup API로 일관된 사본을 만들고, 닫힌 사본의 해시를 지정한다.
원본의 실행 코드·뷰·트리거를 실행하거나 기존 엔진 bundle로 추정 변환하지 않는다.
원본 설정, 연구용 설정, 원래 성과는 각각 원래 테이블과 열의 의미를 유지한다.
`strategy list`는 검증된 실행 bundle 목록이며 `db sources`의 원본 자료 목록과 구분된다.

대량 분석 자료는 `storage.source_library.import_arrow`로 명시적인 Arrow reader에서
DuckDB에 적재한다. 같은 스키마끼리 묶고 파일 경로와 원본 행 번호를 보존한다.
원본 시각이나 숫자의 정밀도를 임의로 줄이지 않는다. 원본 자료실 등록은 PIT 사용 자격이나
백테스트 실행 성공을 뜻하지 않으며, `data datasets`의 게시된 데이터 버전에 자동 추가되지 않는다.
대량 이전은 원본 파일·행 번호를 유지한 여러 source로 나눠 적재할 수 있다. 행 수만으로
메모리 사용량을 판단하지 않으며, 실제 사용량과 처리 속도에 맞춰 작업 단위를 조정한다.
DuckDB의 메모리 설정은 전체 Python 프로세스의 메모리 한도가 아니다.
고정 행 묶음을 하나의 Arrow batch로 합칠 수 없으면 원본 적재를 취소한다.
큰 문자열·바이너리 값은 원본 스키마에서 large-offset 형식을 명시해야 한다.
적재 실패 시 완료 목록에 노출하지 않으며 같은 원본 ID와 해시로 재시도할 수 있다.

검토한 시뮬레이션 결과를 원본 자료실에 보존할 때도 입력·출력 해시와 검토 근거를 함께
기록하고, 저장 후 읽기 전용 연결에서 원래 행과 대조한다. 이 등록은 실행 가능한 전략
등록이나 기존 실행 집계 변경과 별개다. 일부 저장만 끝난 상태를 완료로 기록하지 않는다.

기존 원본과 수집 설정을 유지한 채 새 홈으로 이전한다. 실제 자료·파일 목록·원본 해시·
행 수 대조 결과는 비공개로 기록한다. 원본 파일을 보존할 때는 `raw/` 아래에 스트리밍으로
저장하면 기존 백업에 포함된다. 전체 검증은 모든 등록 원본을 다시 읽고, 백업은 추가 사본을
만드므로 자료 크기에 맞는 디스크 공간과 I/O 시간이 필요하다. 원본 자료 이전만으로
기존 수집기나 예약 작업의 저장 위치가 바뀌지는 않는다.

## 검사·복구·백업

```bash
aas db verify
aas db recover
aas db quarantine --operation OPERATION_ID --reason REASON
aas db backup --output /path/to/new-backup
aas --home /path/to/new-home db restore --backup /path/to/new-backup
```

파일 간 저장은 state의 PREPARED 의도, 대상 DB commit, 최종 카탈로그 순서다. `recover`는
완료 marker·요청·원본·논리 해시를 대조해 게시만 재개한다. 공급자 호출을 다시 하지 않는다.
marker가 없는 작업은 pending으로 남긴다. 재개하지 않을 작업은 명시적 `quarantine`으로
이력과 원본을 보존한 채 격리한다. 정상 카탈로그에 없는 데이터는 자동 채택하지 않는다.

백업은 모든 설치·파일 잠금을 잡고 pending 작업과 실행 중 분석이 있으면 거부한다.
SQLite backup API, DuckDB CHECKPOINT와 연결 종료 후 복사를 사용한다. 외부 경로의
raw/runs도 포함하며 키·provider 설정과 절대 운영 경로는 제외한다. 백업은 비공개 데이터다.
같은 디스크의 사본은 디스크 장애를 보호하지 않으므로 필요한 경우 백업 세트를 다른 매체에 보관한다.

복원은 존재하지 않는 새 루트만 받는다. 파일·스키마·FK·논리 해시 검증이 끝나야 ready로 바꾸며
실패한 복원은 정상 설치로 열지 않는다. store ID는 유지하고 deployment ID는 새로 만든다.
지원 schema는 [저장소 검증 코드](../src/aegis_alpha/storage/workspace.py)가 기준이다.
자동 업그레이드·다운그레이드와 자료 삭제 명령은 제공하지 않는다.

## 명시한 ETF 목표 비중 재생

`aas backtest --input request.json --sha256 <파일의-SHA256>`는 명시한 ETF 목표 비중을
다음 공급 거래일 시가에 체결하고 비용·현금·일별 NAV를 반환한다. 입력은 최대 64MiB이며
스키마는 `aas-etf-backtest-v1`이다. 필수 필드는 `module`(aegis), `instrument_types`,
`dates`, `opens`, `closes`, `targets`, `initial_cash`, `cost`, `source_pins`,
`research_mode`다. 날짜는 YYYY-MM-DD, 가격 배열의 각 원소는 종목 ID와 숫자의 객체다.
`targets`는 의사결정 날짜별 종목 목표 비중이며 현금은 비중의 나머지로 표현한다.

양수 목표 비중의 종목 유형은 ETF여야 한다. `research_mode`는 합성 입력의 `synthetic`
또는 실제 ETF 자료를 제출하는 `observed_etf_research`다. 제출한 분류·가격의 진위와
달력 완전성은 호출자 책임이다. `source_pins`의 각 항목은 `source_id`, `source_sha256`,
`table`, `table_digest`이며 이 명령은 해당 DB를 읽어 출처를 검증하지 않는다. 출력의
`source_pins_verified`, `observed_prices_verified`, `point_in_time_verified`는 false다.
원본 전략 규칙의 자동 해석이나 실주문을 시작하지 않는다.

여러 전략을 외부 실행기로 재현할 때는 입력·계산 코드·환경을 고정하고, 전략별로
원본 규칙과 연구용 변형의 결과를 따로 기록한다. 이전 결과와 비교할 때 날짜와 숫자뿐
아니라 열 자료형도 확인한다. 허용 오차 안의 일치와 완전히 같은 결과를 구분한다.
중단 후에는 해시로 검증한 완료 결과만 재사용하고, 미완료·실패 과정에서 남은 파일은
DB 저장 대상에서 제외한다. 저장한 결과를 다시 열 때는 선택한 출처 명세와 결과 영수증,
원본 파일 목록, 테이블 내용·행 수를 모두 대조한다. 연구용 재현의 일치는 원본 전략의
비용·납입일·시점 가용성까지 확인했다는 뜻이 아니다.

### 날짜별 납입·출금

같은 `aas backtest` 명령에 `schema_version="aas-etf-backtest-v2"`를 제출하면
v1의 모든 필드와 `cashflows` 배열이 필요하다. 예를 들어
`[{"date":"2024-01-04","amount":100.0}]`은 해당 거래일 시가에서 100을 납입한다.
양수는 납입, 음수는 출금이며 계좌와 같은 통화다. 배열이 비어 있으면 입출금 없이 계산한다.

날짜는 제출한 거래일에 속하고 오름차순이어야 한다. 첫 기준일의 입출금, 중복 날짜,
0·무한대·숫자가 아닌 금액은 거부한다. 휴일 이동이나 반복 납입일 추정은 하지 않는다.
입출금은 보유 자산을 해당 시가로 평가한 뒤, 직전 종가의 목표 비중을 체결하기 전에
반영한다. 출금액이 기존 현금보다 크면 거부한다. 매도 예정 자산도 출금 재원으로
미리 계산하지 않는다. 목표 비중 체결이 없는 날의 납입액은 현금으로 남는다.

v2의 `result.account`에는 기존 형식의 계좌 NAV·체결 내역이 담긴다.
`result.unit_nav`는 `date`, `unit_value`, `units`, `external_flow`를 담는다.
초기 단위당 가치는 1이고, 입출금 직전 단위당 가치로 단위를 발행·상환한다.
납입액은 계좌 잔액을 늘리지만 투자 수익으로 기록되지 않는다. 수수료는 수익률에 반영된다.
`result.cashflows`는 검증한 입력 내역이며 `cashflow_convention`은 처리 순서를 설명한다.
원본 전략의 현금 흐름·시점·자료 검증은 별도로 필요하다. v1은 기존 응답을 유지하고
`cashflows` 필드를 받지 않는다.

## 명시한 ETF 연구 후보 생성

`aas research --input request.json --sha256 <파일의-SHA256>`는 최대 64MiB의 입력에서
연구 후보를 생성한다. 스키마는 `aas-etf-research-v1`, `module`은 `aegis`, `action`은
`generate`다. 나머지 필수 필드는 `parent_hash`, `seed`, `grid`, `train_end`,
`validation_end`, `test_end`, `cost_ref`, `max_trials`다. 날짜는 YYYY-MM-DD 형식이고
세 평가 구간의 끝 날짜가 순서대로 증가해야 한다. `max_trials`는 1~256의 정수다.

`grid`에는 `etf_universes`, `instrument_types`, `momentum_horizons`, `absolute_filters`,
`trend_filters`, `weightings`, `volatility_caps` 배열을 명시한다. 종목 유형은
`[["ETF_A", "ETF"], ["ETF_B", "ETF"]]`처럼 ID·유형 쌍으로 제출하며, 후보 종목은 모두
ETF여야 한다. 가중 방식은 `equal` 또는 `inverse_volatility`, 변동성 상한은 양의 숫자다.
유형 표시의 진위와 비용 참조는 이 명령이 검증하지 않는다.

출력은 해시가 포함된 전체 후보 명세와 개수다. `research_candidate_generation`만
활성 기능으로 보고하며 `evaluation_performed`는 false다. 가격 조회·전략 실행·DB 저장·
최종 시험 구간 평가를 시작하지 않는다. 학습·검증 평가와 별도 최종 평가가 필요하면
Python 연구 API에 평가 함수를 명시하고, 실행 측에서 이전 평가 이력을 보존해야 한다.

## 명시한 ETF 프로필 비교

`aas etfs --input request.json --sha256 <파일의-SHA256>`는 최대 64MiB의 입력을
검증한 뒤 현재 ETF와 최대 256개 후보를 비교한다. 최상위 필드는 `schema_version`,
`module`, `action`, `current`, `candidates`, `policy`이며 각각의 고정값은
`aas-etf-comparison-v1`, `aegis`, `compare`다. 해시가 다르거나 JSON 키가 중복되면 거부한다.

프로필은 `instrument_id`, `exposure_id`, `currency`, `hedged`, `leverage`, `reset`,
`fee_bps`, `inception`, `as_of`, `source_hash`, `tracking`, `liquidity`를 모두 포함한다.
확인하지 못한 보수·상장일·자료일·출처 해시·추적오차·유동성은 `null`로 제출한다.
`tracking` 객체에는 `start`, `end`, `value`, `source_hash`, `basis`가 필요하다.

정책은 `as_of`, `max_profile_age_days`, `min_liquidity`, `min_fee_saving_bps`,
`max_tracking_error`, `tracking_start`, `tracking_end`, `tracking_basis`를 포함한다.
날짜는 YYYY-MM-DD 형식이다. 추적오차 기간과 수익률 기준이 정책과 맞지 않거나
자료가 누락·만료되면 `insufficient_evidence`로 남긴다. 가격 총수익과 NAV 총수익 등
서로 다른 기준을 같은 이름으로 제출해서는 안 된다.

비교 결과는 출력의 `result`에 담긴다. `research_only=true`,
`automatic_replacement=false`, `source_pins_verified=false`는 유지된다.
입력 출처의 진위 확인, 프로필 수집, DB 저장과 정기 실행은 호출 측의 별도 책임이다.
여러 노출을 조사하는 실행 측은 비교 그룹마다 기준지수·기간·후보를 명시하고 그룹 안에서만
비교해야 한다. 이미 완료한 기간은 당시 설정과 저장 근거로 검증하며, 새 설정으로 과거
결과를 다시 해석하지 않는다. 프로필이 없는 대상도 자료 부족 사유와 함께 남긴다.

## 연구용 과거 수익률 연결

외부에서 수집한 Nifty 가격지수 JSON은 `data.nifty_history.parse_nifty_price_history`에
원본 바이트, 고정한 SHA256, 정확한 제공자 지수명과 조회 시작·종료일을 전달해 검증한다.
UTF-8 BOM과 제공자의 `d` 응답 포장을 처리하며, 잘못된 지수명·중복 날짜·조회 기간 밖의
행·유효하지 않은 종가는 거부한다. 빈 결과는 자료 없음으로 남긴다. 이 Python 함수는
수집이나 DB 저장을 실행하지 않으며, 가격지수를 ETF 총수익으로 바꾸지 않는다.

`aas proxy --input request.json --sha256 <파일의-SHA256>`는 최대 64MiB의
`aas-proxy-returns-v1` 입력을 읽는다. 필수 필드는 `schema_version`, `module`(aegis),
`target_type`(ETF), `donor`, `target`, `recipe`다.

두 시계열은 `instrument_id`, `currency`, `return_kind`(price_return 또는 total_return),
`close_convention`, `net_of_fees`, `anchor_date`, `dates`, `returns`, `source_sha256`을
명시한다. 각 수익률은 직전 날짜 또는 anchor_date부터 해당 날짜까지의 변화다.
날짜는 중복 없이 증가해야 하며 두 시계열에 공통 전환 경계가 있어야 한다.

`recipe`는 `target_id`, `donor_id`, `switch_date`, `annual_fee`, `fee_model`, `reason`을
담는다. `already_net`은 비용 반영 후 수익률과 추가 비용 0만 허용한다.
`annual_expense`는 비용 반영 전 수익률에 연간 비용을 실제 경과 일수로 나눠 적용한다.
`zero_expense_sensitivity`는 비용 0을 가정한 민감도 분석이며 그 이유를 명시해야 한다.
목표 ETF 수익률에는 비용이 이미 반영돼 있어야 한다.

결과는 날짜별 연구 지수이며 체결 가격으로 사용할 수 없다. 지수·현물 자료를 기초
시계열로 제출할 수 있지만 가격수익과 배당 재투자 총수익, 서로 다른 종가 시각을
섞을 수 없다. 같은 문자열을 제출했다는 사실은 실제 자료가 일치한다는 증거가 아니다.
DB 출처·ETF 유형·시점 검증은 별도 호출자가 담당하며 결과에도 미검증 상태가 남는다.

Python에서 `engine.reset_returns`의 `ResetCosts`, `ResetRecipe`,
`build_reset_returns`를 직접 호출할 수 있다. `ResetCosts`에는 `anchor_date`,
`dates`, `financing_drag`, `collateral_return`, `expense_drag`, `source_sha256`,
`convention`, `basis`를 전달한다. `convention`은 `fraction_of_starting_nav`이며 각 항목은
기간 시작 NAV의 비율이며 차입 규모 반영은 호출자가 맡는다. 자금조달비와 보수는 음수를 허용하지 않고, 담보수익은
음수도 허용한다. `basis`는 `observed_inputs`, `explicit_assumptions`, `zero_sensitivity` 중 하나다.
마지막 값은 모든 비용·수익 항목이 0일 때만 허용한다. 이 표시는 호출자의 주장이고
자료를 인증하지 않는다. `source_sha256`은 비용 입력 파일의 식별값이다.
`ResetRecipe`에는 일정한 `multiplier`와 가정을 설명하는 `reason`을
명시한다. `expected_sessions`는 시작일을 포함하며 두 입력의 모든 날짜와 같아야 한다.

기간 수익률은 `배율 × 기초 수익률 − 자금조달비 + 담보수익 − 보수`다. 비용이 이미
반영된 기초 입력은 거부한다. 연간 금리, 차입 원금, 실제 ETF 비용을 자동 추정하지
않으며 무비용 시나리오도 모든 비용 배열에 0을 명시해야 한다. 일별 자료인지와
휴장일 누락 여부는 호출자가 확인해야 한다. 이 함수에는 별도 CLI 명령이 없고,
기존 `aas proxy` 입력 형식은 그대로다.

## 선택적 단일 컨테이너

```bash
install -d -m 700 "$HOME/.aas"
export AAS_UID="$(id -u)" AAS_GID="$(id -g)"
docker compose build aas
docker compose run --rm aas init
docker compose run --rm aas doctor
```

기본 Compose는 AAS 앱 하나와 사용자 루트 bind mount만 사용한다. DB 서비스·named DB volume·
공개 포트가 없다. 이미지에 DB·전략·키를 넣지 않는다. 외부 데이터 경로를 사용하면 해당 경로도
컨테이너에 명시적으로 연결해야 한다. 동시에 실행한 CLI는 설치 잠금으로 거부한다.
상시 앱의 소켓·예약 실행은 [0013](decisions/0013-first-install-workspace.md)에서 승인됐지만
미구현인 설계다. 현재 명령은 소켓으로 전달되지 않으며 잠금 충돌 시 `installation_busy`로 실패한다.

## Qveris 원문 수집

`aas collect daily --config /path/to/collection.json --state-root /path/to/private-journal`은
명시한 공급자 설정으로 하루 한 번 수집을 접수한다. Qveris 설정은 `provider=qveris`,
`mode=daily`, 비공개 `credential_file`, 양수 `max_calls`를 요구한다. `options`에는
`jobs`, `jobs_sha256`, `raw_store_root`, `max_credits`, `timeout_seconds`를 지정한다.
작업 문서는 [qveris_contracts.py](../src/aegis_alpha/data/qveris_contracts.py), 설정 검증은
[provider_config.py](../src/aegis_alpha/application/provider_config.py)가 소유한다.

작업 해시가 다르면 키를 읽기 전에 실패한다. 유료 호출 전에 견적과 잔액을 확인하며,
원문을 저장한 뒤 사용량을 대조한다. 완료된 작업은 재사용하고 결과가 불확실한 호출은
자동 재시도하지 않는다. 같은 UTC 날짜의 접수 기록을 유지해야 중복 실행을 막을 수 있다.
`provider_calls`와 `http_requests`는 각각 유료 실행과 전체 HTTP 시도 수다.

이 명령은 원문 수집까지만 수행하며 `native_import_completed=false`를 반환한다.
검증된 원문을 DuckDB 원본 자료실에 적재하려면 별도의 명시적 적재 작업이 필요하다.
가격의 조정 기준·종목 식별·거래일·공개 시각을 확인하기 전에는 백테스트 입력이나
`data datasets`의 시장 버전으로 자동 승격하지 않는다. 예약 실행은 운영자가 별도로
설치하고 실제 적재 결과와 중복 호출 여부를 확인한다. 패키지 설치는 예약 작업을 만들지 않는다.

## 전환 중인 공급자 도구

`aas providers`와 기존 수집기는 유지되지만 일부는 아직 PostgreSQL/Parquet adapter를 쓴다.
이 경로는 `uv tool install '.[legacy]'` 또는 개발 환경과 명시한 이전 설정이 필요하다.
기존 DB 명령은 `aas legacy-db`, publication 조회는 `aas legacy-data`로 구분한다.
이 도구를 새 `state.sqlite3`에 연결하거나 실제 보관 데이터를 자동 채택하지 않는다.
`docker-compose.data.yml`과 이전 설치 실행기는 이 전환 경로이며 새 설치 절차가 아니다.

라이브 공급자 검증·기존 DB 이전·스케줄러 활성화·실주문은 위 오프라인 설치 검사의 범위에
포함되지 않는다. CLI preview는 합성 비중 계산이고 전체 백테스트는 아직 별도 구현이다.
