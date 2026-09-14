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
aas strategy import /path/to/child.json --id ID --version VERSION --sha256 SHA256 \
  --parent-id PARENT_ID --parent-version PARENT_VERSION --change-kind KIND --reason REASON
aas strategy list
aas strategy show --id ID --version VERSION --sha256 SHA256
aas strategy show --id ID --version VERSION --sha256 SHA256 \
  --requirements /path/to/requirements.json --requirements-sha256 SHA256
aas data datasets
aas data import /path/to/typed-data.json --sha256 SHA256
aas data inspect --dataset ID --version VERSION
aas data read --dataset ID --version VERSION --cutoff-us UTC_MICROSECONDS
```

전략은 `engine.bundle`의 envelope를 검증하고 원문과 계약 해시를 저장한다. 원래 성과를
추정해 채우지 않는다. 현재 bundle은 실행 계약이며 별도 원본 성과 입력 UI는 제공하지 않는다.
같은 바이트와 같은 계보로 다시 import하면 현재 실행 정의 검증을 통과하는 bundle은
그대로 성공하고, 내용이나 계보가 다르면 거부한다.
계보 네 옵션은 모두 지정하거나 모두 생략한다. 부모가 아직 등록되지 않았어도 import는
성공하지만 그 버전은 `unresolved`로 남고, 나중에 부모를 등록해도 바뀌지 않는다.
바로잡으려면 새 자식 버전을 등록한다. state 작업이 PREPARED로 남은 채 커밋된 import는
`aas db recover`가 저장 내용을 대조해 완료하며, 이 완료가 실행 자격을 뜻하지는 않는다.
부모 상태는 첫 PREPARED 커밋 직전의 승인 시점에 v2 요청 해시로 고정한다. private 커밋 전에
중단되어도 같은 입력의 재시도는 그 상태를 유지하며, 영수증이 없으면 recover는 pending으로 남긴다.
계보 없는 v1은 그대로 지원한다. 상태를 봉인하지 않은 중간 개발 버전의 caller-only 계보 v1은
원본을 보존하지만 조회·재봉인·검증·복구 완료·백업·ready 복원은 거부한다. 자동 변환은 없다.
정확한 바이트 규약과 호환 한계는 [계보 프로토콜](design/strategy-lineage.md)에 있다.

`strategy show`는 고정한 전략의 실행 정의를 JSON으로 출력하며 계산·기록을 하지 않는다.
출력에는 자산·현금 ID, 역할별 입력 요구, 달력 규약, `required_convention_roles`,
`unresolved_convention_roles`, `executable`이 있다. 결합 없이 조회하면 네 역할이 모두
미해결이고 `executable=false`다. 원문 bundle이나 DB 경로는 출력하지 않는다.
`--requirements`와 `--requirements-sha256`은 함께 지정한다. 파일은 1 MiB 이하의 BOM 없는
UTF-8 JSON이며 `schema_version`은 `aas-execution-requirements-v1`, `convention_bindings`는
`kind`·`id`·`version`·`hash`만 가진 pin 배열이다. 관례 본문을 인라인으로 넣을 수 없다.
`--requirements-sha256`은 이 파일 바이트의 해시이고, pin의 `hash`는 등록된 관례 문서의
정규화 전체 해시다. 두 값은 서로 다른 대상을 가리킨다. pin의 kind는 정의의 필수 역할에
속해야 하며 중복될 수 없다. 의미를 해석하는 kind는 `basis`뿐이고, 등록된 관례와 호환될
때만 가격 요구의 basis가 채워진다. 다른 역할의 pin은 무결성만 확인하며 미해결로 남고
`executable`은 계속 false다. 잘못된 해시, 미등록 pin, `capital` 요구에 대한 `total_return`
결합, 미해결 계보 버전은 종료 코드 1과 한 줄 오류로 끝난다.

관례 문서는 `aas data convention-import --spec FILE --sha256 SHA256`으로 등록한다. 반환된
`pin`의 `hash`가 위 pin 배열과 [준비 요청](#저장한-전략과-입력의-준비)에 들어갈 값이다.
Python에서는 `storage.input_pins.register_convention(workspace.state, raw,
expected_file_sha256=...)`과 `read_convention(workspace.state, pin)`이 같은 등록·조회를 맡는다.
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

## 원본 자료의 연구 입력 등록과 고정 조회

```bash
aas data register-prices --spec /path/to/prices-transform.json --sha256 SHA256
aas data register-sessions --spec /path/to/sessions-transform.json --sha256 SHA256
aas data register-proxy --spec /path/to/proxy-transform.json --sha256 SHA256
aas data read-prices --request /path/to/price-request.json --sha256 SHA256
```

원본 자료실의 테이블은 저절로 시장 generation이 되지 않는다. 세 등록 명령은 명시한 변환
문서를 읽어 `db source-import`로 보존한 원본 테이블에서 새 generation을 게시한다. 문서는
UTF-8 JSON이고 `--sha256`과 바이트가 다르면 거부한다. 스키마는 각각 `aas-price-transform-v1`,
`aas-sessions-transform-v1`, `aas-proxy-transform-v1`이며 필드 검증은
[research_inputs.py](../src/aegis_alpha/storage/research_inputs.py)가 소유한다.

세 문서의 공통 필드는 `source`(source_id·source_sha256·table·table_digest), `dataset`
(dataset_id·version·generation_id·operation_id·parent_id, 부모가 없으면 null), `columns`,
`instruments`, `publication_at_us`(모르면 null), `provider`, `normalizer_version`이다.
`columns`는 공통 revision 열(generation_id, record_id, revision_id, supersedes_revision_id,
op, available_at_us, revision_known_at_us, ingested_at_us, source_snapshot_id,
source_row_hash)과 도메인 열을 모두 서로 다른 원본 열에 연결해야 한다. 열이 빠지거나
알 수 없는 키가 있으면 거부한다. revision 연결, record 식별자, 원본 행 해시는 원본 열에서
읽는다. 오늘 날짜나 티커로 만들어 채우지 않는다. 변환 문서 원문은 `raw/` 아래에 해시로
보존되고 카탈로그의 `transform_sha256`이 그 문서를 가리킨다. 같은 문서를 다시 제출하면
같은 결과를 돌려준다. 게시된 generation의 수집 시각과 행 해시는 AAS가 새로 계산한 값이며
원본의 값은 원본 자료실과 변환 문서로 추적한다.

`register-prices`는 `price`(basis·currency·price_role), `calendar`(calendar_id·timezone·
timezone_version), `decimal_conversion`을 추가로 요구한다. `decimal_conversion`은
open·high·low·close·volume 각각이 원본에서 십진 문자열(`decimal_string`)인지 IEEE
실수(`ieee_float`)인지 선언한다. 두 경우 모두 DECIMAL(38,12)로 정확히 표현돼야 하며
자릿수 손실이나 범위 초과는 거부한다. 실수는 `Decimal.from_float`의 정확한 값으로 비교한다.
원본 행의 basis·currency·price_role은 문서와 같아야 하고, 부모 generation과 기준이 다르면
별도 dataset이다. OHLCV는 다섯 값이 모두 있고 `value_state`가 `present`이거나, 다섯 값이
모두 null인 행 단위 결측이어야 한다. 일부만 있는 행은 거부하며 종가로 나머지를 채우지 않는다.
`asset_type`이 `proxy`인 종목은 가격 테이블에 넣을 수 없다. 공급자의 조정 스냅샷은
`price_role=reference`로 등록하고, 실행용 OHLC와 신호·참조 가격은 서로 다른 pin을 가진다.

`register-sessions`는 `calendar`(calendar_id·venue·timezone·timezone_version)를 요구하고
`instruments`는 빈 배열이어야 한다. timezone은 IANA 이름이다. 각 세션 행은 calendar_id,
venue, session_date, open_at_us, close_at_us, status, timezone_version을 담는다. `open`
상태는 0 이상이고 순서가 맞는 UTC 마이크로초 두 값이, `closed` 상태는 둘 다 null이 필요하다.
세션 시각에서 공개 시각이나 수정 인지 시각을 추정하지 않는다.

`register-proxy`는 외부에서 정의한 프록시 지수 점을 `feature_values`에 저장한다. `proxy`에는
`proxy_id`, `version`, `normalization`, `transition`을 명시한다. `contract_id`는 제출한
proxy_id, `contract_version`은 그 버전이다. 모든 정의가 하나의 ID를 공유하지 않는다. 정의와
입력 pin은 `feature_contracts`·`feature_inputs`에 기록하며, 원본·열 연결·대상·정규화 버전이
바뀌면 새 proxy 버전이 필요하다. `transition`은 donor_id, target_id, logical_exposure_id,
switch_decision_date(YYYY-MM-DD), mode(`signal_only` 또는 `observed_instrument_switch`),
donor_source·target_source(SourcePin), basis_ref·calendar_ref·cost_ref(id·version·sha256)를
담는다. 참조 버전은 `latest`일 수 없다. `instruments`는 logical_exposure_id 하나이고
`asset_type`은 `proxy`다. `feature_values.value`는 DOUBLE이므로 `normalization`이 입력 숫자
형식과 `ieee754_binary64` 출력을 선언한다. 십진 문자열은 binary64로 반올림되고 원본 값은
원본 자료실에 남는다. 이 저장을 Decimal 정확 저장으로 표시하지 않는다. 결과는
`non_executable=true`다. 프록시 지수로 체결하지 않고, 보유 여부도 프록시나 요청 필드가 아니라
실제 이전 체결에서 정한다. 체결 재원은 실제 ETF OHLC뿐이다.

`read-prices`는 `aas-price-input-request-v1` 요청을 읽는다. `prices`에는 `pin`,
`sessions_pin`, `identity_pin`, `universe_pin`(없으면 명시적 null), `instrument_ids`,
`session_dates`, `currency`, `basis`, `price_role`, `calendar_id`, `venue`,
`timezone_version`, `interval`(`1d`), `mode`가 모두 필요하다. pin은 `data inspect`가 돌려주는
dataset_id·version·generation_id·chain_hash·manifest_hash다. `decision`은 `at_us`,
`session_date`, `ingestion_cutoff_us`(없으면 null)다. 기본값이나 최신 head 대체는 없다.
이 명령은 명시한 compute 환경(`AAS_HOST_CPU_LIMIT`, `AAS_HOST_MEMORY_LIMIT_BYTES`,
`AAS_COMPUTE_LOCK_FILE`)이 있어야 실행된다. 요청 파일과 전체 출력은 각각 1 MiB 안이어야 하며
넘치면 출력 없이 실패한다.

`mode=strict_pit`은 결정 시각까지 공개 시각과 수정 인지 시각이 모두 알려진 revision만 반영하고
참조 가격은 제외한다. 나중에 게시한 generation은 이전 pin의 결과를 바꾸지 않는다.
`mode=observed_snapshot_research`는 고정한 참조 스냅샷을 경제 날짜로만 제한하는 연구용
읽기다. `observed_snapshot_research` 사유가 붙고 인증되지 않으며 strict 읽기를 바꾸지 않는다.
두 모드 모두 `backtest_eligible=false`, `coverage.certified=false`다. `coverage.cells`는
요청한 종목과 날짜의 격자 전체를 기록하고 셀마다 `present`와 `reasons`(`missing_session`,
`missing_price`, `missing_sell_open`, `future_session`, `unknown_price_evidence`,
`reference_price` 등)를 남긴다. `identity_pin`·`universe_pin`이 null이면 `identity_unpinned`·
`universe_unpinned`가 남는다. 잘린 이력을 성공으로 돌려주지 않고, 요청 격자나 이력이 compute
메모리 예산을 넘으면 명시적으로 실패한다.

등록과 조회는 원본 자료의 진위, 공급자 조정 기준, 거래 가능성을 인증하지 않는다.
`data datasets`에 보이는 generation은 검증한 변환의 결과일 뿐이며 PIT 자격이나 백테스트
입력으로 자동 승격되지 않는다.

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

## 저장한 전략과 입력의 준비

```bash
aas data convention-import --spec /path/to/convention.json --sha256 SHA256
aas data binding-import --spec /path/to/pin-document.json --sha256 SHA256
aas prepare --request /path/to/prepare-request.json --sha256 SHA256 --output /path/to/new/envelope.json
aas backtest --input /path/to/new/envelope.json --sha256 <prepare가 돌려준 envelope.sha256>
aas db run-install [--backup-output /path/to/new-backup]
```

`aas prepare`는 등록된 전략과 고정한 입력만 읽어 각 의사결정 시점의 목표 비중을 계산하고,
기존 `aas backtest`가 읽는 봉투(`aas-etf-backtest-v1`/`v2`)로 내보낸다. 체결·NAV 계산,
run 등록, 결과 저장은 하지 않는다. 봉투 회계는 별도 `aas backtest` 호출이며, run·결과의
정식 저장·검증·복원은 아직 연결되지 않은 후속 작업이다. `db run-install`은 그 후속 소비자가
쓸 state·market 추가 스키마를 백업 후 설치할 뿐이고 `prepare`에는 필요 없다.

### 준비 요청 문서

요청은 `aas-prepare-request-v1` JSON이며 최상위 키는 `schema`, `hash_format`
(`aas-canonical-json-sha256-v1`), `strategy`, `bindings`, `refs`, `price_inputs`,
`macro_inputs`, `derived_inputs`, `proxy_rules`, `period`, `history`, `cutoff`,
`decision_latency_us`, `explicit_decision_dates`, `account`, `comparison`, `envelope`,
`metadata`다. 모든 키가 필수이고 알 수 없는 키·중복 키·bool을 숫자로 쓴 값은 거부한다.
기계용 JSON Schema는 [backtest_request.py](../src/aegis_alpha/engine/backtest_request.py)의
`PREPARE_REQUEST_SCHEMA`가 정본이다. 인라인 가격·전략·기본 관례는 받지 않는다.

- `strategy`: `strategy_store_id`(전략 DB의 `store_id`), `strategy_id`, `version`,
  `raw_sha256`, `contract_sha256`, 고정값 `schema=aas-engine-bundle-v1`,
  `contract_version=aas-engine-v1`, `raw_hash_format=aas-sha256-bytes-v1`,
  `contract_hash_format=aas-canonical-json-sha256-v1`. 세 해시·ID는 `strategy import`가
  돌려준 값과 정확히 같아야 하며 `latest`는 버전이 아니다.
- `bindings`: 역할별 참조 목록. 각 항목은 `role`, `ordinal`, `ref_kind`, `ref_id`,
  `ref_version`, `hash`, `ref_schema`, `hash_format`이다. 필수 역할은 `signal_prices`,
  `execution_prices`, `sessions`, `identity`, `universe`, `membership`, `calendar`, `basis`,
  `cost`, `execution`이고, `macro`·`derived`·`proxy`는 전략 정의가 요구할 때만, `benchmark`·
  `risk_free`·`fx`는 0 또는 1개다. 여러 개를 허용하는 역할은 `signal_prices`·`execution_prices`·
  `macro`·`derived`·`proxy`뿐이며 ordinal은 역할 안에서 0부터 연속이다.
- `refs`: bindings가 가리키는 참조 서술자. `ref_kind`, `ref_id`, `ref_version`, `hash`,
  `schema`, `hash_format`, `pin`을 담고 같은 서술자를 두 번 넣으면 거부한다.
  `generation`의 pin은 `data inspect`가 돌려주는 `dataset_id`·`version`·`generation_id`·
  `chain_hash`·`manifest_hash`, `identity`는 `snapshot_id`·`content_hash`, `universe`는
  `universe_id`·`version`·`content_hash`, `derived`·`membership`·`convention:<kind>`는
  `kind`·`id`·`version`·`hash`다. binding의 `hash`는 그 pin의 `chain_hash`, `content_hash`,
  또는 문서 전체 해시와 같아야 한다.
- `price_inputs`: `signal_prices`·`execution_prices` binding마다 하나씩. `binding`(`role`·
  `ordinal`), 정렬된 `instrument_ids`, `currency`, `basis`(`unadjusted`·`split_adjusted`·
  `total_return`), `price_role`(`canonical`·`reference`), `interval=1d`. 신호 가격은 basis
  관례와 맞아야 하고 체결 가격은 `unadjusted`·`canonical`이어야 한다.
- `macro_inputs`(`binding`·`series_id`·`unit`), `derived_inputs`(`binding`·`series_id`),
  `proxy_rules`(`binding`·`logical_exposure_id`): 전략 정의의 요구를 빠짐없이 채워야 하며
  요구하지 않은 입력을 조용히 무시하지 않는다.
- `period`(`start`·`end`)는 기준 종가일과 최종 평가 종가일, `history`(`start`·`end`)는
  신호·거시·파생 관측 구간이다. `history.start <= period.start < period.end`이고
  `history.end`는 모든 의사결정일을 덮어야 한다.
- `cutoff`: `mode`(`strict_pit` 또는 `observed_snapshot_research`), 필수 `knowledge_cutoff_us`,
  `ingestion_cutoff_us`(제한 없음은 null). `decision_latency_us`는 종가 이후 판단 지연이다.
- `explicit_decision_dates`는 null(자동 월말 판단) 또는 날짜 배열이다. 빈 배열은 판단 없음을 뜻한다.
- `account`: `currency`, 양수 `initial_cash`, `cashflows`. v1 봉투는 빈 배열, v2는
  날짜별 입출금 배열이다. `comparison`의 `benchmark`·`risk_free`·`fx`는 null이거나 해당
  관례 binding의 `{role, ordinal: 0}`이다. `envelope`는 `schema_version`과 `research_mode`
  (`synthetic` 또는 `observed_etf_research`), `metadata.created_at_us`는 null 허용이다.

세 해시는 서로 다른 대상이다. `--sha256`은 요청 파일 바이트 그대로의 SHA-256이다.
출력의 `request_hash`는 `metadata`를 뺀 정규화 의미 투영(`aas-backtest-request-v1`)의
해시이며 엔진 계산 소스 해시, Python·Decimal 컨텍스트 정체성, 관례 전체·payload 해시를
포함한다. 같은 내용에서 들여쓰기·키 순서·`created_at_us`를 바꿔도 같고, cutoff·pin·ordinal·
비용을 바꾸면 달라진다. `envelope.sha256`은 내보낸 봉투 바이트의 해시이고 `request_hash`에
들어가지 않는다.

### 등록 선행 조건

준비는 저장된 pin만 읽는다. 원본 파일은 등록 뒤 지워도 된다. 새 설치에서 필요한 등록은
다음과 같고, 어느 하나가 빠지면 `prepare`는 기본값이나 최신 head 대체 없이 거부한다.

1. `aas strategy import`: 전략 bundle. 반환된 `raw_sha256`·`contract_sha256`과
   `installation.json`의 전략 store ID가 `strategy`에 들어간다.
2. `aas db source-import`와 `aas data register-prices`/`register-sessions`: 신호 가격
   generation(예: `split_adjusted`·`reference`), 체결 가격 generation(`unadjusted`·`canonical`),
   세션 달력 generation. 각 `data inspect --dataset ID --version VERSION` 출력이 `generation`
   pin이다. `aas data import`로 게시한 typed generation도 같은 pin 형식이다.
3. `aas data binding-import`: `aas-identity-snapshot-v1`, `aas-universe-version-v1`,
   `aas-ensemble-membership-v1` 문서. 문서 스키마에 따라 identity snapshot, universe version,
   정의(derived·membership) 또는 `aas-input-bundle-v1` 묶음을 등록하고 `pin`을 돌려준다.
   identity·universe 문서의 정확한 바이트 규약은 [membership pins](design/membership-pins.md),
   derived·membership·bundle 문서는 [input_pins.py](../src/aegis_alpha/storage/input_pins.py)가
   소유한다. 이 명령은 `read-prices`와 같은 명시한 compute 환경이 필요하다.
4. `aas data convention-import`: `calendar`, `basis`, `cost`, `execution` 관례 문서 각각.
   `benchmark`·`risk_free`·`fx`는 요청에서 참조할 때만 등록한다. `calendar` payload의 여덟
   달력 필드는 전략 정의와 같아야 하고, `cost`는 `proportional_traded_notional`의 명시한
   `rate`, `execution`은 현재 루프(종가 판단·다음 시가 체결·소수 롱온리·현금 잔여) 문자 그대로다.
5. 조건부 입력: 정의가 거시 시계열을 요구하면 `data import`로 `macro_observations`
   generation을, 파생 시계열을 요구하면 `aas-derived-definition-v1` 문서를 `binding-import`로,
   프록시 노출을 쓰면 `data register-proxy`를 등록하고 각각 `macro`·`derived`·`proxy` binding으로
   묶는다. 프록시는 기존 전환 정의로만 실제 종목에 대응하며 여전히 체결 재원이 아니다.

등록 응답의 `backtest_eligible=false`는 그대로다. 등록은 원본의 진위·PIT 자격·거래 가능성을
인증하지 않는다.

### 실행 조건과 출력

`prepare`는 `read-prices`와 같은 compute 환경(`AAS_HOST_CPU_LIMIT`,
`AAS_HOST_MEMORY_LIMIT_BYTES`, `AAS_COMPUTE_LOCK_FILE`; 선택적으로 `AAS_CPU_LIMIT`·
`AAS_MEMORY_LIMIT_BYTES`)이 있어야 실행되며, 없으면 기본 예산 없이 종료 코드 1로 끝난다.
lock 파일이 설치의 저장 잠금과 같은 경로면 거부한다. 요청 파일은 1 MiB 이하의 일반 파일이어야
하고 symlink·FIFO·디렉터리·`..` 경로는 거부한다. `--output`과 `<output>.preparation.json`은
둘 다 새 파일이어야 하며 어떤 종류의 기존 경로도 덮어쓰지 않는다. 봉투는 64 MiB 안이다.

준비는 설치를 읽기 전용으로 열고 SELECT만 수행한다. 등록·run·bundle 기록을 남기지 않고
`engine.execution`을 호출하지 않으며, 같은 설치에서 같은 요청을 다시 준비하면 같은 봉투와
같은 `request_hash`를 낸다. 성공 응답은 `prepared=true`, `request_sha256`, `request_hash`,
`envelope.path`·`envelope.sha256`, `preparation.path`·`preparation.sha256`, `certified=false`다.
두 파일은 fsync 후 다시 읽어 바이트와 파일 정체성을 대조한 뒤에만 응답한다.

`<output>.preparation.json`은 `aas-prepared-backtest-v1` 문서로 `request_hash`, 의미 투영
전체(`request`), `envelope_sha256`, 실행 정의, 읽은 관례 문서, 실제로 읽은 저장 입력의
증거(`stored_inputs`), `source_pins`, 판단·체결 슬롯, 각 판단의 replay 영수증, feature 값,
`certified=false`를 담는다. 봉투 자체에는 이 출처가 들어가지 않는다.

실패는 stdout 없이 stderr 한 줄 `{"error": ...}`와 종료 코드 1이다. 봉투를 쓴 뒤 sidecar나
fsync 단계에서 실패하면 이미 쓴 파일이 검사용으로 남는다. 그 파일은 성공 영수증이 아니며
재시도는 같은 경로를 덮어쓰지 않으므로 새 경로를 쓰거나 직접 정리한다.

### 시점 규칙

의사결정일은 세션 종가의 경제 날짜이고, 지식 시점은 별도로 정한다. 각 슬롯의 cutoff는
`min(knowledge_cutoff_us, 종가 시각 + decision_latency_us)`이며 종가 이후, 다음 시가 이전이어야
한다. 신호·거시·파생 관측은 그 cutoff까지 공개·수정 인지된 revision만 반영하고 stale 검사도
cutoff의 UTC 날짜를 기준으로 한다. `ingestion_cutoff_us`가 있으면 그 시각까지 AAS가 수집한
revision만 재생한다. 나중에 게시한 generation·revision은 같은 pin의 과거 판단을 바꾸지 않는다.
`strict_pit`은 참조 가격과 인지 시각이 없는 행을 제외하고, `observed_snapshot_research`는
고정 스냅샷을 경제 날짜로만 제한하는 미인증 연구 모드다. 두 모드 모두 `certified=false`다.

체결 결과 가격은 `period` 안의 모든 open 세션에서 따로 읽는다. 판단일의 다음 시가가 봉투의
날짜 격자와 다르면(나중에 안 달력 수정이 그 세션을 없애거나 더 이른 시가를 넣은 경우)
`incompatible outcome calendar projection`으로 내보내기 전에 거부한다. 체결·격자를 조용히
옮기지 않는다. 매수 종목의 시가가 없거나 이력 버킷이 부족하거나 pin된 달력이 불완전해도
거부한다. 매도만 남은 종목의 시가 누락은 준비가 아니라 `aas backtest` 회계에서 거부한다.

### 봉투 회계와 Python API

`aas backtest --input ENVELOPE --sha256 <envelope.sha256>`는 새 프로세스에서 봉투를 읽어
NAV·체결을 계산한다. 응답의 `source_pins_verified`·`point_in_time_verified`·`live_orders`는
계속 false다. 회계 결과는 파일로만 남고 run 등록·결과 확정·복원은 아직 없다.

같은 준비를 Python에서 호출할 수 있다.
[backtest_prepare.py](../src/aegis_alpha/application/backtest_prepare.py)의
`parse_prepare_request(raw: bytes) -> ParsedPrepareRequest`, `PrepareRequest(parsed)`,
`prepare_backtest(workspace, request, *, budget: ComputeBudget) -> PreparedBacktest`가
공개 진입점이다. 호출자가 `storage.workspace.open_workspace(home)`으로 읽기 전용 설치와
`compute_resources.ComputeBudget`을 소유하며, CLI가 잡는 compute lease는 여기서 자동으로
잡히지 않는다. `PreparedBacktest`는 `request`, `definition`, `slots`, `decisions`, `features`,
`inputs`, `projection`, `envelope`(`canonical_bytes`·`envelope_sha256`), `provenance`(sidecar
바이트), `targets`, `request_hash`, `certified=False`를 가진다. 봉투 바이트는
`application.backtest_cli.run_document(canonical_bytes, envelope_sha256)`로 회계에 넘긴다.
`calculation_identity()`·`environment_identity()`는 `request_hash`에 들어가는 엔진·환경
정체성이고, `engine.backtest_request`의 `request_projection`·`export_envelope`는 저장소 없이
순수 투영·내보내기만 맡는다.

### 저장소 checkout 실습

아래는 설치된 앱의 사용 절차가 아니라 저장소 checkout에서 공개 합성 fixture로 전체 흐름을
확인하는 실습이다. `uv sync --locked --dev`로 준비한 환경이 필요하며, 이 환경에는 `legacy`
추가 의존성이 함께 들어 있다. `tests.application.test_prepare_cli.register_fixture`는 테스트
helper이지 설치되는 공개 API가 아니다. helper는 seed 설치에서 합성 문서를 뽑은 뒤 시험
대상 home에 대해 `init`, `strategy import`, `db source-import` 3회, `data register-prices` 2회,
`data register-sessions`, `data inspect` 3회, `data binding-import` 3회, `data convention-import`
4회를 실제 CLI로 실행하고 `request.json`을 쓴 뒤 `incoming/` 원본 파일을 지운다.

```bash
LAB="$(mktemp -d /var/tmp/aas-prepare-lab-XXXXXX)"
export AAS_HOST_CPU_LIMIT=1 AAS_HOST_MEMORY_LIMIT_BYTES=1073741824 \
  AAS_COMPUTE_LOCK_FILE="$LAB/compute.lock"

# 1. 합성 fixture를 실제 CLI 명령으로 등록한다 (테스트 helper, 설치된 공개 API가 아니다).
uv run --no-sync python - "$LAB" <<'EOF'
import json, subprocess, sys
from pathlib import Path
from tests.application.test_prepare_cli import register_fixture

root = Path(sys.argv[1])
home = root / "home"

def cli(*args):
    print("$ aas --home", home, *args, file=sys.stderr)
    result = subprocess.run(
        [sys.executable, "-m", "aegis_alpha", "--home", str(home), *args],
        text=True, capture_output=True, check=False, timeout=120,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr)
    return json.loads(result.stdout)

home, request = register_fixture(root, cli)
print(json.dumps({"home": str(home), "request": str(request)}))
EOF
ls "$LAB"

# 2. 원본 파일 없이 저장된 pin만으로 준비한다.
uv run --no-sync aas --home "$LAB/home" prepare --request "$LAB/request.json" \
  --sha256 "$(sha256sum "$LAB/request.json" | cut -d' ' -f1)" \
  --output "$LAB/envelope.json" | tee "$LAB/prepare-receipt.json"

# 3. 반환된 봉투 해시로 새 CLI 프로세스에서 회계를 실행한다.
ENVELOPE_SHA256="$(uv run --no-sync python -c \
  'import json,sys;print(json.load(open(sys.argv[1]))["envelope"]["sha256"])' "$LAB/prepare-receipt.json")"
uv run --no-sync aas backtest --input "$LAB/envelope.json" --sha256 "$ENVELOPE_SHA256" \
  | tee "$LAB/backtest.json"

# 4. 필수 pin이 빠진 요청은 기본값 없이 거부된다.
uv run --no-sync python - "$LAB" <<'EOF'
import json, sys
from pathlib import Path
from aegis_alpha.data.serialization import canonical_json_bytes

root = Path(sys.argv[1])
body = json.loads((root / "request.json").read_bytes())
body["bindings"] = [row for row in body["bindings"] if row["role"] != "membership"]
body["refs"] = [row for row in body["refs"] if row["ref_kind"] != "membership"]
(root / "missing-membership.json").write_bytes(canonical_json_bytes(body))
EOF
set +e
uv run --no-sync aas --home "$LAB/home" prepare --request "$LAB/missing-membership.json" \
  --sha256 "$(sha256sum "$LAB/missing-membership.json" | cut -d' ' -f1)" \
  --output "$LAB/missing.json"
echo "exit=$?"
set -e
ls "$LAB"

# 5. 같은 요청을 Python API로 준비한다.
uv run --no-sync python - "$LAB" <<'EOF'
import hashlib, json, sys
from fractions import Fraction
from pathlib import Path
from aegis_alpha.application.backtest_prepare import (
    PrepareRequest, parse_prepare_request, prepare_backtest)
from aegis_alpha.compute_resources import ComputeBudget
from aegis_alpha.storage.workspace import open_workspace

root = Path(sys.argv[1])
request = PrepareRequest(parse_prepare_request((root / "request.json").read_bytes()))
with open_workspace(root / "home") as workspace:
    prepared = prepare_backtest(
        workspace, request, budget=ComputeBudget(Fraction(1), 512 * 1024 * 1024))
print(json.dumps({
    "request_hash": prepared.request_hash,
    "envelope_sha256": prepared.envelope.envelope_sha256,
    "provenance_sha256": hashlib.sha256(prepared.provenance).hexdigest(),
    "slots": [[s.decision_date.isoformat(), s.execution_date.isoformat()] for s in prepared.slots],
    "targets": {day.isoformat(): dict(rows) for day, rows in prepared.targets.items()},
    "certified": prepared.certified,
}, indent=1))
EOF
rm -rf "$LAB"
```

한 실행에서 관측한 값이다. store ID·등록 시각·원본 SQLite 바이트가 실행마다 달라지므로
해시는 예시이고, 날짜·비중·NAV·체결은 fixture가 고정한 독립 기대값이다.

- 2단계 영수증: `request_sha256=40ce893b…def9`, `request_hash=3f2b4821…564a`,
  `envelope.sha256=a30a6dd4…be4f`, `preparation.sha256=b20b060e…98a3`, `certified=false`.
  `$LAB/incoming/`은 이미 없다.
- 3단계 회계: NAV 날짜 `2026-01-29, 02-02, 02-26, 03-02, 03-30`에 equity
  `100, 100, 80, 80, 80`, 체결은 `01-29→02-02 ASSET_A 100/15주 @15`,
  `02-26→03-02 ASSET_A -100/15주 @12`, `02-26→03-02 ASSET_B 4주 @20`, 수수료 0,
  `source_pins_verified=false`, `point_in_time_verified=false`, `live_orders=false`.
  독립 계산: 1월 29일 수익률 ASSET_A 0.5 > ASSET_B 0.1이라 A 전량, 2월 2일 시가 15에
  100/15주, 2월 26일 종가 12로 80, 3월 2일 A 매도 후 B를 시가 20에 4주, 3월 30일 종가 20으로 80.
- 4단계: stderr `{"error": "missing required executable bindings"}`, `exit=1`,
  `missing.json`과 그 sidecar는 만들어지지 않는다.
- 5단계: 같은 `request_hash`·`envelope_sha256`·`provenance_sha256`, 슬롯
  `[2026-01-29, 2026-02-02]`·`[2026-02-26, 2026-03-02]`, 목표 `{2026-01-29: {ASSET_A: 1.0},
  2026-02-26: {ASSET_B: 1.0}}`, `certified=false`.

이 실습은 합성 자료의 흐름 확인이며 실제 자료의 출처·PIT 자격·전략 성과를 인증하지 않는다.

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

비교 대상을 늘릴 때는 캐시가 없는 다음 기간에도 필요한 발행사 문서를 갱신할 수 있도록
유한한 요청 한도를 함께 점검한다. 한도가 부족한 대상은 자료 부족으로 남겨야 한다.
완료한 기간의 추가 조사는 별도 결과로 보존하고, 정기 실행 설정은 다음 기간부터 적용한다.
정기 실행이 정상 종료돼도 고정된 시장 자료의 비교 기간이 갱신되는 것은 아니다.
발행사 문서의 기준일과 시장 자료의 종료일을 각각 확인하고, 만료된 입력은 새로 검증한다.

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
`storage.source_library.import_arrow`의 Arrow 적재도 같은 추가 의존성의 PyArrow를 쓴다.
위 준비 실습은 그 의존성이 들어 있는 잠긴 개발 환경에서 확인했으며, 기본 설치만으로
같은 흐름이 도는지는 따로 검증하지 않았다. 의존성 목록은 `pyproject.toml`과 `uv.lock`이 정본이다.
기존 DB 명령은 `aas legacy-db`, publication 조회는 `aas legacy-data`로 구분한다.
이 도구를 새 `state.sqlite3`에 연결하거나 실제 보관 데이터를 자동 채택하지 않는다.
`docker-compose.data.yml`과 이전 설치 실행기는 이 전환 경로이며 새 설치 절차가 아니다.

라이브 공급자 검증·기존 DB 이전·스케줄러 활성화·실주문은 위 오프라인 설치 검사의 범위에
포함되지 않는다. CLI preview는 합성 비중 계산이고, `prepare`와 `backtest`는 저장한 입력의
준비와 명시한 봉투의 회계까지다. run 저장·결과 확정·복원은 아직 별도 구현이다.
