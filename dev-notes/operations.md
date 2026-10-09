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
aas strategy promote --source SOURCE_ID --sha256 SHA256 --plan
aas strategy promote --source SOURCE_ID --sha256 SHA256 --apply
aas strategy definitions [--id ID]
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

`strategy promote`는 `db source-import`로 보존한 전략 원본 레코드(`strategy`·`asset_dependency`·
`macro_dependency` 테이블)를 [정의 문서](design/data-vertical.md#전략-레지스트리)로 등록한다.
`--sha256`은 그 원천을 적재할 때의 SHA-256이다. `--plan`은 아무것도 쓰지 않고 전략 수, 새 전략·새
버전·재사용 버전 수, 요구 행의 도메인·사상 상태별 수, dataset ID마다 요구 수·전략 수와 state
카탈로그의 등록 여부(`in_catalog`)·committed 버전 수, 사상되지 않은 토큰과 이유, 원천 의존 테이블과
요청이 어긋난 전략(`dependency_mismatches`)을 보고한다. `--apply`는 어긋난 전략이 하나라도 있으면
거부하고, 같은 원천으로 다시 실행하면 `reused=true`로 끝나며 바뀌는 것이 없다. 내용이 바뀐 레코드는
새 버전이 되고 이전 버전은 그대로 남는다. 정의는 실행 bundle이 아니므로 `strategy list`에 나오지 않고
`strategy definitions`로 조회하며 모든 행이 `execution_eligible=false`다. 원천 테이블은 계산 예산
(설정한 AAS compute 환경, 없으면 512 MiB 직렬 기본값)에서 세 테이블을 누적해 승인한다. 등록 작업이 PREPARED로 남으면
`aas db recover`가 저장된 정의를 다시 도출해 완료하고 원천을 다시 읽지 않는다. 비공개 marker 없이
남은 작업은 같은 `--apply`를 다시 실행해 끝내며, `aas db quarantine`은 등록 작업을 격리하지 않는다.

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
aas db source-link --plan
aas db source-link --apply
```

`source-import`는 원본 SQLite 테이블을 비공개 원본 자료실에 보존한다. 먼저 읽기 전용
연결의 SQLite backup API로 일관된 사본을 만들고, 닫힌 사본의 해시를 지정한다.
원본의 실행 코드·뷰·트리거를 실행하거나 기존 엔진 bundle로 추정 변환하지 않는다.
원본 설정, 연구용 설정, 원래 성과는 각각 원래 테이블과 열의 의미를 유지한다.
`strategy list`는 검증된 실행 bundle 목록이며 `db sources`의 원본 자료 목록과 구분된다.
`source-link`는 원본 자료실 commit마다 `sl:` 원천 snapshot과 원본 파일 행을 state에 남긴다.
`--plan`은 연결할 commit 수, 이미 연결된 수, 원본 bytes가 `raw/`에 없어 연결하지 못하는
`unbacked`, `raw/`의 bytes가 pin과 달라 연결하지 못하는 `corrupt`, 완료되지 않은 `incomplete`,
기록끼리 맞지 않는 `invalid`를 commit ID와 함께 보고하고 아무것도 쓰지 않는다. 한 commit이
연결되지 못해도 나머지는 계속 처리한다. `--apply`는 빠진 연결만 기록하며 다시 실행하면 바뀌는
것이 없다. 원본 bytes는 두 모드 모두 `raw/`에서 다시 해시한다. `corrupt` commit은 원본을 백업에서
되살린 뒤 다시 실행한다. 내용 ID commit이 적재 도중 멈춰 연결 없이 남으면 `aas db verify`가 실패하고
`aas db recover`가 그 연결을 기록해 `linked_sources`로 보고하고, 연결할 수 없는 commit은 `invalid_sources`에 남긴 채 나머지를 계속 연결한다.

대량 분석 자료는 `storage.source_library.import_content_arrow`로 명시적인 Arrow reader에서
DuckDB에 적재한다. 원천 ID는 `raw/`에 먼저 보존한 원본 파일의 내용에서 나오고 적재 코드의 해시는
`lineage`로만 기록되므로, 코드만 바꿔 같은 원본을 다시 적재하면 기존 원천을 재사용한다. commit 하나는
경계가 원본 bytes로 정해지는 완결 단위 하나(예: 수집 job 하나의 `complete.json`과 그것이 나열한
파일)다. 여러 단위를 적재 코드의 batch 크기로 묶어 commit하면 batch가 바뀔 때마다 새 원천이 생긴다.
`import_arrow`는 내용 ID 이전에 만든 명시 ID를 그대로 쓰는 경로다. 같은 스키마끼리 묶고 파일 경로와 원본 행 번호를 보존한다.
원본 시각이나 숫자의 정밀도를 임의로 줄이지 않는다. 원본 자료실 등록은 PIT 사용 자격이나
백테스트 실행 성공을 뜻하지 않으며, `data datasets`의 게시된 데이터 버전에 자동 추가되지 않는다.
대량 이전은 원본 파일·행 번호를 유지한 여러 source로 나눠 적재할 수 있고, 나누는 경계는 위의
완결 단위다. 행 수만으로 메모리 사용량을 판단하지 않으며, 실제 사용량과 처리 속도에 맞춘 작업
크기 조정은 한 commit 안의 reader batch로 한다.
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
aas data register-observations --spec /path/to/observation-transform.json --sha256 SHA256
aas data read-prices --request /path/to/price-request.json --sha256 SHA256
```

원본 자료실의 테이블은 저절로 시장 generation이 되지 않는다. 네 등록 명령은 명시한 변환
문서를 읽어 `db source-import`로 보존한 원본 테이블에서 새 generation을 게시한다. 문서는
UTF-8 JSON이고 `--sha256`과 바이트가 다르면 거부한다. 스키마는 각각 `aas-price-transform-v1`,
`aas-sessions-transform-v1`, `aas-proxy-transform-v1`, `aas-observation-transform-v1`이며 필드 검증은
[research_inputs.py](../src/aegis_alpha/storage/research_inputs.py)가 소유한다.

네 문서의 공통 필드는 `source`(source_id·source_sha256·table·table_digest), `dataset`
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

`register-observations`는 관측한 시가 또는 종가만 있고 고가·저가·거래량이 없는 보존 연구
패널을 `feature_values`에 저장한다. 실행용 OHLC가 아니므로 `prices`로 가지 않으며,
DECIMAL(38,12) 정확 승인과 부분 OHLCV 거부는 그대로다. `observation`에는 `series_id`,
`version`, `observation_role`(`open` 또는 `close`), `basis`, `currency`,
`price_role`(항상 `reference`), `adjustment`, `value_domain`(`positive` 또는 `real`),
`certified`(항상 `false`), `normalization`(입력 숫자 형식과 `ieee754_binary64` 출력),
`observed_source`(상위 패널을 가리키는 SourcePin), `calendar_ref`(id·version·sha256)를
명시한다. `contract_id`는 `series_id/observation_role`이므로 한 series의 시가와 종가는
서로 다른 행으로 남고 결측도 따로 갖는다. 열 연결은 `register-proxy`와 같은
`feature_values` 열을 쓰고, `value_state`는 `present` 또는 `missing`만 허용한다.
`ieee_float`는 비트를 그대로 보존하고 `decimal_string`은 binary64로 반올림되므로 원본
값은 원본 자료실에 남는다. `asset_type`이 `proxy`인 종목은 이 경로에 넣을 수 없다.
`publication_at_us`를 모르면 null로 두며 거래일로 채우지 않는다. 패널은 여러 개의 제한된
generation으로 나눠 등록할 수 있고, 확장은 `parent_id`로 같은 contract를 이어야 하며 조상의
변환까지 같은 정의여야 한다. 결과는 `non_executable=true`, `certified=false`다. 이
계약은 언제나 참조 자료이므로 strict PIT는 인지 시각이 모두 알려져 있어도 한 행도 고르지
않는다. 읽기는 `market_inputs.load_pinned_observations`이며, 파생 series 입력으로는 받지
않는다. 연구 수익률 프록시와는 계속 다른 계약이고 체결 재원은 여전히 실제 ETF OHLC뿐이다.

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
`data datasets`는 committed dataset version(`datasets`), [dataset 카탈로그](design/data-vertical.md#dataset-카탈로그)
항목마다 도메인·역할·매퍼·규칙·flag·동결 여부와 그 항목에 게시된 dataset의 version 수와 head(`catalog`),
카탈로그 밖 이름으로 게시된 dataset(`uncataloged`)을 돌려준다. 거기 보이는 generation은 검증한 변환의
결과일 뿐이며 PIT 자격이나 백테스트 입력으로 자동 승격되지 않는다.

## identity 등록

```bash
aas identity register --file registry.json --sha256 SHA256 --plan
aas identity register --file registry.json --sha256 SHA256
aas identity snapshot --id SNAPSHOT_ID [--provider P] [--namespace N] [--plan]
aas identity show --instrument INSTRUMENT_ID | --anchor NAMESPACE TOKEN
aas identity show --key PROVIDER NAMESPACE TOKEN | --snapshot SNAPSHOT_ID
```

`register`는 `aas-identity-registry-v1` 문서를 파일 SHA-256과 함께 받아 issuer·instrument·assertion을
state에 덧붙인다. `--plan`은 아무것도 쓰지 않고 새 행·기존 행 수, 충돌, 누락 참조(원천 snapshot,
issuer, instrument, 정정 대상)를 보고한다. 충돌이나 누락이 하나라도 있으면 적용은 문서 전체를
거부하며, 같은 문서를 다시 적용하면 바뀌는 것이 없다. assertion이 가리키는 `sl:` 원천은 먼저
`aas db source-link --apply`로 연결한다. `snapshot`은 선택한 assertion을 chunked identity 문서로
등록하고 manifest pin(`snapshot_id`, `content_hash`)과 part 목록을 돌려준다. 이 pin이 실행 입력
bundle의 identity binding이 된다. `show`는 읽기 전용이다. 문서 형식, ID 발급 규칙, 충돌 정의는
[데이터 수직 계약](design/data-vertical.md#identity-등록과-chunked-문서)이 소유한다.

### KR identity

```bash
aas identity kr-import --kind-receipt KIND_DIR/response.json --eodhd-job JOB_DIR [--plan]
aas identity kr-build --eodhd SOURCE_ID [--eodhd SOURCE_ID] [--kind SOURCE_ID] \
  [--dart SOURCE_ID] ... --output registry.json [--report report.json]
aas identity register --file registry.json --sha256 SHA256 --plan
```

`kr-import`는 수집해 둔 KIND 상장법인목록 receipt와 EODHD 거래소 종목 목록 job 디렉터리를 읽어 원본
bytes를 `raw/`에 두고 내용 원천(`kind-listings-<hex>`, `qveris-eodhd-exchange-symbols-<hex>`)으로
commit한다. 두 옵션 모두 반복할 수 있고, 같은 수집물을 다시 넣으면 원천을 재사용한다. `--plan`은 원천 ID,
행 수, commit 여부만 보고하고 쓰지 않는다. commit에는 `pyarrow`(legacy extra)가 필요하며, 없으면 공급자를 부르기 전에 실패한다. 공급자를
호출하지 않는다.

`kr-build`는 설치본을 읽기 전용으로 열어 지정한 원천을 pin과 대조해 읽고, `eodhd.kr_symbol@1`,
`kind.listings@1`, `dart.corp_codes@1` 매퍼로 `aas-identity-registry-v1` 문서를 `--output`에 쓴다.
`--dart`는 DART `corp_codes` receipt 한 행을 담은 원천 자료실 `receipts` 테이블의 원천 ID다. 세
옵션 모두 반복할 수 있다. 입력은 누적이다. 이미 등록된 KR assertion이 인용하는 원천을 모두 다시 넣어야
하며, 빠진 원천이 있으면 그 ID를 보고하고 아무것도 쓰지 않는다. 새로 수집한 목록은 그 원천들에 더해
넣는다. 출력과 `--report`는 둘 다 없는 경로여야 하고, 두 파일을 온전히 쓴 뒤 함께 만들며 실패하면
아무것도 남기지 않는다. 응답은 문서 SHA-256, issuer·instrument·assertion 수, 매퍼별 행 보고, 미해결 key의
이유별 수와 표본, 그리고 등록돼 있지만 이번 원천이 더는 내지 않는 assertion(`withdrawn`)이다.
`withdrawn`은 대체 assertion으로 정정할 대상이며 빌더가 닫지 않는다. 등록은 그 파일과 SHA-256으로 위
`register`를 실행한다. `sl:` 원천 연결이 없으면 `register --plan`이 누락 원천으로 보고하므로 먼저
`aas db source-link --apply`로 연결한다. 해석 규칙과 미해결 이유는
[KR 등록](design/data-vertical.md#kr-등록)이 소유한다.

`scripts/kr_identity_report.py`(로직은 `application/kr_identity_report.py`)는 state 파일이 없는 store도 읽도록 market 파일만 읽기 전용으로 열고,
수집물 파일에서 `kr-build`와 같은 bytes의 문서를 메모리에서 만들어, 일봉 원천(`--symbols-prefix`,
`--table`)의 EODHD 심볼이 몇 개 해석되는지와 미해결 심볼을 이유별로 보고한다. 설치본에 쓰지 않는 검토
근거다.

### US identity

```bash
aas identity us-build --master SOURCE_ID [--fmp SOURCE_ID] ... [--sec SOURCE_ID] ... \
  [--norgate-exports] [--bindings SOURCE_ID] --output registry.json [--report report.json]
aas identity register --file registry.json --sha256 SHA256 --plan
```

`us-build`는 설치본을 읽기 전용으로 열어 지정한 원천을 pin과 대조해 읽고, `norgate.master@1`,
`eodhd.us_symbol@1`, `fmp.profile@1`, `sec.tickers@1` 매퍼로 `aas-identity-registry-v1` 문서를 `--output`에
쓴다. `--master`는 Norgate security master 원천 하나, `--fmp`는 FMP company profile 원천, `--sec`는
`aas import legacy`의 `sec.submissions_zip@1`로 편입한 SEC submissions 내용 원천이다(`raw/`의 archive를
읽는다). `--fmp`와 `--sec`는 반복할 수 있다. `--norgate-exports`는 `aas import legacy`의
`norgate.history_export@1`로 편입한 `norgate-history-csv-*` 원천을 모두 `norgate.export_listing@1`로 읽어,
master에 없는 시리즈를 발급하고 master의 마지막 세션 뒤 내보내기 창의 티커 주장을 더한다. 원천마다 `sl:` 연결이 있어야 하므로 명시 ID 원천은 먼저
`aas db source-link --apply`로 연결한다. `--bindings`는 legacy identity bindings 원천과 발급한 asset ID
집합을 비교해 보고에 싣는다. 입력의 누적 규칙, 출력·`--report` 파일 규칙, 응답 형태(`withdrawn` 포함)는
`kr-build`와 같다. 응답에는 asset ID 집합의 `aas-norgate-assetids-v1` 해시(`assetids_sha256`)와 issuer가
연결된 instrument 수, 티커 주장이 끝나는 master의 마지막 관측 세션(`through`), 내보내기의 마지막 주식
세션(`export_through`)과 창의 EODHD 심볼 수(`export_symbols`)가 더 실린다. 공급자를
호출하지 않는다. 해석 규칙과 미해결 이유는 [US 등록](design/data-vertical.md#us-등록)이 소유한다.

```bash
uv run --no-sync python -m scripts.us_identity_report --market MARKET.duckdb \
  --master SOURCE_ID --bars qveris-bulk- --quarantine qveris-bulk- \
  [--quarantine-reason REASON] [--output report.json]
```

`scripts/us_identity_report.py`(로직은 `application/us_identity_report.py`)는 market 파일만 읽기 전용으로
열고 `--master`의 Norgate master로 US 문서를 메모리에서 만들어, `--bars` 접두사 아래 `bars` 테이블과
`--quarantine` 접두사 아래 `quarantine` 테이블(`source_row_json`의 `code`, `date`, `exchange_short_name`)의
`.US` 행을 `eodhd.bars@1`처럼 세션 날짜의 New York 0시에 해석한다. 표 종류마다 모든 행과 고유
(심볼, 날짜) 키의 해석 수와 미해결 행·심볼 수를 이유별로 보고한다. EODHD 심볼 주장은 master에만 기대므로
해석은 같은 master로 만든 `us-build` 문서와 같다. 문서 자체는 state의 `sl:` 연결 시각이 필요하므로 싣지
않는다. 설치본에 쓰지 않는 검토 근거다.

### universe

```bash
aas universe index --source SOURCE_ID [--source SOURCE_ID] ... [--index NAME] ... \
  --version VERSION [--report report.json] [--plan]
aas universe listings --master SOURCE_ID --version VERSION [--report report.json] [--plan]
aas universe show --id UNIVERSE_ID --version VERSION
```

`index`는 `aas import legacy`의 `norgate.index_membership@1` 항목이 commit한 지수 구성 원천을, `listings`는
Norgate security master 원천을 pin과 대조해 읽고 universe 문서를 만든다. 원천마다 `sl:` 연결이 있어야 하고
member instrument는 US identity 등록이 먼저 있어야 한다. `index`는 지수마다 universe
`index.us.norgate/<지수 이름>` 하나를 같은 `--version`으로 등록하고, `--index`는 등록할 지수를 고른다(모든
쌍은 여전히 읽고 검사한다). 지수 구성 원천은 legacy 편입 단위마다 하나이므로 한 번에 모두 넘긴다.
`--plan`은 설치본을 읽기 전용으로 열어 pin과 part 수만 보고한다. 한 명령이 만든 universe는 한 transaction에서
모두 등록되거나 하나도 등록되지 않는다. 응답은 매퍼 보고, universe별 member 수, 미해결 asset ID 표본(100개),
날짜별 member 수 요약, pin이고, `--report`는 미해결 전체와 pin을 담은 같은 보고를 새 파일에 쓴다. 이미 있거나
폴더가 없는 `--report` 경로는 아무것도 등록하기 전에 거부된다. 등록은 state만 쓰고 strategy 저장소를 쓰지
않는다. `show`는 등록된 universe의 header와 part별 member 수를 읽는다. 공급자를 호출하지 않는다.
규칙은 [universe 등록](design/data-vertical.md#universe-등록)이 소유한다.

## 원천 자료의 승격과 은퇴

원천 자료실 자료를 공급자별 시장 dataset으로 승격하는 명령(`aas data promote`), 원천 ID 연결
(`aas db source-link`), core schema 업그레이드(`aas db migrate`), 원천 은퇴(`aas db source-retire`)와
compact(`aas db compact`)의 계약은 [데이터 수직 계약](design/data-vertical.md)이 소유한다. 현재 CLI에는
`aas db migrate`, `aas data promote`·`promotions`·`kr-prices`, `aas calendar refresh`, `aas import legacy`·`sec-companies`,
`aas db source-retire`·`compact`와 위 [원본 자료 이전과 조회](#원본-자료-이전과-조회)의 `source-link`가 있다.
승격된 dataset을 읽는 소비자 경로(`read_heads`)가 연결되기 전까지 원천 자료의 연구 입력은 아래
`register-*` 경로가 맡는다.

```bash
aas db migrate --to 3 --plan
aas db migrate --to 3 --backup-output /path/to/other-device/new-backup
aas data promote --spec /path/to/promotion.json --sha256 SHA256 --plan
aas data promote --spec /path/to/promotion.json --sha256 SHA256
aas data promotions
aas db source-retire --spec /path/to/retirement.json --sha256 SHA256 [--backup /path/to/other-device/backup] --plan
aas db source-retire --spec /path/to/retirement.json --sha256 SHA256 --backup /path/to/other-device/backup --apply
aas db compact --to /path/to/new-root
```

`db migrate --plan`은 compute lease 안에서 state·market을 읽기 전용으로 열어 각 저장소의 버전, 인식한 `schema_migrations` checksum,
다음 단계의 남은 부분(`backup`, `intent`, `market`, `state`, `receipt`, `complete`), 목표까지의 단계별 목록
(`migrations`), 백업 필요 여부, 가지고 갈 승격(`carried_operations`)과 막는 작업(`blocking_operations`)을
보고하고 아무것도 쓰지 않는다. 실행은 한 버전씩 올리며, 단계마다 실행 중인 run이나 PREPARED 작업이
없을 때만 시작해 검증된 백업을 만든 뒤 그 단계의 intent를 기록하고 market과 state를 각각 한
트랜잭션으로 올린 다음 설치 영수증을 바꾸고 intent를 완료한다. 백업이 필요한 단계가 여럿이면
`--backup-output`은 새 디렉터리여야 하고 그 아래 단계의 operation ID마다 백업이 생긴다. 목적지는 어느
단계도 쓰기 전에 백업과 같은 조건(새 디렉터리, `raw`·`runs`·`secrets`와 Git checkout 밖)으로 확인한다. 응답은 두
저장소의 버전과 영수증 행, 이번에 끝낸 단계마다 백업 경로와 manifest SHA-256, 남은 PREPARED 작업을
담는다. 목표 이상인 설치본에는 아무것도 하지 않는다. 중간에 멈춘 설치본은 다른 명령으로 열리지 않으며,
같은 명령을 다시 실행하면 그 단계의 백업 없이 남은 부분부터 끝낸다. 한 단계를 끝내고 다음 단계의 백업
도중 멈췄으면 다음 단계는 새 `--backup-output`을 요구한다. `aas db recover`와 `aas db quarantine`은 이
작업을 다루지 않는다. COMMIT 전에 멈춘 승격 intent 가운데 `aas db recover`가 게시할 것(marker·행·flag·
catalog가 없고, 보존 증거가 intent의 것이며, 보존 명세를 다시 계획하면 intent의 manifest가 그대로 나오는
것)만 PREPARED인 채로 단계를 지나고, 그 단계의 백업에 그대로 담긴다. `--plan`은 그런 승격마다 다시 계획해
증명했다고 적는다. 그 재계획이 compute lease 안에서 돌 수 없으면 `ComputeResourceError`로 멈춘다. 그 백업은 migration의
rollback snapshot이며 복구를 마친 백업으로 쓰지 않는다. migration 뒤 `aas db recover`가 그 승격을
게시한다. 규칙은 [데이터 수직 계약의 core schema 버전](design/data-vertical.md#core-schema-버전)이 소유한다.

실제 설치본에서 이 명령들을 실행하는 순서와 정확한 명령은 [운영 전환](#운영-전환)이 정한다.

`promote --plan`은 설치본을 읽기 전용으로 열어 명세가 요청하는 승격을 끝까지 계산하고 아무것도
쓰지 않는다. 응답은 원천 행 수, 매핑 행 수, 매퍼가 고르지 않은 행 수, 응답을 담는 원천(DART 재무제표 응답 등)의 결과 분포
(`source_outcomes`: 완료·자료 없음·실패·읽을 수 없음·요청 불일치·결산월 미선언), 행 상태(승격 가능·보류·미해결·거부), 미해결 token 표본, 숫자 규칙
flag 분포, 시간 규칙별 null·상한 적용 수, op 분포, stale 행, 계획한 marker, 그리고 설치본의 전제
부족(`blocking`: core schema v2, `sl:` 연결, identity snapshot 등록)과 자료 문제(`refusals`)를 담는다.
`--plan` 없이 실행하면 둘 중 하나라도 있을 때 아무것도 쓰지 않고 거부한다. delta가 비면 generation을
게시하지 않으며, 결과 분포를 내는 승격이면 head의 dataset version에 `promotion_coverage@1` 품질 검사를
남기고 그 ID를 `coverage_check`로 돌려준다. DART 명세의 매퍼 인자 `accept`는 읽을 수 없거나 요청과
맞지 않는 응답을 빼고 승격하도록 허용하는 grant이며, 뺀 응답도 결과 분포에 기록된다. 같은 명세를 다시 실행하면
기존 generation을 검증해 돌려주고, 중단된 승격은 같은 명령이나 `aas db recover`가 끝낸다. 승격은
공급자를 호출하지 않으며, 설정된 공유 계산 예산이 있으면 그 예산 안에서 돈다. `promotions`는
승격 intent마다 단계, generation, dataset version, 행 수, 명세 해시와 매퍼를 나열한다.

`source-retire`는 `aas-source-retirement-v1` 문서의 group마다 참조, 동치 digest, 다른 장치 백업을 확인한다.
`--plan`은 설치본을 읽기 전용으로 열고, `--backup` 없이도 group별 상태와 이유, 증명 범위(`compared`와
`uncompared_columns`), 양쪽 행 수와 digest, 원천별 참조 위치, 후보·은퇴 가능 행의 합계를 보고한다. group의
`uncompared`는 은퇴할 테이블의 비교 밖 열을 정확히 나열해야 하고, 그 열은 은퇴 뒤 백업이나 원본 bytes에서만
다시 얻는다. 백업을 넘기면 그 백업의 모든 파일을 다시 해시하고, 은퇴할 원천마다 백업 안의 테이블 행을
commit이 기록한 digest로 다시 해시하므로 백업 크기와 은퇴 원천 행 수만큼 시간이 걸린다. 백업 안의 행이
기록과 다르면 그 원천은 `backup_content_mismatch`로 거부된다. 보고의 `backup_deep`은 백업이 `--deep`으로
만들어졌는지 알려 준다(그 기록이 없는 백업이면 `null`). `--apply`는 v2 설치본에서 실행 중인 run이나 다른 PREPARED 작업이 없을 때
증명을 통과한 group을 모두 은퇴하고 나머지를 이유와 함께 보고한다. 응답의 `retired`는 은퇴한 원천,
`retired_rows`는 그 행 수, `operation_id`는 intent다. 같은 문서를 다시 실행하면 이미 은퇴한 group을
`already_retired`로 보고한다. 중단된 은퇴는 같은 명령이나 `aas db recover`가 끝낸다. `raw/`의 bytes는 지우지
않는다.

`compact`는 설치본을 검증하고 새 루트에 저장소를 다시 써서 지운 테이블의 공간을 회수한 뒤 새 루트에서 같은
검증 결과를 확인한다. 실행 중인 run, PREPARED 작업, orphan generation이 있으면 거부한다. 응답은 새 루트,
검증 결과, 저장소별 이전·이후 크기다. 원래 설치본은 그대로 남으며, 결과를 확인한 뒤 `AAS_HOME`(또는
`--home`)을 새 루트로 바꾸고 원래 파일은 별도 확인 뒤에 지운다. 실패하면 새 루트는 `restore-incomplete`로
남으므로 새 경로로 다시 실행한다. 두 명령은 설정된 공유 계산 예산 안에서 돈다. 규칙은
[원천 은퇴와 동치 증명](design/data-vertical.md#원천-은퇴와-동치-증명)이 소유한다.

ALFRED vintage(`fred.alfred@1`)는 generation 하나에 관측마다 vintage를 하나만 담으므로 백필은 계약의
[거시와 FX 매퍼](design/data-vertical.md#거시와-fx-매퍼)가 정한 vintage 구간마다 명세 하나를 만들어, 직전
generation을 parent로 순서대로 승격한다. 구간 없이 원천 전체를 계획하면 반복된 자연키로 거부된다.

US 가격 dataset은 이 명령에 명세를 하나씩 넘겨 만든다. 매퍼와 dataset의 대응과 규칙은
[dataset 카탈로그](design/data-vertical.md#dataset-카탈로그)이 소유한다. 명세는 US identity snapshot(`us-build
--norgate-exports`로 만든 문서를 등록한 뒤 provider·namespace별로 만든 snapshot)과 `sessions.xnys` 달력
generation을 pin한다.

- `prices.us.norgate`: `norgate-history-csv-*` 중 미국 주식 내보내기의 `bars` 원천 전부, `norgate.prices_none@1`,
  다섯 값 `decimal_text@1`, 연도 partition마다 generation 하나.
- `prices.us.norgate.ref`: Norgate 조정 part, `norgate.prices_adjusted@1`, 다섯 값 `float_shortest@1`.
- `prices.us.eodhd`: EODHD US 일간 다운로드 하나마다 generation 하나를 수집 순서대로, `eodhd.bars@1`과
  `cross_provider_mismatch@1`(기준은 `prices.us.norgate`의 generation pin).
- `prices.us.fmp.ref`: `fmp-price-eod-non-split-*` 원천 전부를 pin하고 `fmp.eod_non_split@1`의 `revision`을
  1부터 delta가 빌 때까지 올리며 이어 승격한다.
- `prices.ref.norgate`: 기준 시리즈 표는 `norgate.reference_closes@1`, 지수·기타 내보내기는
  `norgate.reference_history@1`, close `decimal_text@1`. 내보내기 명세의 인자 `signed`는 그 내보내기
  `bars`에 대한 `norgate_prices.signed_series` 결과(음수 close가 있는 시리즈의 asset ID)이고, 그 행은
  `unselected_rows`로 센다.

US 기업행동과 상장 상태 dataset도 같은 명령에 명세를 하나씩 넘겨 만든다. 매퍼의 정의는
[기업행동과 상장 상태 매퍼](design/data-vertical.md#기업행동과-상장-상태-매퍼)가 소유한다.

- `actions.us.norgate`: Norgate 조정 part 중 `CAPITAL` part 전부를 한 명세에 pin하고 파티션을 두지 않는다.
  `norgate.dividends@1`(amount `float_shortest@1`)을 첫 generation으로, `norgate.capital_adjustments@1`(ratio
  `float_shortest@1`)을 그 child로 승격한다. 두 시점은 `sessions.xnys`를 pin한 `exdate_open@1`(근거 `record`,
  입력 `ex_date`)이다.
- `actions.us.fmp.ref`: FMP 배당·분할 원천을 `fmp.dividends@1`과 `fmp.splits@1`로, 각각 `revision`을 1부터 delta가
  빌 때까지 올리며 이어 승격한다. 시점 규칙은 `actions.us.norgate`와 같다.
- `status.us.norgate`: Norgate master 하나를 pin하고 `norgate.status@1`의 `event` `listed`를 첫 generation으로,
  `delisted`를 그 child로 승격한다. 두 시점은 `local_day_end@1`(`America/New_York`, 근거 `record`, 입력
  `status_date`)이다.

조정 가격을 읽는 소비자는 canonical 가격 binding과 기업행동 binding(규칙 grant `exdate_open@1`)을
`adjusted_prices.load_adjusted_prices`에 함께 넘긴다. 유도 총수익 가격과 Norgate `TOTALRETURN` part의 대조는
market 파일을 읽기 전용으로 열어 두 매퍼의 `select`를 `CAPITAL` part 전체의 view에 돌리고, 비조정 close를
`float_shortest@1`로 읽은 bar와 그 결과를 `adjusted_prices.adjust`에 넘겨 같은 asset·날짜의 `TOTALRETURN`
close와 상대오차를 잰다. 아무것도 쓰지 않는다.

XNYS 세션 공백 보고는 `scripts/calendar_compare.py --calendar XNYS --source-prefix norgate-history-csv-
--table bars`이고, 날짜로 읽히지 않는 행은 `undated_rows`로 따로 센다. 08-31..09-08 Norgate↔EODHD close
불일치율은 두 dataset을 게시한 market 파일을 읽기 전용으로 열어 다시 계산한다.

```sql
WITH g AS (SELECT generation_id, dataset_id FROM market_generations
           WHERE dataset_id IN ('prices.us.norgate', 'prices.us.eodhd')),
p AS (SELECT g.dataset_id, instrument_id, session_date, close FROM prices JOIN g USING (generation_id)
      WHERE value_state = 'present' AND session_date BETWEEN DATE '2026-08-31' AND DATE '2026-09-08'
      QUALIFY row_number() OVER (PARTITION BY g.dataset_id, instrument_id, session_date
                                 ORDER BY revision_known_at_us DESC) = 1),
pair AS (SELECT n.session_date, n.close AS n, e.close AS e
         FROM p n JOIN p e USING (instrument_id, session_date)
         WHERE n.dataset_id = 'prices.us.norgate' AND e.dataset_id = 'prices.us.eodhd')
SELECT session_date, count(*) AS pairs,
       avg(CASE WHEN n <> e THEN 1 ELSE 0 END) AS any_difference,
       avg(CASE WHEN abs(e - n) > 0.0001 * n THEN 1 ELSE 0 END) AS over_1bp,
       avg(CASE WHEN abs(e - n) > 0.01 * n THEN 1 ELSE 0 END) AS over_1pct
FROM pair GROUP BY ROLLUP (session_date) ORDER BY session_date NULLS LAST;
```

### legacy 원천 편입

```bash
aas import legacy --manifest /path/to/legacy-import.json --sha256 SHA256 --plan
aas import legacy --manifest /path/to/legacy-import.json --sha256 SHA256
aas import legacy --manifest /path/to/legacy-import.json --sha256 SHA256 --verify
```

`import legacy`는 `aas-legacy-import-v1` manifest가 나열한 legacy 원본(Norgate 내보내기와 지수 구성 수집,
SEC submissions·companyfacts archive와 submissions archive의 공시 행, KIND·BOK·OECD 응답, FRED CSV, FMP 비수정 가격 snapshot, Norgate identity
authority)을 `raw/`에 보존하고 원천 자료실의 내용 원천으로 commit한다. manifest 형식, loader별 완결 단위와
출력 열, 거부 규칙은 [데이터 수직 계약](design/data-vertical.md#legacy-원천-편입)이 소유한다. 실제 경로와 기대
수를 담은 manifest는 비공개로 두고 저장소에 넣지 않는다. 테이블 commit에는 `pyarrow`(legacy extra)가 필요하다.

`--plan`은 설치본을 열지 않는다. 원본만 읽어 단위마다 행을 검증하고 원천 ID, 행 수, digest, loader의 대조
지표와 manifest `expect`의 일치 여부(`reconciled`)를 보고하며 아무것도 쓰지 않는다. 원본은 사용자가 소유한
단일 link의 비공개 파일이어야 한다. 그룹·기타 권한이 있는 원본은 `--plan`에서도 같은 이유로 거부되므로
편입 전에 그 디렉터리의 권한을 `go-rwx`로 바꾼다. 실행은 단위마다 원본을 `raw/`에 보존하고 테이블을 commit하며
다시 실행하면 commit된 원천을 재사용한다. 중단되면 같은 명령을 다시 실행한다. `--verify`는 설치본을
읽기 전용으로 열어 계획한 원천과 보존한 색인 파일이 모두 완료·동일·연결됐고 `raw/`의 원본이 온전한지 확인한다.
보고는 항목마다 어떤 단위도 덮지 않는 파일을 `uncovered`(수, bytes, 앞의 경로)로 싣는다. 남길 파일은 manifest
항목의 `retain` 패턴으로 `raw/`에 보존하고, 버려도 되는 파일은 `exclude` 패턴으로 기록한다. 보존 파일의 경로와
해시는 항목의 `legacy-retained-files-*` 원천(`retained_files` 테이블)에 남으므로 항목 경로를 지운 뒤에도
원천 자료실 테이블에서 경로로 찾을 수 있다. Norgate 내보내기 항목의 `batch-NNN/history/checkpoints/*.json`과
최상위 수집 기록처럼 loader가 읽지 않는 파일은 `retain`이나 `exclude`로 기록하기 전까지 `uncovered`다. `complete`는
`unmatched`가 0이고 `reconciled`가 참이며 `uncovered`가 0일 때만 참이고, 그때만 그 manifest의 항목 경로를 지울 수
있다. 항목 경로가 아닌 디렉터리는 지우지 않는다. `--verify`가 `complete`가 아니거나 `--plan`·실행이
`reconciled`가 아니면 보고를 출력하고 종료 코드 1로 끝나므로 스크립트는 종료 코드를 삭제 조건으로 쓴다. 원본 bytes는 `raw/`로 복사되므로 원본
크기만큼의 디스크 공간과, 계획·검증마다 원본 전체를 다시 읽는 I/O 시간이 필요하다. 편입한 원천은 원천
자료실 metadata로 `aas db verify`의 할당에 청구되므로, 수백 개 commit을 더한 설치본의 verify는 공유 계산 예산
환경(`AAS_*_LIMIT*`, `AAS_COMPUTE_LOCK_FILE`)을 설정해 실행한다. 설정하지 않은 기본 할당은 그 metadata를 거부할 수 있다.

### KR 가격 승격

```bash
aas data kr-prices --identity-snapshot ID --lag-us MICROSECONDS \
  --history-lineage SOURCE_ID_PREFIX [--bulk-lineage SOURCE_ID_PREFIX] [--reference] [--plan]
```

`kr-prices`는 EODHD KR 일봉 이력(`--history-lineage`의 `bars` 테이블, 연도마다), 그 이력에서 수집기가
보류한 행(같은 접두어의 `quarantine` 테이블, 값 없는 `invalid` bar로 한 번), 공급자가 부분 응답이라고
경고한 일간 내려받기(`--bulk-lineage`의 KR `quarantine` 테이블, 세션 날짜마다)를 `prices.kr.eodhd`의
generation으로 차례로 승격한다. `--reference`는 이력과 부분 응답 단계를 adjusted close로
`prices.kr.eodhd.ref`에 만든다. 전제는 core schema v2, 원천의 `sl:` 연결, 등록된 KR identity snapshot(`--identity-snapshot`),
`sessions.xkrx`의 committed generation이다. `--lag-us`는 `session_close_plus_lag@1`이 XKRX 마감에 더하는
상한이며 명세와 transform hash에 들어간다. 부분 응답 행은 flag `provider_reported_partial`과 함께
승격되고 generation마다 `partition_row_count@1` 행 수 대조가 `quality_checks`에 남는다. 같은 날짜를
다시 받은 내용이 다른 내려받기는 `sl:` 연결 순서대로 그 날짜의 다음 generation이 되어 앞의 것을
SUPERSEDE한다.

`--plan`은 설치본을 읽기 전용으로 열어 모든 단계를 현재 head의 자식으로 계획하고, 단계별 `promote
--plan` 보고와 합계를 낸다. 실행은 단계마다 앞 단계가 남긴 head를 parent로 승격하고 첫 거부에서
멈추며, 같은 명령을 다시 실행하면 이미 게시된 단계는 빈 delta라 아무것도 쓰지 않는다. 연도 단위 대량
게시의 DuckDB 메모리는 [대량 게시](design/data-vertical.md#대량-게시와-reader)가 정한다. key 색인이 있는 v2
이하 설치본에서는 그 몫이 테이블의 행 수만큼 커지므로 대량 승격은 core schema v3에서 한다. 단계 순서와 규칙은 [데이터 수직 계약](design/data-vertical.md#kr-가격)이 소유한다.

### SEC 회사 header 편입

```bash
aas import sec-companies --source SEC_SUBMISSIONS_SOURCE_ID --plan
aas import sec-companies --source SEC_SUBMISSIONS_SOURCE_ID
```

`import sec-companies`는 `aas import legacy`의 `sec.submissions_zip@1`로 편입한 SEC submissions 내용 원천의
member 색인으로 `raw/`의 archive를 읽어, CIK 문서마다 회사 header(CIK, 이름, 유형, SIC와 설명, 가장 새
공시일) 한 행을 `sec-submissions-companies-*` 내용 원천으로 commit한다. 이 원천은 `sec.sic@1` 분류
승격(`classifications.us.sec`)의 입력이다. `--plan`은 모든 member를 읽어 수를 보고하고 쓰지 않는다. 실행은
다시 실행하면 같은 원천을 재사용하며, 공급자를 호출하지 않는다. 테이블 commit에는 `pyarrow`(legacy
extra)가 필요하다. 원천 연결은 `aas db source-link --apply`로 한다. 행 규칙과 분류 승격은
[분류](design/data-vertical.md#분류)가 소유한다.

### 선언 달력 갱신

```bash
aas calendar refresh --plan
aas calendar refresh [--calendar XKRX] [--calendar XNYS]
aas calendar refresh --declaration /path/to/declaration.json --sha256 SHA256 [--plan]
```

`calendar refresh`는 `aas-calendar-declaration-v1` 선언 문서를 그 달력의 `sessions.<mic>` dataset의
다음 generation으로 승격한다. 인자가 없으면 패키지에 든 XKRX·XNYS 선언을 모두 쓰고, `--declaration`은
운영자가 만든 선언 하나를 파일 SHA-256과 함께 받는다. 실행은 선언 bytes를 `raw/`에 두고 원천 자료실에
날짜마다 한 행인 원천 테이블(`calendar-declared-sessions-<hex>`)을 commit한 뒤 `calendar.declared@1`
매퍼로 승격한다. 원천 테이블 commit에는 `pyarrow`(legacy extra)가 필요하다. 같은 선언을 다시 실행하면
원천을 재사용하고 빈 delta라 아무것도 쓰지 않는다. 바뀐 날짜만 새 generation의 SUPERSEDE가 되고
이전 generation과 그 pin은 그대로 남는다.

`--plan`은 아무것도 쓰지 않는다. 원천이 이미 commit돼 있으면 `aas data promote --plan`과 같은 승격
계획을, 아니면 head 선언과 날짜 단위로 비교한 추가·변경·불변 수와 변경 표본을 보고한다. 응답은
선언 요약(연도별 개장·휴장 수), 다음 해 말까지 덮는지(`coverage.covers_next_year`), head generation,
원천 ID를 함께 담는다. head를 만든 선언보다 이른 `declared_at`의 선언, 같은 `declared_at`의 다른
내용, 다른 달력·시간대, 현재보다 늦은 `declared_at`, head 선언의 날짜를 모두 덮지 않는 범위는 거부한다.
head가 있으면 `--plan`도 head의 원천 테이블을 `pyarrow`로 검증한다. 계획에 stale 행이 남는 선언은
게시하지 않고 거부한다. strict 소비자는 달력 시점(`declared_session_end@1`)을 binding grant로 허용한다.

임시 휴장처럼 공표된 변경은 패키지 선언을 복사해 그 날짜를 `closed`나 `sessions`에 반영하고
`declared_at`을 공표 시각 이후로 올린 문서를 `--declaration`으로 갱신한다. 다음 해 선언과 정정된
과거 일정은 `scripts/calendar_declarations.py`로 패키지 선언을 다시 만들어 리뷰한다. 선언 형식, 시점
규칙, 순서 규칙은 [데이터 수직 계약](design/data-vertical.md#선언-달력)이 소유한다.

## 검사·복구·백업

```bash
aas db verify [--deep]
aas db recover
aas db quarantine --operation OPERATION_ID --reason REASON
aas db backup --output /path/to/new-backup [--deep]
aas --home /path/to/new-home db restore --backup /path/to/new-backup [--deep]
```

`db verify`, `db backup`, `db restore`, `db compact`는 기본으로 원천 자료실 테이블의 기록된 열과 행 수,
승격 chain의 link와 마지막 delta를 대조하고 저장된 행을 다시 해시하지 않는다. `--deep`은 원천 테이블과
승격 delta의 모든 행을 다시 해시해 기록된 digest와 비교한다. 두 방식의 보고 형식은 같고, 백업·복원
응답의 `deep`과 백업 `backup.json`의 `deep`이 어느 쪽으로 검증했는지 기록한다. 기본 검증은 원천 행 값을
읽지 않으므로 수억 행 설치본에서도 행 수 집계만큼만 걸린다. 행 값 손상까지 확인하려면 정기적으로 `--deep`을
실행한다. `source-retire`는 백업이 어느 쪽이든 은퇴할 원천의 백업 행을 다시 해시한다. `migrate`·`run-install`·
`run-migrate`가 `--backup-output`으로 만드는 백업도 기본 검증을 쓰고 `--deep`이면 deep으로 검증한다.
compact는 새로 쓴 루트를 항상 deep으로 검증한다.

`db verify`, `db recover`, `db backup`, `db restore`, `db run-install`, `db migrate`도 설정된 공유 계산 예산을 사용한다. `recover`는 중단된 승격을 그 예산으로 게시하므로 승격을 실행한 것과 같은 예산 환경에서 실행한다.
CLI는 저장소 잠금을 잡기 전에 계산 lease를 확보한다. Python 호출자는 검증·백업·복원과
run-schema 설치 함수의 `budget=`에 자신이 확보한 `ComputeBudget`을 넘긴다. 설정이나
인자를 생략하면 기존 직렬 기본 예산을 유지한다. import의 파일 크기 상한과 검증의 메모리
상한은 별개이므로 큰 문서나 긴 이력에는 더 큰 명시적 예산이 필요할 수 있다. 복원에도
백업을 검증할 때 충분했던 예산을 제공해야 한다. 예산을 늘려도 게시물·원본·행·의도 검사를
생략하지 않으며, 검증 결과나 백업의 논리 보고서 형식은 바뀌지 않는다.

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
aas run execute --request /path/to/prepare-request.json --sha256 SHA256
```

`aas prepare`는 등록된 전략과 고정한 입력만 읽어 각 의사결정 시점의 목표 비중을 계산하고,
기존 `aas backtest`가 읽는 봉투(`aas-etf-backtest-v1`/`v2`)로 내보낸다. 체결·NAV 계산,
run 등록, 결과 저장은 하지 않는다. 봉투 회계는 별도 `aas backtest` 호출이다. 준비부터
결과 저장까지 한 번에 하려면 [`aas run execute`](#준비부터-run-저장까지-한-번에)를 쓴다.
`db run-install`은 그 run이 쓰는 state·market 추가 스키마를 백업 후 설치하며 `prepare`와
`backtest`에는 필요 없다. 복원은 여전히 별도 경로다.

### 준비 요청 문서

요청은 `aas-prepare-request-v1` JSON이며 최상위 키는 `schema`, `hash_format`
(`aas-canonical-json-sha256-v1`), `strategy`, `bindings`, `refs`, `price_inputs`,
`macro_inputs`, `derived_inputs`, `proxy_rules`, `period`, `history`, `cutoff`,
`decision_latency_us`, `explicit_decision_dates`, `account`, `comparison`, `envelope`,
`metadata`다. 계좌 통화가 아닌 가격 선택이 있으면 `fx_conversions`를 더한다. 그 밖의 키는 모두 필수이고
알 수 없는 키·중복 키·bool을 숫자로 쓴 값은 거부한다.
기계용 JSON Schema는 [backtest_request.py](../src/aegis_alpha/engine/backtest_request.py)의
`PREPARE_REQUEST_SCHEMA`가 정본이다. 인라인 가격·전략·기본 관례는 받지 않는다.

- `strategy`: `strategy_store_id`(전략 DB의 `store_id`), `strategy_id`, `version`,
  `raw_sha256`, `contract_sha256`, 고정값 `schema=aas-engine-bundle-v1`,
  `contract_version=aas-engine-v1`, `raw_hash_format=aas-sha256-bytes-v1`,
  `contract_hash_format=aas-canonical-json-sha256-v1`. `raw_sha256`·`contract_sha256`과
  `strategy_id`·`version`은 `strategy import`가 돌려준 값과 정확히 같아야 하며 `latest`는
  버전이 아니다. `strategy_store_id`는 import 응답이 아니라 설치의 `installation.json`에
  적힌 `strategies` store의 `store_id`다.
- `bindings`: 역할별 참조 목록. 각 항목은 `role`, `ordinal`, `ref_kind`, `ref_id`,
  `ref_version`, `hash`, `ref_schema`, `hash_format`이다. 필수 역할은 `signal_prices`,
  `execution_prices`, `sessions`, `identity`, `universe`, `membership`, `calendar`, `basis`,
  `cost`, `execution`이고, `macro`·`derived`·`proxy`는 전략 정의가 요구할 때만, `benchmark`·
  `risk_free`·`fx`는 0 또는 1개다. `actions`는 `heads`로 묶은 canonical 신호 선택 중 조정 basis인 것(유도 선택)마다
  하나다. 유도 선택은 binding 순서대로 `actions` ordinal을 하나씩 받으므로(첫 유도 선택이 0, 다음이 1)
  시장마다 다른 기업행동 원천을 묶을 수 있다. 여러 개를 허용하는 역할은 `signal_prices`·`execution_prices`·
  `macro`·`derived`·`proxy`·`actions`뿐이며 ordinal은 역할 안에서 0부터 연속이다. `signal_prices`·
  `execution_prices`·`sessions`·`macro`는 `generation` 또는 `heads` 참조를, `actions`는 `heads` 참조만 받는다.
  `fx_conversion`은 `fx_rates` 도메인의 `heads` 참조만 받고 `fx_conversions` 항목마다 하나이며 여러 개를
  허용한다.
- `refs`: bindings가 가리키는 참조 서술자. `ref_kind`, `ref_id`, `ref_version`, `hash`,
  `schema`, `hash_format`, `pin`을 담고 같은 서술자를 두 번 넣으면 거부한다.
  `generation`의 pin은 `data inspect`가 돌려주는 `dataset_id`·`version`·`generation_id`·
  `chain_hash`·`manifest_hash`, `identity`는 `snapshot_id`·`content_hash`, `universe`는
  `universe_id`·`version`·`content_hash`, `derived`·`membership`·`convention:<kind>`는
  `kind`·`id`·`version`·`hash`다. `heads`의 pin은 `aas-head-binding-v1` 문서에서 `schema`를 뺀
  `domain`·`pins`(각 pin은 `generation` pin의 다섯 필드와 `from`·`to` 날짜 또는 null)·`granted_rules`·
  `excluded_flags`이고, `ref_id`와 `hash`는 그 문서의 binding hash, `ref_version`은 `aas-head-binding-v1`이다.
  binding의 `hash`는 그 pin의 `chain_hash`, `content_hash`, binding hash 또는 문서 전체 해시와 같아야 한다.
  `heads` 참조의 읽기 규칙은 [데이터 수직 계약](design/data-vertical.md#strict-실행-준비의-head-binding)이 소유한다.
- `price_inputs`: `signal_prices`·`execution_prices` binding마다 하나씩. `binding`(`role`·
  `ordinal`), 정렬된 `instrument_ids`, `currency`, `basis`(`unadjusted`·`split_adjusted`·
  `total_return`), `price_role`(`canonical`·`reference`), `interval=1d`. 신호 가격은 basis
  관례와 맞아야 하고 체결 가격은 `unadjusted`·`canonical`이어야 한다.
- `fx_conversions`: 계좌 통화가 아닌 가격 통화마다 하나. `binding`(`fx_conversion` 역할·ordinal),
  `currency`(세 글자 대문자), `series_id`(`<currency>/<계좌 통화>` 또는 `<계좌 통화>/<currency>`),
  `max_fixing_age_days`(0 이상 정수), `signal_basis`(`account_currency` 또는 `price_currency`). 환산이
  없으면 열쇠를 두지 않으며 빈 배열은 거부한다. 규칙과 기록은
  [FX 변환 계약](design/data-vertical.md#fx-변환-계약)이 소유한다.
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
   pin이다. `aas data import`로 게시한 typed generation도 같은 pin 형식이지만, 신호·체결
   가격과 세션 입력으로 `generation` 참조가 고를 수 있는 generation은 `register-prices`/
   `register-sessions`의 native transform으로 게시하고 원본 `source-import`와 보존 테이블이 남아 있는
   것뿐이다. 준비는 이 generation마다 native transform 문서와 보존 원본을 다시 대조해 admission하며,
   내용이 불투명한 generic 게시는 그 선행 조건을 대신하지 못한다. `macro` 입력의 generic
   게시는 5번 항목대로 계속 지원한다. 승격한 generation(`aas data promote`, `aas data kr-prices`,
   `aas calendar refresh`)은 `heads` 참조로 묶는다. 그 pin은 `dataset_versions`의 committed 행이고, 각
   generation의 시간 규칙 출처는 보존한 명세에서 읽으며, 규칙 시점은 binding의 `granted_rules`가 허용할 때만
   strict 판단에 쓰인다.
3. `aas data binding-import`: `aas-identity-snapshot-v1`, `aas-universe-version-v1`,
   `aas-ensemble-membership-v1` 문서. 문서 스키마에 따라 identity snapshot, universe version,
   정의(derived·membership) 또는 `aas-input-bundle-v1` 묶음을 등록하고 `pin`을 돌려준다.
   정규 표기의 `aas-head-binding-v1` 문서는 pin이 marker와 catalog에 맞는지 확인한 뒤 `raw/`에 그
   binding hash로 남기고 `pin.binding_hash`를 돌려주므로, 그 hash를 `heads` 참조로 가리키는 묶음은
   실행 전에도 등록·검증된다.
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
증거(`stored_inputs`), `heads` 참조로 한 읽기마다 reader가 돌려준 영수증(`head_reads`), `source_pins`, 판단·체결 슬롯,
각 판단의 replay 영수증, feature 값, `certified=false`를 담는다. 봉투 자체에는 이 출처가 들어가지 않는다.

실패는 stdout 없이 stderr 한 줄 `{"error": ...}`와 종료 코드 1이다. 봉투를 쓴 뒤 sidecar나
fsync 단계에서 실패하면 이미 쓴 파일이 검사용으로 남는다. 그 파일은 성공 영수증이 아니며
재시도는 같은 경로를 덮어쓰지 않으므로 새 경로를 쓰거나 직접 정리한다.

### 시점 규칙

의사결정일은 세션 종가의 경제 날짜이고, 지식 시점은 별도로 정한다. 각 슬롯의 cutoff는
`min(knowledge_cutoff_us, 종가 시각 + decision_latency_us)`이며 종가 이후, 다음 시가 이전이어야
한다. 신호·거시·파생 관측은 그 cutoff까지 공개·수정 인지된 revision만 반영하고 stale 검사도
cutoff의 UTC 날짜를 기준으로 한다. `ingestion_cutoff_us`가 있으면 그 시각까지 AAS가 수집한
revision만 재생한다. 나중에 게시한 generation·revision은 같은 pin의 과거 판단을 바꾸지 않는다.
두 모드 모두 cutoff 이후로 알려진 공개 시각(`available_at_us`)이나 수정 인지 시각
(`revision_known_at_us`)을 가진 revision은 거부한다. `strict_pit`은 여기에 더해 두 시각이
모두 알려진 행만 반영하고 참조 가격을 제외한다. `observed_snapshot_research`는 공개 시각을
모르는 스냅샷 행을 경제 날짜 기준으로 미인증 상태로 받아들이는 연구 모드이며, 모르는
시각을 수집 시각에서 추론하지 않는다. `ingestion_cutoff_us`는 두 모드에서 같은 별도 제한이다.
두 모드 모두 `certified=false`다.

체결 결과 가격은 `period` 안의 모든 open 세션에서 따로 읽는다. 판단일의 다음 시가가 봉투의
날짜 격자와 다르면(나중에 안 달력 수정이 그 세션을 없애거나 더 이른 시가를 넣은 경우)
`incompatible outcome calendar projection`으로 내보내기 전에 거부한다. 체결·격자를 조용히
옮기지 않는다. 매수 종목의 시가가 없거나 이력 버킷이 부족하거나 pin된 달력이 불완전해도
거부한다. 매도만 남은 종목의 시가 누락은 준비가 아니라 `aas backtest` 회계에서 거부한다.

### 봉투 회계와 Python API

`aas backtest --input ENVELOPE --sha256 <envelope.sha256>`는 새 프로세스에서 봉투를 읽어
NAV·체결을 계산한다. 응답의 `source_pins_verified`·`point_in_time_verified`·`live_orders`는
계속 false다. 회계 결과는 stdout의 JSON 응답 하나이며 명령이 결과 파일을 저장하지 않는다.
아래 실습의 `tee "$LAB/backtest.json"`처럼 셸 리디렉션으로 남기는 사본은 run 보존이 아니다.
결과를 run으로 확정하려면 아래 `aas run`을 쓴다. 복원은 별도 경로다.

같은 준비를 Python에서 호출할 수 있다.
[backtest_prepare.py](../src/aegis_alpha/application/backtest_prepare.py)의
`parse_prepare_request(raw: bytes) -> ParsedPrepareRequest`, `PrepareRequest(parsed)`,
`prepare_backtest(workspace, request, *, budget: ComputeBudget) -> PreparedBacktest`가
공개 진입점이다. `prepare_backtest`는 compute lease를 잡지 않으므로 호출자가 먼저 lease를
소유하고, 그 lease가 준 `compute_resources.ComputeBudget`을 넘긴 뒤
`storage.workspace.open_workspace(home)`으로 읽기 전용 설치를 연다. CLI가 쓰는 같은 순서는
[prepare_cli.py](../src/aegis_alpha/application/prepare_cli.py)에 있다.
`application.compute_cli.price_compute(excluded_locks=storage_lock_targets(home,
load_paths(home).stores()))`가 compute 환경을 검증하고 lease를 잡은 뒤 budget을 yield하며,
compute 환경이 없으면 `None`을 yield한다. 아래 실습 5단계가 이 순서를 그대로 쓴다.
`PreparedBacktest`는 `request`, `definition`, `slots`, `decisions`, `features`,
`inputs`, `projection`, `envelope`(`canonical_bytes`·`envelope_sha256`), `provenance`(sidecar
바이트), `targets`, `request_hash`, `certified=False`를 가진다. 봉투 바이트는
`application.backtest_cli.run_document(canonical_bytes, envelope_sha256)`로 회계에 넘긴다.
`calculation_identity()`·`environment_identity()`는 `request_hash`에 들어가는 엔진·환경
정체성이고, `engine.backtest_request`의 `request_projection`·`export_envelope`는 저장소 없이
순수 투영·내보내기만 맡는다.

계산 소스 정체성은 `backtest_prepare.CALCULATION_MODULES`에 명시된 47개 모듈의
설치된 정확한 바이트를 해시한다. 엔진과 두 데이터 직렬화 helper뿐 아니라 준비의 해석·입력
승인을 담당하는 application·storage 모듈을 포함한다. 런타임 import 탐색이나 패키지 전체
해시는 아니며, 선택된 파일의 주석 변경도 정체성을 바꾼다. 이전 27개 범위로 생성한 정규
요청 P·pin·artifact는 수정하지 않고 원래 정체성으로 읽는다. 새 준비의 소스 해시와
`request_hash`는 의도적으로 달라지지만, 같은 입력·동작의 기존 봉투와 회계 결과는 그대로다.

이 정체성은 안정된 설치 소스와 지원 런타임을 전제로 하며 전체 소프트웨어 공급망의 해시가
아니다. 순수 엔진과 달리 준비 경로는 DuckDB·SQLite 및 선택적 Arrow 소스의 PyArrow를
사용한다. 이 외부 driver·의존성 버전은 닫힌 환경 v1에 포함되지 않으므로 임의 버전 간
재현성을 보장하지 않는다. 의존성 정체성이 필요하면 별도의 버전된 환경 계약이 필요하며,
v1 필드에 조용히 추가하지 않는다.

### 준비부터 run 저장까지 한 번에

`aas run execute`는 위의 준비와 회계를 정식 run 기록까지 이어 붙인 명령이다. 네 단계로
나뉘며 계산 동안에는 저장 잠금도 state 트랜잭션도 쥐지 않는다.

| 단계 | 쥐는 것 | 하는 일 |
| --- | --- | --- |
| 0 | compute lease + 읽기 전용 설치 | run add-on 확인, `prepare_backtest`, 선택적 파일 내보내기 |
| A | compute lease + 짧은 쓰기 | 입력 bundle·요청 등록, `open_run`으로 의도 확정과 입력 봉인 |
| B | compute lease만 | 봉인한 봉투 바이트로 회계 계산 |
| C | compute lease + 짧은 쓰기 | 설치 정체성과 pin 재확인 후 `commit_run`, 정상 실패면 `fail_run` |

run 저장 표는 기본 설치에 없다. `aas db run-install`을 한 번 실행해 두지 않으면 0단계에서
그 명령을 알려주며 계산 전에 끝난다. 이 명령이 표를 몰래 만들지 않는다.

```bash
aas db run-install
aas run execute --request "$LAB/request.json" --sha256 "$REQUEST_SHA256"
aas run show --run-id run-0123456789abcdef
aas run list
```

선택 인자는 `--reason`(기록할 사유), `--bundle-id`(입력 bundle 이름; 기본값은 요청의
bindings와 요청 hash에서 함께 유도한다. bundle 하나에는 요청 하나만 저장되므로, 같은 입력을
고정한 채 기간·계좌·일정만 바꾼 요청은 다른 이름을 받는다), `--prior-run-id`(재계산의 선행 run),
`--run-id`(명시적 run ID),
`--envelope-output PATH`(봉투와 `PATH.preparation.json`을 파일로도 남긴다; 기존 경로는
덮어쓰지 않는다)다. 요청 파일 규칙과 compute 환경 요구는 `prepare`와 같다.

성공 응답은 결과가 저장된 뒤에만 돌아온다. `request_hash`, `bundle_id`,
`envelope.sha256`·`preparation.sha256`, 판단별 `target_weights`, 회계 응답 전체를 담은
`backtest`, 그리고 `run_id`·`status`·`result_hash`·표별 hash와 행 수·지표를 담은 `run`,
`certified=false`가 들어 있다. 같은 요청을 CLI로 돌리든
[run_backtest.py](../src/aegis_alpha/application/run_backtest.py)의
`run_backtest(RunBacktestRequest(...))`로 돌리든 `run.run_id`만 다르고 나머지는 같다.
`read_backtest_run(run_id, home=...)`와 `list_backtest_runs(home=...)`가 조회 진입점이며
`aas run show`·`aas run list`가 그것을 그대로 부른다.

`aas run show`는 기록·결과 표시자·봉인 파일이 모두 서로 맞을 때만 run을 돌려주고, 아니면
거부한다. 실패는 stdout 없이 stderr 한 줄 `{"error": ...}`와 종료 코드 1이다. 잘못된 요청
hash, 잘못된 pin, 미등록 전략 버전, 설치 경쟁, 그리고 run을 열기 전의 예산 부족은 run을
남기지 않는다. run을 연 다음의 실패는 다르다. 계산이 정상적으로 실패하거나, 결과를 봉인할
예산이 모자라거나, 준비 이후 설치 정체성이 바뀌면 그 run은 `fail_run`으로 FAILED가 되어
run 이력에 남는다. 어느 쪽이든 성공 영수증은 없다. 그 run을 끝내지 못했다면 오류에
`notes`가 붙어 run ID와 필요한 복구 명령을 알려준다.

준비가 붙잡고 있는 봉투·출처 바이트와 계산이 만든 결과 바이트는 다음 단계의 예산에서
`reserved_bytes`로 뺀다. 각각은 들어가지만 합치면 할당을 넘는 두 적재가 동시에 살아 있는
상황을 막는다. 남은 여유가 없으면 그 단계를 시작하기 전에 거부한다.

기록하는 `engine_hash`는 준비 목록 `CALCULATION_MODULES`의 정체성이다. 회계를 실행하는
`application/backtest_cli`와 응답을 직렬화하는 `storage.publication`은 그 목록에 없다.
두 파일만 바뀌면 같은 `request_hash`가 다른 결과 바이트를 봉인할 수 있다. 목록을 넓히면
모든 요청의 `request_hash`가 바뀌므로 준비 정체성을 소유한 별도 결정으로 다룬다.

계산 도중 프로세스가 죽으면 run은 RUNNING으로 남는다. 기존 복구 계약이 그대로 적용된다.
표시자가 없으면 `aas db recover`가 INTERRUPTED로 끝내고, 정상 표시자가 있으면 저장을
재개하며, 손상 증거는 QUARANTINED가 된다. 복구는 계산이나 공급자 호출을 되풀이하지 않는다.
재계산은 새 run이므로 `--prior-run-id`로 이전 run에 연결한다.

입력을 봉인하는 도중 실패해도 run은 RUNNING으로 남는다. 이때는 `fail_run`으로 억지로
끝내지 않고 복구에 맡긴다. `--envelope-output`으로 내보낸 파일은 run을 열기 전에 쓰므로
실패해도 검사용으로 남는다. 둘 다 성공 영수증이 아니다.

**같은 설치에서 run이 도는 동안 `aas db recover`를 돌리지 않는다.** 표시자가 없는 상태는
"죽은 계산"과 "아직 계산 중"을 구분하지 못한다. 복구는 계산 단계에서 저장 잠금을 쥐지 않는
살아 있는 run도 INTERRUPTED로 끝낼 수 있고, 그러면 그 run의 저장이 거부되며 명령은 구조화된
오류로 실패한다. 잘못된 결과가 저장되지는 않지만 실행은 버려진다. 이는 복구 계약 자체의
성질이며 통합 명령이 새로 만든 것이 아니다.

### 선언한 미인증 연구 실행

`aas run research`는 위의 단계 구조를 그대로 쓰되, 인증된 요청 대신 선언 문서 하나를 받는다.
보존 관측 패널에는 실행 경로가 없고 이 명령도 열지 않는다. 회계는 자기 응답 바이트에 미인증·
비실행을 적는 모드로만 돌고, run 저장소는 선언을 그 선언이 봉인한 준비 문서와 짝지어 받는다.

`aas-research-run-v2` 슬리브 선언과 `aas-research-composition-v1` 표본 조합 선언을 모두 받는다.
어느 쪽인지는 문서의 `schema_version`이 말하므로 호출자가 고르지 않는다.

패널 원천은 선언 최상위의 열쇠 하나로 정한다. `observations`는 보존 관측 generation pin 목록이고,
`prices`는 canonical 가격 binding이다. 둘 다 있거나 둘 다 없으면 거부한다.

```json
"prices": {
  "pins": [{"dataset_id": "prices.kr.eodhd", "version": "<v>", "generation_id": "<g>",
            "chain_hash": "<sha256>", "manifest_hash": "<sha256>", "from": null, "to": null}],
  "excluded_flags": ["provider_reported_partial"]
},
"instrument_map": {"<instrument_id>": "<전략의 자산 ID>"}
```

`pins`는 `aas-head-binding-v1`의 순서 있는 pin과 `[from, to)` cutover 구간이고, `excluded_flags`는
읽지 않을 quality flag다. 연구 읽기는 엄격 PIT가 아니므로 시간 규칙 grant는 선언하지 않는다.
`instrument_map`의 열쇠는 identity가 발급한 instrument ID이고 값 자산은 상태 저장소에서 `etf`로
분류돼 있어야 한다. 가격 통화는 `conventions.currency`이거나 `fx_conversions` grant가 허용한 통화다.
선언한 `knowledge_time`보다 늦게 알려진 revision은 읽지 않는다. 봉인 준비 문서의 `prices.head_read`가
그 읽기 영수증이다.

슬리브가 거시 신호를 읽으면 선언은 그 series를 `macro`로 grant하고, 다른 통화의 가격을 쓰면
`fx_conversions`로 grant한다. 둘 다 쓰지 않으면 열쇠를 두지 않는다.

```json
"macro": [
  {"series_id": "T10Y3M", "unit": "percent",
   "binding": {"domain": "macro_observations", "pins": [<pin>], "excluded_flags": []}}
],
"fx_conversions": [
  {"currency": "USD", "series_id": "USD/KRW", "max_fixing_age_days": 5,
   "signal_basis": "account_currency",
   "binding": {"pins": [<fx_rates pin>], "excluded_flags": []},
   "prices": {"pins": [<USD 가격 pin>], "excluded_flags": []}}
]
```

`macro`는 슬리브들이 읽는 series와 정확히 같아야 하고 판단마다 그 cutoff로 다시 읽힌다.
`fx_conversions`의 `prices`는 선택이며, 있으면 그 통화의 chain을 선언의 `prices`와 함께 읽는다.
봉인 준비 문서의 `macro`와 `fx_conversions`가 grant와 그 아래의 모든 읽기를 기록한다. 계약은
[연구 실행의 canonical 가격 패널](design/data-vertical.md#연구-실행의-canonical-가격-패널)이 소유한다.

run 저장 표는 기본 설치에 없고, 선언된 계약을 담으려면 add-on이 v1보다 높아야 한다. 둘 다
0단계에서 확인하므로 설치가 부족하면 계산 전에 실행할 명령을 알려주고 끝난다.

```bash
aas db run-install
aas db run-migrate
aas run research --declaration "$LAB/declaration.json" --sha256 "$DECLARATION_SHA256"
aas run show --run-id <선언이 정한 run ID>
aas run rerun --run-id <같은 run ID>
aas run rerun --run-id <같은 run ID> --declaration "$LAB/declaration.json" --sha256 "$DECLARATION_SHA256"
```

선택 인자는 `--reason`, `--bundle-id`, `--prior-run-id`, `--envelope-output PATH`로 `aas run execute`와
같다. `--run-id`는 없다. 선언과 그것이 만든 봉투의 내용이 run 식별자를 정하므로 같은 선언은 늘
같은 run을 가리키고, 이름을 고를 수 있으면 한 계산을 다른 이름으로 접수하게 된다. 같은 선언을
두 번 실행하면 두 번째는 끝난 run을 다시 열지 못하고 거부된다.

선언 파일은 원하는 대로 써도 된다. 등록·해시·묶음의 기준은 canonical 형식이고 `--sha256`은
파일 바이트를 확인한다. 응답의 `declaration_sha256`은 파일의 것, `request_hash`는 canonical
내용의 것이다.

성공 응답에는 `request_schema`·`scope`, `bundle_id`와 무엇이 실제로 묶였는지 적은 `bindings`,
`envelope.sha256`·`preparation.sha256`, 판단별 `target_weights`, 조합이면 전환이 일어난 날짜를
담은 `composition`, 회계 응답 전체와 run 기록이 들어 있다. 이 경로가 자기에 대해 말하는 것은
모두 부정형이다: `certified=false`, `non_executable=true`, `executable_prices=false`,
`point_in_time_certified=false`, `observed_prices_verified=false`, `source_parity=unknown`.

표본 조합은 묶음 층에서 membership을 묶지 않는다. 묶음 어휘가 membership 하나만 들 수 있어
두 슬리브 중 하나만 묶으면 절반짜리 묶음이 완전해 보이므로, 조합은 아무것도 묶지 않고 선언의
내용 해시가 둘을 함께 덮는다. 영수증의 `bindings`가 `membership_bound=false`와 해시만이 덮는
항목을 그대로 적는다.

`aas run rerun`은 아무것도 쓰지 않는다. `--run-id`만 주면 봉인한 봉투를 같은 회계에 다시 넣어
저장한 결과 바이트와 맞춰 보고, 선언을 함께 주면 준비까지 다시 해서 run 식별자·봉투·봉인 문서를
각각 비교한다. 결과 재현과 준비 재현은 서로 다른 주장이므로 `checked`에 실제로 확인한 것만
올리고 각각의 결과를 따로 적는다. 바뀐 봉투에서 결과만 재현되는 경우가 있으므로 한쪽을 다른
쪽의 근거로 쓰지 않는다.

기록은 가독성이지 자격이 아니다. 저장한 run이 있어도 `admit_native_input`은 같은 pin을 도메인에서
거부하고, 같은 문서를 `aas run execute`에 넘기면 거부된다.

### 저장소 checkout 실습

아래는 설치된 앱의 사용 절차가 아니라 저장소 checkout에서 공개 합성 fixture로 전체 흐름을
확인하는 실습이다. `uv sync --locked --dev`로 준비한 환경이 필요하며, 이 환경에는 `legacy`
추가 의존성이 함께 들어 있다. `tests.application.test_prepare_cli.register_fixture`는 테스트
helper이지 설치되는 공개 API가 아니다. helper는 seed 설치에서 합성 문서를 뽑은 뒤 시험
대상 home에 대해 `init`, `strategy import`, `db source-import` 3회, `data register-prices` 2회,
`data register-sessions`, `data inspect` 3회, `data binding-import` 3회, `data convention-import`
4회를 실제 CLI로 실행하고 `request.json`을 쓴 뒤 `incoming/` 원본 파일을 지운다.

```bash
set -euo pipefail
LAB="$(mktemp -d /var/tmp/aas-prepare-lab-XXXXXX)"
export AAS_HOST_CPU_LIMIT=1 AAS_HOST_MEMORY_LIMIT_BYTES=1073741824 \
  AAS_CPU_LIMIT=1 AAS_MEMORY_LIMIT_BYTES=1073741824 \
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
MISSING_EXIT=0
uv run --no-sync aas --home "$LAB/home" prepare --request "$LAB/missing-membership.json" \
  --sha256 "$(sha256sum "$LAB/missing-membership.json" | cut -d' ' -f1)" \
  --output "$LAB/missing.json" 2>"$LAB/missing.stderr" || MISSING_EXIT=$?
echo "exit=$MISSING_EXIT"
cat "$LAB/missing.stderr"
test "$MISSING_EXIT" -eq 1
test ! -e "$LAB/missing.json"
test ! -e "$LAB/missing.json.preparation.json"
ls "$LAB"

# 5. 같은 요청을 Python API로 준비한다. 호출자가 lease를 먼저 잡고 그 budget을 넘긴다.
uv run --no-sync python - "$LAB" <<'EOF'
import hashlib, json, sys
from pathlib import Path
from aegis_alpha.application.backtest_prepare import (
    PrepareRequest, parse_prepare_request, prepare_backtest)
from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.storage.locks import storage_lock_targets
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.workspace import open_workspace

root = Path(sys.argv[1])
home = root / "home"
request = PrepareRequest(parse_prepare_request((root / "request.json").read_bytes()))
targets = storage_lock_targets(home, load_paths(home).stores())
with price_compute(excluded_locks=targets) as budget:
    if budget is None:
        raise SystemExit("prepare requires the explicit AAS compute budget environment")
    with open_workspace(home) as workspace:
        prepared = prepare_backtest(workspace, request, budget=budget)
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

한 실행에서 관측한 값이다. 전략 store ID와 identity 등록 시각이 실행마다 달라지므로
`request_sha256`·`request_hash`·`preparation.sha256`은 예시이고, 날짜·비중·NAV·체결은
fixture가 고정한 독립 기대값이다. 봉투 바이트에는 그 값이 들어가지 않아 두 실행에서
같은 `envelope.sha256`을 관측했다.

- 2단계 영수증: `request_sha256=40ce893b…def9`, `request_hash=3f2b4821…564a`,
  `envelope.sha256=a30a6dd4…be4f`, `preparation.sha256=b20b060e…98a3`, `certified=false`.
  `$LAB/incoming/`은 이미 없다.
- 3단계 회계: NAV 날짜 `2026-01-29, 02-02, 02-26, 03-02, 03-30`에 equity
  `100, 100, 80, 80, 80`, 체결은 `01-29→02-02 ASSET_A 100/15주 @15`,
  `02-26→03-02 ASSET_A -100/15주 @12`, `02-26→03-02 ASSET_B 4주 @20`, 수수료 0,
  `source_pins_verified=false`, `point_in_time_verified=false`, `live_orders=false`.
  독립 계산: 1월 29일 수익률 ASSET_A 0.5 > ASSET_B 0.1이라 A 전량, 2월 2일 시가 15에
  100/15주, 2월 26일 종가 12로 80, 3월 2일 A 매도 후 B를 시가 20에 4주, 3월 30일 종가 20으로 80.
- 4단계: `missing.stderr`에 `{"error": "missing required executable bindings"}`, `exit=1`,
  `missing.json`과 그 sidecar는 만들어지지 않는다. 블록의 `test`가 이 둘을 확인하고, 다른
  종료 코드나 남은 파일이 있으면 `set -e`로 실습이 그 자리에서 끝난다.
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

## KR 공시·상장 수집

```bash
aas collect dart plan [--home HOME] [--today YYYY-MM-DD]
aas collect dart plan --legacy-root LEGACY_OPENDART_DIR [--kind-receipt KIND_DIR/response.json] [--today YYYY-MM-DD]
aas collect dart run --key-file OPENDART_KEY_FILE [--max-calls 2000] [--daily-quota 19000] [--home HOME]
aas collect kind run [--home HOME]
```

`dart plan`은 설치본을 읽기 전용으로 열어 오늘(Seoul) 실행이 물을 것을 보고한다. corp code 목록 갱신
여부, 빠진 공시 목록 page와 날 수, 재무 요청의 이유별·기간별 수, 회사 수와 KIND 축소 여부, commit되지
않은 receipt 수다. 공급자를 호출하지 않는다. `--legacy-root`는 설치본 대신 legacy OpenDART 수집 디렉터리의
`attempts.jsonl`과 receipt를 읽어 같은 계획을 만들고 그 원장 요약(`legacy`)을 함께 보고한다.

`dart run`은 설치본을 쓰기로 열고 한 번의 제한된 수집을 한다. 키 파일은 소유자만 읽을 수 있는 한 줄
파일이며 Git checkout 밖에 둔다. 중단된 attempt 정산, commit되지 않은 receipt의 commit, corp code 목록,
공시 목록, 재무 요청 순서로 진행하고, 호출 수는 `--max-calls`와 24시간 quota의 남은 수 중 작은 값이다.
응답은 결과와 상관없이 `raw/`와 `opendart-receipts-<hex>` 원천에 남고, 호출마다 state의
`collection_jobs`·`collection_attempts`·`usage_events`에 기록된다. 응답은 공급자 호출 수, 이유별 요청 수,
결과 분포, 멈춘 이유(`budget`, `provider_refused:<상태>`, `transport_failures`), 남은 계획, commit한 원천이다.
예산을 다 쓰면 정상 종료이고, 키·한도 거부나 전송 실패로 멈추면 받은 답을 commit한 뒤 종료 코드 1이다.
commit에는 `pyarrow`(legacy extra)가 필요하다.

`kind run`은 KIND 유가증권·코스닥 상장법인목록을 받아 각각 `kind-listings-<hex>` 원천으로 commit한다. 두
목록이 모두 commit되면 다음 `dart` 계획은 그 단축코드의 회사로 좁혀진다. 목록 하나라도 commit되지 않으면
종료 코드 1이다.

첫 `dart run` 전에 `kind run`으로 두 목록을 commit한다. 두 목록이 없으면 계획은 corp code 목록에서 종목코드가
있는 모든 회사(상장폐지·코넥스 포함)를 묻고 보고서의 `kind_filter`가 `false`다. `dart plan`으로
`kind_filter: true`와 회사 수를 확인한 뒤 `dart run`을 켠다.

승인된 수집기(OpenDART corp code·공시 목록·재무, KIND 목록)지만 패키지 설치나 이 명령은 예약 실행을
만들지 않는다. 예약 실행은 [하루 유지보수 실행](#하루-유지보수-실행)이고, `[owner]` [운영 전환](#운영-전환)에서 키 파일과 호출
상한을 정해 켠다. 같은 키를 쓰는 legacy DART backfill unit은 그 전에 멈춘다(두 수집기는 quota를 서로 세지 않는다). 요청·cohort·원장·보존 규칙은
[KR 공시·상장 수집](design/data-vertical.md#kr-공시상장-수집)이 소유한다.

## US 공시·거시 수집

```bash
aas collect sec plan [--home HOME] [--today YYYY-MM-DD] [--since YYYY-MM-DD] [--issuers registered|all]
aas collect sec run --user-agent-file SEC_USER_AGENT_FILE [--since YYYY-MM-DD] [--issuers registered|all] [--max-calls 2000] [--home HOME]
aas collect fred plan [--home HOME] [--today YYYY-MM-DD]
aas collect fred run --key-file FRED_KEY_FILE [--max-calls 500] [--home HOME]
```

`plan`은 설치본을 읽기 전용으로 열어 실행이 처음 물을 것을 보고하고 공급자를 호출하지 않는다. `sec plan`은
New York 날짜(`--today`)의 덮이지 않은 색인 날, 이미 commit된 색인에서 나온 submissions·companyfacts 요청의
이유별 수, 원한 공시·완료·발행인 범위 밖·포기 수, 처음 날과 덮인 마지막 날이다. `fred plan`은 FRED 날짜의
시계열별 알려진 vintage 날, 실시간 끝(어제), 요청의 이유별 수(`vintage_dates:origin`·`vintage_check`,
`series_csv:daily`)다. 둘 다 commit되지 않은 receipt 수를 함께 보고한다.

`run`은 설치본을 쓰기로 열고 한 번의 제한된 수집을 한다. `--user-agent-file`은 SEC 공정 접근 규칙의 연락처를
담은 `User-Agent` 한 줄, `--key-file`은 FRED API 키 한 줄이며, 둘 다 소유자만 읽을 수 있는 Git checkout 밖의
파일이다. 중단된 attempt 정산과 commit되지 않은 receipt의 commit을 먼저 한다. SEC는 색인, submissions,
companyfacts 순으로, FRED는 CSV, 시계열별 vintage 확인과 observations 창 순으로 묻는다. 호출마다 state의
`collection_jobs`·`collection_attempts`·`usage_events`에 기록된다. 응답은 `raw/`와 `sec-*`·`fred-*` 내용
원천에 남는다. 출력은 공급자 호출 수, 이유별 요청 수, 결과 분포, 멈춘 이유(`budget`,
`provider_refused:<HTTP 상태>`, `transport_failures`), commit한 원천이다. SEC는 덮인 마지막 날과 남은 계획을,
FRED는 시계열별 알려진 vintage 날과 완결되지 않은 창을 함께 낸다. 예산을 다 쓰면 정상 종료이고, 거부나 전송
실패로 멈추면 받은 답을 commit한 뒤 종료 코드 1이다. commit에는 `pyarrow`(legacy extra)가 필요하다.
`--issuers registered`(기본)는 identity에 SEC 발행인으로 등록된 제출자의 문서만 묻는다. 처음 SEC 실행은
`--since`로 legacy bulk archive 뒤의 첫 색인 날을 정한다.

수집한 원천의 승격은 [원천 자료의 승격과 은퇴](#원천-자료의-승격과-은퇴)의 명세로 한다.
`sec-submissions-filings-*`는 `sec.submissions@1`, `sec-companyfacts-facts-*`는 `sec.companyfacts@1`,
`fred-alfred-observations-*`는 `fred.alfred@1`(원천마다 `vintage_partitions`의 구간 순서로),
`fred-series-csv-*`는 `fred.fx_series@1`이다. 승인된 수집기(SEC 색인·submissions·companyfacts, FRED/ALFRED와
DEXKOUS CSV)지만, 패키지 설치나 이 명령이 예약 실행을 만들지는 않는다. 예약 실행은
[하루 유지보수 실행](#하루-유지보수-실행)이고, `[owner]` [운영 전환](#운영-전환)에서 연락처·키 파일과 호출 상한을 정해 켠다. 요청·창·선택·보존 규칙은
[US 공시·거시 수집](design/data-vertical.md#us-공시거시-수집)이 소유한다.

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
검증된 원문은 아래 `aas collect qveris import`가 원본 자료실에 적재한다.
가격의 조정 기준·종목 식별·거래일·공개 시각을 확인하기 전에는 백테스트 입력이나
`data datasets`의 시장 버전으로 자동 승격하지 않는다. 예약 실행은 운영자가 별도로
설치하고 실제 적재 결과와 중복 호출 여부를 확인한다. 패키지 설치는 예약 작업을 만들지 않는다.

### 명시한 job의 수집과 적재

```bash
aas collect qveris daily-jobs --raw-root RAW --exchange US --exchange KO --exchange KQ \
  --dataset prices --dataset splits --dataset dividends --lookback-days 14 --output JOBS.json
aas collect qveris plan --jobs JOBS.json --raw-root RAW
aas collect qveris run --jobs JOBS.json --raw-root RAW --key-file KEY \
  --max-calls 30 --max-credits 100 [--workers 4] [--request-interval 0.75]
aas collect qveris quarantine --raw-root RAW --key-file KEY \
  (--batch parallel-batches/ID | --page jobs/FINGERPRINT/0000) --reason TEXT
aas collect qveris import --raw-root RAW --identity IDENTITY.json [--market KR] \
  [--dataset price_history] [--fingerprint FP ...] [--limit N] [--plan]
```

- `daily-jobs`는 선언 달력에서 관측일(기본 UTC 오늘) 이전의 열린 세션마다 일간 내려받기 요청을 만들고,
  raw root에 이미 완료된 요청은 빼며 완료 없이 시도만 있는 요청은 `held`로 보고한다. 새 요청이 있을
  때만 `--output`을 새로 만들고 그 SHA-256을 출력한다. 키를 읽지 않고 요청하지 않는다.
- `plan`은 job 문서를 검증하고 문서마다 `completed`(같은 fingerprint가 완료돼 `run`이 HTTP 없이 재사용),
  `held`(같은 fingerprint의 시도를 `run`이 정산), `equivalent`(같은 요청이 관측일이나 job ID가 다른 job으로
  완료·시도돼 `run`이 다시 유료 호출), `new`를 센다. 키를 읽지 않고 raw root를 만들지 않는다.
- `run`은 유료 호출 수(`--max-calls`)와 크레딧(`--max-credits`)을 실행 하나의 상한으로 예약하고, 서버
  잔액에서 다른 예약을 뺀 값도 확인한다. HTTP 시도 수(`--max-http-requests`, 기본 유료 호출의 16배와 32 중
  큰 값)와 시간(`--time-limit-seconds`)은 새 page를 시작하지 않게 한다. 이미 시작한 page는 실행과 정산을
  마치므로 `http_requests`가 한도를 조금 넘을 수 있다. 작업자 1·2·3·4·8·16 중 하나이며 모든 요청 시작은 한
  간격을 공유한다. 종료 코드는 완료와 예산 소진이 0, 정산된 실패가 있으면 1, 불확실한 호출이나 정산되지 않은
  page·group으로 멈추면 2다. 진행 기록은 raw root의 `cohorts/`·`parallel-cohorts/`에 남고 표준 오류로 진행
  줄을 낸다.
- 종료 코드 2의 page나 group은 다음 `run`이 먼저 정산한다. usage가 끝내 나타나지 않거나 intent 묶음이
  불완전해 정산할 수 없으면 `quarantine`이 그 page(`--page`) 또는 group(`--batch`)을 운영자 사유와 함께
  격리한다. 격리는 견적 전체의 예약을 남기고 자동 재호출하지 않으며, 그 뒤 계정의 새 실행을 다시 허용한다.
  대상은 `run`의 표준 오류 보고와 raw root의 `jobs/`·`parallel-batches/`에서 찾는다.
- `import`는 완료된 job을 내용 원천으로 commit하고 공급자를 호출하지 않는다. `--plan`은 읽기 전용으로
  설치본을 열어 정규화와 행 digest만 보고한다. identity 문서는 가격·기업행동 행에 필요하고 통화쌍
  이력에는 필요 없다. 실패한 job은 `failures`, 완료 문서가 없는 `--fingerprint`(`no_completion`)와 읽을 수
  없는 완료 문서(`unreadable_completion`)는 `missing`으로 남고 나머지 job을 적재한 뒤 종료 코드 1이다. 단위와 원천 ID 규칙은 [데이터 수직 계약](design/data-vertical.md#qveris-수집과-원천-적재)이 소유한다.

## 하루 유지보수 실행

```bash
aas maintain plan [--home HOME] [--at 2026-10-03T03:00:00Z] [--promotions]
aas maintain run [--home HOME]
aas maintain receipt [--home HOME] [--lock uv.lock]
```

`run`은 설치본을 쓰기로 한 번 열고 끝까지 저장소 잠금을 가진 채 복구, 실행 환경 대조, 선언 달력 갱신,
수집(KIND, OpenDART, SEC, FRED/ALFRED, Qveris), Qveris·KR 종목 목록 적재, KR identity 증분, dataset chain
승격, head·watermark 보고를 순서대로 한다. 실행 중 다른 명령은 `installation_busy`로 거부된다. 보고는 표준
출력과 `raw/`, `<runtime>/maintain-report.json`에 남는다. 종료 코드는 모든 단계가 끝나면 0(예산 소진 포함),
단계 실패나 공급자 거부가 있으면 1, Qveris가 정산·격리가 필요한 유료 호출로 멈추면 2다. 종료 코드 2의 page나
group은 [Qveris 원문 수집](#명시한-job의-수집과-적재)의 `quarantine`으로 사유와 함께 격리한다. 격리된 요청은
다음 날 새 job으로 다시 묻는다.

`plan`은 읽기 전용으로 같은 단계를 계획한다. 자격 증명을 읽지 않고 공급자를 부르지 않는다. 공급자별 계획(KR은
`aas collect dart plan`, US는 `sec plan`·`fred plan`과 같은 보고, Qveris는 요청 창·이유별 수·보류·대기·job 목록),
identity 증분 계획, dataset마다 새 원천과 단계를 낸다. `--promotions`는 그 단계의 승격을 현재 head에 대해
계획한다.

설정은 설치본 `runtime.json`의 `jobs`와 `providers`다. `jobs.enabled`가 `true`이고 공급자 절의 `enabled`가
`true`인 공급자만 부른다. 자격 증명 파일은 소유자만 읽을 수 있는 한 줄 파일이며 상대 경로는 설치본 `secrets/`
안이다. 필드와 기본값은 [유지보수 실행](design/data-vertical.md#유지보수-실행)이 소유한다.

```json
{
  "jobs": {"enabled": true},
  "providers": {
    "kind": {"enabled": true},
    "dart": {"enabled": true, "key_file": "opendart-api-key", "max_calls": 2000, "daily_quota": 19000},
    "sec": {"enabled": true, "user_agent_file": "sec-user-agent", "max_calls": 2000, "since": "2026-08-31"},
    "fred": {"enabled": true, "key_file": "fred-api-key", "max_calls": 500},
    "qveris": {"enabled": true, "key_file": "qveris-api-key", "raw_root": "/path/to/raw/qveris",
               "identity": "/path/to/qveris-bulk-identity.json",
               "since": {"US": "2026-09-01", "KO": "2026-09-05", "KQ": "2026-09-05"},
               "extra_sessions": {"US": ["2026-07-28"]},
               "symbol_lists": ["KO", "KQ"], "forex": ["USDKRW"], "max_calls": 30, "max_credits": "100"}
  }
}
```

유지보수는 chain을 이어 붙일 뿐 시작하지 않는다. 각 dataset의 첫 generation은 [운영 전환](#운영-전환)에서 운영자가
`aas data promote`(또는 `data kr-prices`, `calendar refresh`)로 만들고, 그 뒤 실행이 새로 수집된 원천을 그
명세의 규칙으로 이어 붙인다. head가 없는 dataset은 `no_head`로 보고된다.

설치와 예약은 [운영 전환](#운영-전환)의 일이다. 태그된 dev 커밋을 정확한 CPython 경로로 설치하고 receipt를 남긴다.

```bash
uv tool install --python /path/to/cpython-3.13.N/bin/python3.13 \
  'aegis-alpha-system[legacy] @ git+https://github.com/thisisjun786/aegis-alpha-system@<tag>'
aas maintain receipt --lock /path/to/<tag>/uv.lock
install -m 0644 config/systemd/aas-maintain.service config/systemd/aas-maintain.timer \
  ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now aas-maintain.timer
```

`receipt`는 interpreter 경로·버전, package의 저장소·태그·커밋과 `RECORD` 해시, 환경의 distribution 목록 해시,
lock 해시를 `<runtime>/install-receipt.json`과 `raw/`에 남긴다. 실행마다 그 receipt와 실행 환경의 차이가 보고에
남지만 실행을 막지는 않는다. timer는 매일 03:00 UTC에 `aas maintain run`을 시작하고 놓친 실행을 따라잡는다.
사용자 unit은 system의 `network-online.target`을 기다릴 수 없으므로 service가 시작 전에 공급자 host 이름이
해석될 때까지 최대 5분 기다린다. 그래도 네트워크가 없으면 공급자 단계가 실패로 남고(종료 코드 1) 다음 실행이
놓친 날을 묻는다. 종료 코드 2는 Qveris에 정산되지 않은 유료 page가 남았다는 뜻이므로 보고의 `held`를 보고
정산이나 격리를 한다.
패키지 설치나 이 명령은 unit을 설치하거나 켜지 않는다.

## 운영 전환

운영 전환은 legacy 예약 unit이 쓰던 설치본을 한 번에 `aas maintain`으로 옮기는 일회성 절차다. 순서대로
실행하고, 각 단계는 같은 요청으로 다시 실행하면 재사용되거나 이미 끝난 일을 보고한다. `[owner]`는 운영자가
값을 정하거나 결과를 보고 넘어가는 단계다. 삭제는 증명과 다른 장치 백업을 통과한 항목만 일괄로 하고
결과를 은퇴 기록으로 남긴다. 수집기는 승인된 다섯 공급자(KIND, OpenDART, SEC, FRED/ALFRED, Qveris)를 모두
설정의 실행 상한과 함께 켠다.

**다른 장치.** `aas db source-retire`는 설치본 루트, state, market, `raw/` 중 하나와 같은 장치의 백업을
`backup_on_installation_device`로 거부한다. 그래서 백업 위치(`~/aas-backups`)와 다른 장치에 설치본 전체를
모은다. 1단계 백업을 3단계에서 저장소 장치의 새 루트로 복원해 그 루트를 설치본으로 쓰고, 원래 설치본은
비교와 되돌리기용으로 그대로 둔다.

```bash
TS=20261004T000000Z                   # 전환을 시작한 UTC 시각; 전환 내내 같은 값
OLD_HOME=~/.aas                       # 지금의 설치본(runtime.json이 market·raw를 다른 장치에 둘 수 있음)
BACKUPS=~/aas-backups                 # 새 설치본의 모든 경로와 다른 장치
REHEARSAL=/path/to/store-device/rehearsal-$TS
HOME_NEW=/path/to/store-device/aas-new  # 복원한 새 설치본(이름은 core schema 버전과 무관)
HOME_FINAL=/path/to/store-device/aas-final # 은퇴 뒤 compact한 최종 설치본
LEGACY=~/.local/share/aegis-alpha     # legacy 런타임 루트
QVERIS=/path/to/store-device/qveris   # 유지보수의 Qveris raw root와 identity 문서
SPECS=/path/to/private/specs          # 승격 명세, 은퇴 문서, legacy manifest(비공개, 읽기만 함)
REPORTS="$BACKUPS/$TS-reports"        # 계획·실행·검증 보고와 만든 registry
LEGACY_UNITS="aas-native-data-maintenance aas-qveris-korea-backfill aas-korea-historical-backfill"
export AAS_HOST_CPU_LIMIT=8 AAS_HOST_MEMORY_LIMIT_BYTES=25769803776 \
  AAS_CPU_LIMIT=8 AAS_MEMORY_LIMIT_BYTES=25769803776 \
  AAS_COMPUTE_LOCK_FILE=/path/to/store-device/compute.lock
sha() { sha256sum "$1" | cut -d' ' -f1; }
mkdir -p -m 0700 "$BACKUPS"
mkdir -m 0700 "$REPORTS"
```

`/path/to/<tag>`는 설치한 태그의 저장소 checkout이다. 계산 예산은 수억 행 원천 자료실의 `--deep` 검증이 쓰는 메모리(최대 약 18 GB)를 담는 값이다. 백업 하나는
설치본의 state, strategies, market, `raw/`, `runs/` 크기만큼 공간을 쓴다.

**0. 동결.** 설치본에 쓰는 legacy timer와 service를 멈추고 남은 작업을 정리한다. ETF 탐색 unit은
설치본에 쓰지 않으므로 5단계에서 은퇴한다. 저장소는 Git checkout 안의 경로를 거부하므로(상위 어딘가의 빈
`.git`도 그렇다) 먼저 전환이 쓰는 경로의 상위에 `.git`이 없는지 확인한다. 아무것도 출력되지 않아야 한다.

```bash
for p in "$OLD_HOME" "$BACKUPS" "$REHEARSAL" "$HOME_NEW" "$HOME_FINAL" "$QVERIS" "$SPECS"; do
  d=$(realpath -m "$p"); while [ "$d" != / ]; do [ -e "$d/.git" ] && echo "$d/.git"; d=$(dirname "$d"); done
done
for unit in $LEGACY_UNITS; do systemctl --user stop "$unit.timer" "$unit.service"; done
systemctl --user list-units --all 'aas-*'   # 위 service가 모두 active가 아님
AAS_HOME="$OLD_HOME" aas db recover
AAS_HOME="$OLD_HOME" aas db status
```

**1. 백업.** `--deep` 백업은 먼저 모든 원천 테이블과 승격 delta를 다시 해시해 검증하므로 전환 전 설치본의
deep 검증을 겸한다. 백업 ID는 `backup.json`의 SHA-256이다.

```bash
AAS_HOME="$OLD_HOME" aas db backup --output "$BACKUPS/$TS-v1" --deep > "$BACKUPS/$TS-v1.json"
sha "$BACKUPS/$TS-v1/backup.json"
```

**2. 리허설.** 같은 백업을 새 루트로 복원해 설치본 안에 쓰는 단계만 먼저 끝까지 실행한다. 3단계의
migrate·연결, 4단계 전체, 6단계의 은퇴 백업·`source-retire`·`compact`이고, 단계마다 시간과 최대
메모리(`/usr/bin/time -v`)와 `aas db verify` 결과를 기록한다. 리허설의 모든 출력은 리허설 이름을 단
경로(`$REHEARSAL`, `$REHEARSAL-compact`, `$RB`)에 쓰므로 실제 3·6단계의 백업과 compact 대상과 겹치지 않는다. 복원한
`runtime.json`은 공급자가 없고 `jobs.enabled`가 `false`라 리허설은 공급자를 부르지 않는다. 설치본 밖을 지우는
6단계의 legacy 정리, systemd 편집과 timer, 설치 receipt는 리허설에서 실행하지 않는다. 리허설은 legacy 원본을
읽기만 하므로 실제 4단계가 같은 원본을 다시 읽는다. 마지막 `rm -rf` 줄은 끝까지 마친 리허설을 버리는 정리
예제다. 4단계 도중 멈춘 리허설을 설치본으로 쓸 때는 그 줄을 실행하지 않고 아래 [리허설 채택](#리허설-채택)으로 간다.

```bash
RB="$BACKUPS/rehearsal-$TS"; mkdir -m 0700 "$RB" "$RB/reports"
aas --home "$REHEARSAL" db restore --backup "$BACKUPS/$TS-v1"
(
  export AAS_HOME="$REHEARSAL" REPORTS="$RB/reports"
  aas db migrate --to 3 --backup-output "$RB/migrate"
  aas db source-link --apply
  # 여기서 4단계 1–7의 명령을 그대로 실행한다(설치본과 $REPORTS에만 쓴다)
  retire="$SPECS/retirement.json"
  aas db backup --output "$RB/retire" --deep > "$RB/retire.json"
  aas db source-retire --spec "$retire" --sha256 "$(sha "$retire")" --backup "$RB/retire" --plan > "$RB/retire.plan.json"
  aas db source-retire --spec "$retire" --sha256 "$(sha "$retire")" --backup "$RB/retire" --apply > "$RB/retire.apply.json"
  aas db compact --to "$REHEARSAL-compact" > "$RB/compact.json"
  AAS_HOME="$REHEARSAL-compact" aas db verify --deep > "$RB/verify.json"
)
rm -rf -- "$REHEARSAL" "$REHEARSAL-compact" "$RB"
```

**3. 설치본 이동, 마이그레이션, 연결.** 1단계 백업을 새 설치본으로 복원하고 그 설치본에서 core schema를 현재
버전 v3로 올린 뒤 원천 자료실 commit을 모두 `sl:` 연결한다. 이후 모든 명령은 `AAS_HOME="$HOME_NEW"`로 실행한다.

```bash
aas --home "$HOME_NEW" db restore --backup "$BACKUPS/$TS-v1"
export AAS_HOME="$HOME_NEW"
aas db migrate --to 3 --plan
aas db migrate --to 3 --backup-output "$BACKUPS/$TS-migrate"
aas db source-link --plan
aas db source-link --apply
```

`migrate --to 3`은 v1 설치본을 v2와 v3 두 단계로 올리고, 단계마다 `$BACKUPS/$TS-migrate/core-schema-migrate-v2`와
`.../core-schema-migrate-v3`에 검증된 백업을 만든 뒤에만 그 단계의 intent를 기록한다. 단계의 intent 뒤에 멈추면 같은
명령이 그 단계를 백업 없이 끝내고, 한 단계를 끝내고 다음 단계의 intent 전에 멈추면 새 `--backup-output`으로
다시 실행한다. v3 단계는 market의 도메인 테이블과 `quality_flags`를 한 트랜잭션에서 다시 쓰므로 market 장치에
그 테이블을 한 벌 더 담을 여유와 DuckDB 임시 디렉터리의 spill 여유가 필요하고, 단계를 끝내기 전 모든 generation을
다시 해시한다. `--plan`의 `blocking_operations`가 비어 있어야 실행한다. `source-link --plan`의 `unbacked`·`corrupt`·`incomplete`·`invalid`가 비어 있어야 다음 단계로 간다.

### 리허설 채택

4단계 도중 멈춘 리허설은 1단계 백업에서 복원해 같은 명령을 실행한 설치본이므로 3단계와 앞선 4단계를 다시 하지
않고 그 루트를 새 설치본으로 채택할 수 있다. 예를 들어 v3 이전 코드로 v2까지 올린 리허설에서 대량 승격의
COMMIT이 DuckDB 할당을 넘어 그 승격 intent만 PREPARED로 남은 경우다. 채택하면 `$REHEARSAL`이 `$HOME_NEW`의
자리를 맡고 리허설의 보고(`$RB/reports`)가 `$REPORTS`다. v3를 담은 검증된 태그를 설치하고 그 리허설을 쓰는
프로세스가 없을 때 실행한다. 리허설의 기존 출력과 멈춘 intent는 지우거나 `quarantine`하지 않는다.

```bash
HOME_NEW="$REHEARSAL"; RB="$BACKUPS/rehearsal-$TS"; REPORTS="$RB/reports"
export AAS_HOME="$HOME_NEW"
aas db status
aas db migrate --to 3 --plan > "$RB/migrate-v3.plan.json"
aas db migrate --to 3 --backup-output "$RB/migrate-v3" > "$RB/migrate-v3.json"
aas db verify --deep > "$RB/migrate-v3.verify.json"
aas db recover
aas db verify
```

`--plan`은 남은 단계가 v3 하나이고(`migrations`), `blocking_operations`가 비어 있으며, `carried_operations`가 멈춘
승격 intent 하나를 `proof`와 함께 적어야 한다. 아니면 실행하지 않는다. `$RB/migrate-v3`는 그 intent를 PREPARED로
담은 migration의 rollback snapshot이며 복구를 마친 백업으로 쓰지 않는다. migration 뒤 `verify --deep`은 모든
generation의 hash와 v3 감사를 통과하고 `pending_operations`가 1이다. `recover`는 보존 명세를 다시 계획해 intent의
manifest가 그대로 나올 때만 게시하므로 게시한 generation의 hash는 멈추기 전 계획과 같다. `recover`가 그 intent를
`pending`으로 남기면 멈추고 보존 증거를 그대로 둔다. 게시가 끝나면 멈춘 명세부터 4단계 6을 이어 간다. 멈춘
명세의 `--plan`과 실행은 게시된 generation을 검증해 `reused`로 돌려주고 새 generation을 만들지 않으며, 그
다음 명세부터 새 generation이 생긴다. 앞선 명세와 앞선 dataset은 다시 승격하지 않는다. 이어서 4단계 7과
5–7단계를 `AAS_HOME="$HOME_NEW"`로 실행한다. `$RB`의 migration 백업(`$RB/migrate`, `$RB/migrate-v3`)은 7단계의
`$TS-migrate`에 해당하고, `$RB/reports`는 7단계가 읽으므로 그때까지 둔다.

**4. 승격.** 아래 순서로 dataset의 첫 generation을 만든다. 명세와 문서는 모두 `$SPECS`의 비공개 파일이고,
`[owner]`는 각 `--plan` 보고(원천·매핑 행 수, `blocking`, `refusals`, 미해결 표본)를 보고 실행한다. 실행은
`blocking`이나 `refusals`가 있으면 아무것도 쓰지 않고 거부하며, 실패한 게시는 `aas db recover`가 재개한다.
공급자는 부르지 않는다.

1. 달력: `aas calendar refresh --plan`, `aas calendar refresh`.
2. KR identity: `aas identity kr-import ... [--plan]`, `aas identity kr-build ... --output "$REPORTS/kr-registry.json"`,
   `aas identity register --file "$REPORTS/kr-registry.json" --sha256 "$(sha "$REPORTS/kr-registry.json")" [--plan]`,
   `aas identity snapshot --id ID --provider P --namespace N [--plan]`([KR identity](#kr-identity)).
3. KR 가격: `aas data kr-prices --identity-snapshot ID --lag-us LAG --history-lineage PREFIX --bulk-lineage PREFIX
   [--reference] --plan`, 같은 명령에서 `--plan`을 뺀 실행([KR 가격 승격](#kr-가격-승격)).
4. legacy 원본: manifest마다 `--plan`이 `reconciled`이고 `[owner]` 항목마다 `uncovered` 파일을 `retain`·`exclude`로
   기록한 뒤 실행하고 `--verify`가 `complete`인지 확인한다([legacy 원천 편입](#legacy-원천-편입)).

   ```bash
   for manifest in "$SPECS"/legacy-*.json; do
     name=$(basename "$manifest" .json)
     aas import legacy --manifest "$manifest" --sha256 "$(sha "$manifest")" --plan > "$REPORTS/$name.plan.json" &&
     aas import legacy --manifest "$manifest" --sha256 "$(sha "$manifest")" &&
     aas import legacy --manifest "$manifest" --sha256 "$(sha "$manifest")" --verify > "$REPORTS/$name.verify.json" ||
     break
   done
   aas import sec-companies --source SEC_SUBMISSIONS_SOURCE_ID
   aas db source-link --apply
   ```

5. US identity와 universe: `aas identity us-build --master ID ... --norgate-exports --bindings ID --output
   "$REPORTS/us-registry.json"`, `register`, `snapshot`([US identity](#us-identity)), 이어서 `aas universe index`와
   `aas universe listings`([universe](#universe)).
6. 명세 승격: US 가격(연도별), 기업행동·상장 상태, 거시·FX(ALFRED는 vintage 구간 순서), 공시, 재무(SEC는 연도
   partition, DART), 분류 순서로 파일 이름을 붙인 명세를 하나씩 승격하고 generation마다 검증한다.

   ```bash
   for spec in "$SPECS"/promote/*.json; do
     name=promote-$(basename "$spec" .json)
     aas data promote --spec "$spec" --sha256 "$(sha "$spec")" --plan > "$REPORTS/$name.plan.json" &&
     aas data promote --spec "$spec" --sha256 "$(sha "$spec")" > "$REPORTS/$name.apply.json" &&
     aas db verify > "$REPORTS/$name.verify.json" ||
     break
   done
   aas data promotions
   ```

7. 전략 레지스트리: `aas strategy promote --source SOURCE_ID --sha256 SHA256 --plan`, 같은 명령의 `--apply`.

**5. 유지보수 전환.** 태그된 dev 커밋을 설치하고, 자격 증명과 Qveris 수집 기록을 새 설치본으로 옮기고,
다섯 수집기를 켠 `runtime.json`으로 계획을 확인한 뒤 timer를 켜고 legacy unit을 은퇴한다.

```bash
uv tool install --python /path/to/cpython-3.13.N/bin/python3.13 \
  'aegis-alpha-system[legacy] @ git+https://github.com/thisisjun786/aegis-alpha-system@<tag>'
aas maintain receipt --lock /path/to/<tag>/uv.lock
install -m 0600 "$LEGACY/secrets/opendart-api-key" "$LEGACY/secrets/qveris-api-key" "$AAS_HOME/secrets/"
# [owner] FRED API 키 한 줄과 SEC 연락처 User-Agent 한 줄
(umask 077 && cat > "$AAS_HOME/secrets/fred-api-key")
(umask 077 && cat > "$AAS_HOME/secrets/sec-user-agent")
mkdir -p -m 0700 "$QVERIS"
cp -a "$LEGACY/data/raw/qveris" "$QVERIS/raw" && diff -r "$LEGACY/data/raw/qveris" "$QVERIS/raw"
install -m 0600 "$LEGACY/research/native-etf/qveris-bulk-identity.json" "$QVERIS/identity.json"
aas collect qveris import --raw-root "$QVERIS/raw" --identity "$QVERIS/identity.json" --plan > "$QVERIS/parity.json"
TODAY=$(TZ=Asia/Seoul date +%F)
aas collect dart plan --today "$TODAY" > "$REPORTS/dart-plan.json"
aas collect dart plan --legacy-root /path/to/legacy-opendart --today "$TODAY" > "$REPORTS/dart-legacy-plan.json"
```

`parity.json`은 Qveris의 replay 대조다. 옮긴 raw root의 완료 job은 이미 적재된 원천 ID로 다시 계산되어
`reused`이고 `failed`와 `missing`이 0이어야 한다. `built`는 아직 적재되지 않은 job 수이며 첫 실행이 적재한다.
OpenDART는 legacy 원장을 같은 계획 규칙으로 재생한 `dart-legacy-plan.json`과 설치본의 `dart-plan.json`을
`[owner]`가 비교한다. 설치본 계획이 legacy 계획에 없는 공시 목록 날을 묻는다면 그 날의 legacy 응답이 설치본에
들어오지 않은 것이므로 timer를 켜기 전에 정산한다. KIND·SEC·FRED의 legacy 원문은 4단계 `--verify`가
`complete`인 것이 replay 대조다. 최근 원문까지 다시 읽어 같은 원천 ID와 digest로 맞춘다. 이어서
`runtime.json`에 `jobs.enabled: true`와 [하루 유지보수 실행](#하루-유지보수-실행)의 다섯 공급자 절을 쓴다.
Qveris 절의 `raw_root`는 `$QVERIS/raw`, `identity`는 `$QVERIS/identity.json`이고, `[owner]` `max_calls`와
`max_credits`는 남은 크레딧 안에서 한 실행의 상한으로 정한다. SEC `since`는 legacy bulk archive 뒤의 첫 색인
날이다.

```bash
aas maintain plan --promotions > "$AAS_HOME/runtime/cutover-plan.json"
install -m 0644 /path/to/<tag>/config/systemd/aas-maintain.service \
  /path/to/<tag>/config/systemd/aas-maintain.timer ~/.config/systemd/user/
mkdir -p ~/.config/systemd/user/aas-maintain.service.d
cat > ~/.config/systemd/user/aas-maintain.service.d/home.conf <<CONF
[Service]
Environment=AAS_HOME=$AAS_HOME
Environment=AAS_HOST_CPU_LIMIT=$AAS_HOST_CPU_LIMIT AAS_HOST_MEMORY_LIMIT_BYTES=$AAS_HOST_MEMORY_LIMIT_BYTES
Environment=AAS_CPU_LIMIT=$AAS_CPU_LIMIT AAS_MEMORY_LIMIT_BYTES=$AAS_MEMORY_LIMIT_BYTES
Environment=AAS_COMPUTE_LOCK_FILE=$AAS_COMPUTE_LOCK_FILE
CONF
for unit in $LEGACY_UNITS aas-etf-discovery; do
  systemctl --user disable --now "$unit.timer"
  systemctl --user stop "$unit.service"
  rm -f ~/.config/systemd/user/"$unit".service ~/.config/systemd/user/"$unit".timer
done
systemctl --user daemon-reload
systemctl --user reset-failed
systemctl --user enable --now aas-maintain.timer
systemctl --user list-units --all 'aas-*'   # aas-maintain.service와 active인 aas-maintain.timer만
```

`maintain plan`은 시작한 dataset chain마다 `no_head`나 `no_template` 없이 새 원천의 단계를 내야 한다. 실패한
legacy service는 unit 파일을 지워도 `reset-failed` 전까지 목록에 남는다. 종료 코드 2로 끝난 실행은
[Qveris 원문 수집](#명시한-job의-수집과-적재)의 `quarantine`으로 정산한다.

**6. 정리.** 은퇴 문서를 계획하고, 승격까지 담은 다른 장치 deep 백업으로 증명을 통과한 원천을 일괄 은퇴한 뒤
compact로 공간을 회수하고 설치본을 새 루트로 바꾼다. 은퇴와 compact 동안 timer를 멈춘다.

```bash
systemctl --user stop aas-maintain.timer
aas db backup --output "$BACKUPS/$TS-retire" --deep > "$BACKUPS/$TS-retire.json"
retire="$SPECS/retirement.json"
aas db source-retire --spec "$retire" --sha256 "$(sha "$retire")" --backup "$BACKUPS/$TS-retire" --plan > "$REPORTS/retire.plan.json"
aas db source-retire --spec "$retire" --sha256 "$(sha "$retire")" --backup "$BACKUPS/$TS-retire" --apply > "$REPORTS/retire.apply.json"
aas db compact --to "$HOME_FINAL" > "$BACKUPS/$TS-compact.json"
sed -i "s|^Environment=AAS_HOME=.*|Environment=AAS_HOME=$HOME_FINAL|" ~/.config/systemd/user/aas-maintain.service.d/home.conf
export AAS_HOME="$HOME_FINAL"
aas maintain receipt --lock /path/to/<tag>/uv.lock
systemctl --user daemon-reload && systemctl --user start aas-maintain.timer
```

은퇴 대상은 Norgate normalized·canonical-v1 사본, KR 이전 lineage와 그 보류분, 중복 DART lineage, Qveris
부분 중복분, FMP 반복 retrieval분, master 사본이다. `--plan`은 group마다 상태와 이유, 참조 위치, `uncompared`
열을 보고하고 `--apply`는 증명을 통과한 group을 모두 은퇴하고 나머지를 이유와 함께 보고한다. `[owner]`
`uncompared` 열이 있는 group은 은퇴 문서에 그 열을 이름으로 적어 허가한다. 은퇴는 `raw/`의 bytes를 지우지
않으므로 은퇴한 원천의 원본 archive tar는 `raw/`에 남는다. compact는 `runtime/`을 옮기지 않으므로 새 루트에서
설치 receipt를 다시 남긴다. 은퇴 백업(`$TS-retire`)은 `source_retirements`의 `backup_id`가 가리키므로 지우지
않는다.

설치본 밖 legacy 원본은 증명된 것만 지우고, 최종 설치본(`AAS_HOME`이 `$HOME_FINAL`)에서만 지운다. manifest의
`--verify`가 `complete`이고 종료 코드가 0이면 그 항목 경로만 지운다. 그 보고는 7단계가 다시 읽는다. manifest 항목이 아닌 legacy 경로(코드 snapshot, 릴리스, DB dump, 준비 snapshot)는 `$BACKUPS`에
tar로 보관해 원본과 대조한 뒤 지운다. legacy worktree는 커밋되지 않은 변경과 push되지 않은 커밋이 없을 때만
지운다.

```bash
if [ "$AAS_HOME" = "$HOME_FINAL" ]; then
  for manifest in "$SPECS"/legacy-*.json; do
    name=$(basename "$manifest" .json)
    aas import legacy --manifest "$manifest" --sha256 "$(sha "$manifest")" --verify > "$REPORTS/$name.final-verify.json" &&
    jq -e '.complete' "$REPORTS/$name.final-verify.json" > /dev/null &&
    jq -r '.entries[].path' "$manifest" | while read -r path; do rm -rf -- "$path"; done
  done
  diff -r "$LEGACY/data/raw/qveris" "$QVERIS/raw" && rm -rf -- "$LEGACY/data/raw/qveris"
  ARCHIVE="$BACKUPS/$TS-legacy"; mkdir -m 0700 "$ARCHIVE"
  for path in "$LEGACY/preparation" "$LEGACY/releases" /path/to/legacy-postgres-dump; do
    name=$(basename "$path"); parent=$(dirname "$path")
    tar --create --file "$ARCHIVE/$name.tar" -C "$parent" "$name" &&
    tar --compare --file "$ARCHIVE/$name.tar" -C "$parent" &&
    (cd "$ARCHIVE" && sha256sum "$name.tar" >> SHA256SUMS) &&
    rm -rf -- "$path"
  done
  worktree=/path/to/legacy-worktree
  test -z "$(git -C "$worktree" status --porcelain)" &&
    test -z "$(git -C "$worktree" log --branches --not --remotes --oneline)" && rm -rf -- "$worktree"
fi
```

원래 설치본(`$OLD_HOME`과 그 `runtime.json`이 가리키는 market·`raw/`)과 `$HOME_NEW`는 7일 동안 두고, 그동안
유지보수 실행이 성공하고 7단계가 통과하면 지운다. 기본 설치 위치를 쓰지 않으므로 대화형 셸도
`AAS_HOME="$HOME_FINAL"`를 설정한다.

**7. 사후 확인과 은퇴 기록.** 새 설치본에서 유지보수를 한 번 실행하고, deep 백업을 다른 장치에 만든 뒤
전환 확인을 기록한다. 백업과 확인 사이에 예약 실행이 시작되지 않도록 `list-timers`의 다음 실행 시각이 확인을
마칠 만큼 남았는지 본다.

```bash
systemctl --user start aas-maintain.service   # 끝날 때까지 기다린다
systemctl --user list-timers aas-maintain.timer
aas db backup --output "$BACKUPS/$TS-post" --deep > "$BACKUPS/$TS-post.json"
systemctl --user list-units --all 'aas-*'
aas maintain cutover-check --backup "$BACKUPS/$TS-post" \
  --expect-provider kind --expect-provider dart --expect-provider sec \
  --expect-provider fred --expect-provider qveris \
  $(for manifest in "$SPECS"/legacy-*.json; do
      printf -- '--legacy-manifest %s --legacy-verify %s ' \
        "$manifest" "$REPORTS/$(basename "$manifest" .json).final-verify.json"
    done) \
  --removed "$LEGACY/data/raw/qveris" --removed "$LEGACY/preparation" --removed "$LEGACY/releases" \
  --removed /path/to/legacy-postgres-dump --removed /path/to/legacy-worktree \
  --record > "$BACKUPS/$TS-cutover-record.json"
```

`--deep` 백업은 설치본 전체의 deep 검증을 먼저 통과해야 만들어지므로 `aas db verify --deep`과 `aas db backup`의
성공을 함께 증명한다. `cutover-check`는 설치본을 읽기 전용으로 열고 항목마다 결과를 낸다.

| 항목 | 통과 조건 |
| --- | --- |
| `schema` | core schema가 현재 버전 |
| `operations` | `PREPARED`로 남은 storage operation이 없음 |
| `backup` | 백업의 모든 파일이 다시 해시해 맞고, `deep`, 같은 설치본, 끝난 storage operation을 모두 담고, 설치본 루트·state·market·`raw/`와 다른 장치 |
| `units` | `aas-*` user unit이 `aas-maintain.service`와 enabled·active인 `aas-maintain.timer`뿐(`systemctl --user list-unit-files`와 `list-units --all`) |
| `collectors` | `jobs.enabled`이고 `--expect-provider`로 이름 붙인 공급자 절이 모두 켜짐 |
| `install_receipt` | 설치 receipt가 있음. 실행 환경과의 차이는 보고만 한다 |
| `maintain_run` | 이 설치본의 마지막 `aas maintain run` 보고가 `succeeded` |
| `legacy` | `--legacy-manifest`마다 정확한 bytes가 `raw/`에 있고(`retained`), 같은 bytes의 `--legacy-verify` 보고가 `mode: verify`·`complete: true`이고 그 보고의 원천이 모두 `committed`·`retired`이며 이 설치본에 끝난 commit으로 남아 있고(`verified`), 그 항목 경로가 모두 없으며, `--removed` 경로가 모두 없음 |

보고는 이 밖에 `source_retirements`를 백업·operation·이유별로 묶은 원천·행 수와 `raw/`의 파일 수·bytes를
싣는다. `apply`는 첫 unit 전에 manifest를 `raw/`에 남기므로 `retained`만으로는 편입이 끝났다는 증명이 아니고,
끝난 편입의 증명은 `verified`다. 모든 항목이 통과하면 종료 코드 0이고, `--record`는 이름 붙인 verify 보고의
정확한 bytes를 `raw/`에 남긴 뒤 그 보고를 `aas-cutover-record-v1` 은퇴 기록으로 정확한 bytes를 `raw/`와
`<runtime>/cutover-record.json`에 남기고 그 SHA-256을 `record_sha256`으로 돌려준다. 확인에서 백업에 없는 storage
operation이 생겨 `backup`의 `operations_after_backup`이 0이 아니면(사이에 예약 실행이 돈 경우) 백업과 확인을 다시
실행한다.
하나라도 실패하면 `failed`에 항목 이름을 담아 종료 코드 1로 끝나고 아무것도 기록하지 않는다. 백업 대조는
백업 전체를 다시 읽으므로 설치본 잠금을 잡기 전에 한다. 확인이 통과하면 `$TS-retire`와 `$TS-post`를 남기고
이전 백업(`$TS-v1`, `$TS-migrate`)을 지운다. 규칙은 [운영 전환 확인](design/data-vertical.md#운영-전환-확인)이
소유한다.

## 전환 중인 공급자 도구

`aas providers`와 기존 수집기는 유지되지만 일부는 아직 PostgreSQL/Parquet adapter를 쓴다.
이 경로는 `uv tool install '.[legacy]'` 또는 개발 환경과 명시한 이전 설정이 필요하다.
`storage.source_library.import_content_arrow`와 `import_arrow`의 Arrow 적재도 같은 추가 의존성의 PyArrow를 쓴다.
위 준비 실습은 그 의존성이 들어 있는 잠긴 개발 환경에서 확인했으며, 기본 설치만으로
같은 흐름이 도는지는 따로 검증하지 않았다. 의존성 목록은 `pyproject.toml`과 `uv.lock`이 정본이다.
기존 DB 명령은 `aas legacy-db`, publication 조회는 `aas legacy-data`로 구분한다.
이 도구를 새 `state.sqlite3`에 연결하거나 실제 보관 데이터를 자동 채택하지 않는다.
`docker-compose.data.yml`과 이전 설치 실행기는 이 전환 경로이며 새 설치 절차가 아니다.

라이브 공급자 검증·기존 DB 이전·스케줄러 활성화·실주문은 위 오프라인 설치 검사의 범위에
포함되지 않는다. CLI preview는 합성 비중 계산이고, `prepare`와 `backtest`는 저장한 입력의
준비와 명시한 봉투의 회계까지다. 그 둘을 이어 run으로 확정하고 다시 읽는 것은 `aas run`이며
`aas db run-install`이 필요하다. 저장한 run은 `aas db backup`이 함께 담고
`aas db restore`가 존재하지 않는 새 home에 되살린다. 복원한 run은 입력 pin·결과와 artifact
해시·표 행 수와 내용·판정 상태가 원본과 같다.

## 설치한 wheel 검증

`scripts/verify-lane-build`는 wheel을 만들어 깨끗한 환경에 설치한 뒤
`scripts/verify_installed_scenario.py`로 제품 경로 전체를 돌린다. 합성 전략 둘을 등록해
두 사례를 CLI로 실행하고, 그중 첫 사례는 Python API로도 실행해 두 경로가 같은 결과를
내는지 대조한다. 준비에 쓴 입력 문서를 모두 지우고 다른 작업
디렉터리에서 재실행·재조회한다. 그 다음 설치본을 백업해 새 home에 복원하고 기록한 run을
하나씩 대조하며, 기존 home 대상 복원과 손상·누락 백업이 거부되는지 확인한다. 마지막으로
실제 run 둘을 커밋 순서의 정확한 지점에서 중단시켜 복구시킨다.

증거 경계는 이렇다. 기본 설치만 검사하며 `legacy` 추가 의존성은 돌리지 않는다. 합성
입력 문서는 저장소의 생성기가 만들고, 등록부터는 설치한 실행 파일만 쓴다. 후보 revision과
wheel·lock 해시, Python 버전, 설치 extra, import 경로는 작업 기록에 남는다. 이 검사가
통과했다는 것이 실행 자격이나 실거래 승인을 뜻하지는 않는다.
