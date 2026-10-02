# 데이터 수직 계약

[0017](../decisions/0017-data-vertical-contract.md)에 따른 **구현 대상 계약**이다. 원천 자료실에
보존한 공급자 자료를 시점 조건이 있는 typed generation으로 승격하고, 소비자가 exact pin으로
읽고, 대체된 원천을 증명과 함께 은퇴시키는 경로 전체를 이 문서가 소유한다.
테이블의 물리 DDL은 schema 파일이, 공통 저장·게시·복구 규칙은
[로컬 DB 설계](backtest-data-foundation.md)가, 실행 명령은 [operations](../operations.md)가 소유한다.

이 문서의 계약은 하나씩 [계약과 테스트 대응표](#계약과-테스트-대응표)의 행으로 고정된다.
행의 상태가 `구현`이면 이름이 적힌 테스트가 저장소에 있고 그 계약을 검증한다. `예정`이면 그 계약을
구현하는 변경이 같은 이름의 테스트를 추가하고 같은 변경에서 상태를 `구현`으로 바꾼다.
`예정` 계약은 설계이며 설치본의 동작이 아니다.

## 층과 소유자

| 층 | 내용 | 소유 코드 |
| --- | --- | --- |
| L0 raw | 공급자 응답·내보내기 원본 bytes. 해시 경로, no-clobber | `storage/raw.py` |
| L1 원천 자료실 | `sl_*` 테이블. 원래 열·값·행 순서를 그대로 보존하는 `source_only` 자료 | `storage/source_library*.py` |
| L2 승격 | 승격 명세 → 매퍼 → head 비교 → 품질 flag → generation 게시 | `storage/promotion/` |
| L3 저장 | `market.duckdb`의 typed generation과 `state.sqlite3`의 카탈로그·identity·품질·수집 기록 | `storage/market.py`, `storage/state.py`, `storage/identity.py` |
| L4 소비 | exact pin, cutoff, grant로 head를 투영하는 reader와 실행 준비 | `storage/market_inputs.py`, `application/backtest_prepare.py` |

원천 자료실에 있다는 사실은 PIT 자격이 아니다. 시점·식별·품질 규칙을 명시한 승격만 L1 자료를
L3 generation으로 만든다. 13F 보유 내역, 애널리스트 추정치, 연구 산출물처럼 승격 대상 도메인이
정해지지 않은 자료는 `source_only`로 남고 원천 자료실 reader로 조회한다.

## 원천 자료실의 내용 정체성

원천 ID는 내용에서 나온다.

```text
source_id = <provider>-<shape>-<hex>
hex = sha256(정규 JSON ["aas-source-id-v1", 출력 schema major, [[상대 경로, 크기, SHA-256], ...]])
```

- 원천 바이트 manifest는 적재에 쓴 원본 파일의 `[상대 경로, 크기, SHA-256]` 목록이다. 상대 경로는
  `raw/` 안의 해시 주소(`SHA-256 앞 두 글자/SHA-256`)이고, 목록은 상대 경로 순이며 같은 bytes는
  한 항목이다. 따라서 원본 파일의 원래 이름·위치·적재 순서는 ID를 바꾸지 않는다. `hex`는 소문자
  16진수 64자 전체이고, 정규 JSON은 키 정렬·공백 없는 구분자·ASCII escape다.
- 원천 ID 하나는 파일 묶음 하나를 가리키므로 묶음 경계도 정체성의 일부다. 적재기는 경계가 원본
  bytes로 정해지는 완결 단위 하나마다 commit 하나를 만든다. 예를 들어 수집 job 하나의
  `complete.json`과 그것이 나열한 파일이 한 단위이고, 단위가 여럿인 원본 manifest 하나 전체도 한
  단위가 될 수 있다. 적재 코드가 정한 batch 크기로 단위를 묶어 commit하지 않는다. 이 규칙을 지키면
  같은 파일을 다른 실행 순서나 batch로 다시 적재해도 같은 ID 집합이 나온다. 묶음을 바꾸는
  적재기는 바뀐 묶음마다 새 ID를 만든다. 메모리에 맞춘 작업 크기 조정은 한 commit 안에서
  reader batch로 한다.
- `provider`는 소문자·숫자, `shape`는 하이픈으로 이은 소문자·숫자 단어이며 ID 전체는 240자 이하다.
  같은 원본에서 나온 두 테이블(가격 행과 격리 행)은 `shape`로 구분되고 `hex`를 공유한다.
- 출력 schema major는 적재기가 만드는 열 집합과 타입이 바뀔 때만 오른다.
- 적재 코드와 변환의 해시는 ID에도 요청 해시에도 넣지 않고 commit manifest의
  `metadata.lineage`에 기록한다. 코드만 바뀌고 같은 단위의 원본이 같으면 같은 ID로 재사용되며 처음 적재한
  lineage가 남는다. 재사용 때 적재 결과의 행 digest가 기록과 다르거나 Arrow schema가 다르면
  거부한다. 출력이 바뀐 적재기는 major를 올려 새 원천이 된다. 원본이 바뀌면 새 ID가 나온다.
- 원본 파일은 적재 전에 모두 `raw/`에 있어야 하며 적재는 각 파일의 크기와 해시를 다시 확인한다.
  ID 문서의 정확한 bytes도 `raw/`에 보존하므로 commit의 `source_sha256`(= `hex`)은 그 문서로
  풀린다. 진입점은 `storage.source_library.import_content_arrow`다.
- 원천 자료실 commit마다 state에 `source_snapshots` 한 행(`provider='source-library'`,
  `source_snapshot_id='sl:'+source_id`, `status='raw_verified'`)과 원본 파일별 `source_files` 행을
  남긴다. 새 테이블은 필요 없다. 이 연결이 승격 행의 `source_snapshot_id`가 가리키는 대상이다.
  연결의 `requested_at_us`와 `retrieved_at_us`는 그 commit의 `source_import` intent가 만들어지고
  완료된 시각이고, `publication_at_us`는 null이다. 연결은 commit marker, 완료된 intent와 `raw/`만으로
  다시 만들어지며 새 시계 값을 쓰지 않는다.
- 명시 ID 경로(`import_arrow`, `db source-import`)는 내용 ID 이전에 정해진 ID를 그대로 쓴다. 그
  경로의 metadata는 `aas-source-id-v1` 내용 정체성을 주장할 수 없고, 그 주장은 적재 전에 거부된다.
  내용 ID 이전에 명시 ID로 만든 commit은 그 ID를 유지한다.
  그 연결의 원본 파일은 intent가 pin한 `source_sha256`의 `raw/` 객체 하나다. 그 객체가 `raw/`에
  없으면 연결하지 않고 `unbacked`로 보고한다. 완료되지 않은 intent의 commit은 `incomplete`로
  보고하고 완료된 뒤 연결한다. 내용 commit은 원본 파일과 ID 문서가 모두 `raw/`에 있어야 연결된다.
- 적재·재사용·복구는 끝에서 연결을 함께 기록한다. intent 완료와 연결 기록 사이에서 멈춘 내용
  commit은 `aas db recover`가 연결한다. 연결이 없는 명시 ID commit은
  `aas db source-link --plan|--apply`가 같은 규칙으로 만든다. 같은 commit을 다시 연결하면 아무것도
  바뀌지 않고, 기록된 연결이 유도한 연결과 다르면 그 commit을 `invalid`로 보고한다. `raw/`의 bytes가
  pin과 다른 commit은 `corrupt`로 보고한다. 한 commit의 실패는 나머지 commit의 연결을 막지 않는다.
- `aas db verify`는 살아 있는 commit의 연결 행이 유도와 같은지 확인하고, 연결 파일의 bytes는 다른
  `source_files`처럼 `raw/`에서 다시 해시한다. 완료된 내용 commit은 연결과 `raw/`의 ID 문서가 모두
  있어야 통과한다. 연결이 하나라도 있으면 보고에 `source_library.linked`가 나오고, 연결이 없는
  설치본의 보고 형태는 연결 도입 전과 같아 그 전에 만든 백업도 복원 대조를 통과한다.

## 승격 명세 `aas-promotion-v1`

승격은 해시로 고정한 명세 문서 하나로 요청한다. 명세의 정확한 bytes는 `raw/`에 보존하고
SHA-256을 호출자가 함께 준다. 64 MiB 크기 상한은 명세 문서에만 적용되며 승격되는 행은
DuckDB 안에서 흐르고 Python으로 통째로 올라오지 않는다. 엄격한 UTF-8 JSON이며 알 수 없는
필드·중복 키·`latest` 같은 이동하는 참조는 거부한다.

| 필드 | 의미 |
| --- | --- |
| `schema_version` | `aas-promotion-v1` |
| `target` | `domain`, `dataset_id`, `parent`(직전 generation ID, 첫 generation은 null) |
| `sources` | 순서 있는 원천 pin 목록. 각 항목은 `source_id`, `source_sha256`, `table`, `digest`(commit manifest의 테이블 digest) |
| `mapper` | `name@major` |
| `time_rules` | `available_at_us`, `revision_known_at_us` 각각의 `id@version`, 입력 근거(`revision`·`record`), 그 규칙의 인자 |
| `decimal_rule` | 숫자 열별 `id@version` |
| `quality_rules` | 적용할 품질 규칙 `id@version` 목록 |
| `tombstone_policy` | `never`, 또는 `absent_in_full_snapshot`과 그 원천이 빠짐없이 담는 범위(`scope`: instrument 집합과 날짜 구간) |
| `identity_snapshot` | instrument·issuer를 해석한 identity snapshot pin. instrument가 없는 도메인(거시·FX·달력)은 null |

`request_hash = sha256(정규 JSON ["aas-promotion-request-v1", 명세 SHA-256, 원천 digest 목록, parent])`다.
같은 요청은 기존 operation과 generation을 그대로 돌려준다. 같은 parent에 다른 요청이 먼저
게시됐으면 부모 CAS가 실패하고 호출자가 새 parent로 다시 계획한다.

명세의 해시는 `dataset_versions.transform_hash`가 된다. 따라서 매퍼 major, 시간 규칙 버전,
숫자 규칙 버전이 바뀌면 transform hash도 바뀐다. 게시된 generation은 자기 명세를 raw 증거로
다시 읽어 검증할 수 있다.

`aas data promote --spec FILE --sha256 SHA256 [--plan]`이 실행 진입점이다. 순서는 원천 stage →
매퍼 → head 비교 → quality flag → 대량 게시 → state 카탈로그(`dataset_versions`,
`dataset_sources`, `quality_checks`, `watermarks`) → 완료다. `--plan`은 같은 계산을 하고 행 수·
op 분포·flag 분포만 보고하며 아무것도 쓰지 않는다. 중단되면 `aas db recover`가 게시 단계만
재개한다. 승격은 공급자를 호출하지 않는다.

## 매퍼

매퍼는 원천 relation을 받아 도메인 열과 `source_row_hash`를 가진 DuckDB relation을 돌려주는
순수 함수다. 네트워크·현재 시각·난수·환경 변수를 읽지 않는다. 이름은 `<provider>.<shape>`,
버전은 major 하나다. 같은 입력에 대한 출력이 한 행이라도 달라지면 major가 오른다.
매퍼마다 합성 원천 fixture와 독립 기대값으로 검증한다.

`source_row_hash`는 원천 행 내용의 해시다.

```text
source_row_hash = sha256(정규 JSON ["aas-source-row-v1", [[열 이름, 값], ...]])
```

열은 원천 schema 순서이며 원천 자료실의 내부 열(`_aas_ordinal`)은 제외한다. 값의 표현은 원천
자료실 digest와 같다(`source_library_digest.scalar`: float은 `float_hex`, bytes는 base64).
원본 값은 숫자 규칙이 바꾼 뒤에도 이 해시와 원천 자료실에 그대로 남는다.

매퍼 목록: `norgate.prices_none`, `norgate.prices_adjusted`, `norgate.master`,
`norgate.dividends`, `norgate.index_membership`, `norgate.reference_series`, `eodhd.bars`,
`fmp.profile`, `fmp.actions`, `sec.submissions`, `sec.companyfacts`, `dart.corp_codes`,
`dart.fnltt`, `dart.list`, `kind.listings`, `fred.alfred`, `fx.series`, `calendar.declared`.

자연키가 겹치는 원천 행 두 개는 매퍼가 거부한다. 어느 쪽을 고를지 추정하지 않는다.
identity snapshot으로 instrument를 해석하지 못한 행은 승격하지 않고 수와 원천 키를 미해결
보고에 남긴다. 티커·경로·날짜로 instrument ID를 만들지 않는다.

## 결정적 공통 열

공통 열은 원천과 공개된 규칙만으로 재현된다. 값을 꾸며내지 않는다.

| 열 | 규칙 |
| --- | --- |
| `record_id` | 기존 `aas-record-v1`: `sha256(정규 JSON ["aas-record-v1", domain, 자연키 [이름, 값] 목록])` (`market.normalize_rows`) |
| `op` | parent chain의 현재 head와 비교해 정한다(아래) |
| `supersedes_revision_id` | head 조인에서 온다. ASSERT만 null |
| `revision_id` | `sha256(정규 JSON ["aas-revision-v1", dataset_id, record_id, op, supersedes_revision_id, source_row_hash])` |
| `available_at_us`, `revision_known_at_us` | [시간 규칙](#시간-규칙과-소비자-grant)과 그 절의 revision 시점 규칙으로 정한다. 규칙이 없으면 null |
| `ingested_at_us` | 원천 행의 수집 시각 열, 없으면 원천 snapshot의 수집 시각, 그것도 없으면 원천 자료실 commit manifest의 적재 시각 |
| `source_snapshot_id` | `'sl:' + source_id` |
| `source_row_hash` | 위 매퍼 규칙 |

`revision_id`는 직전 revision을 포함하므로 같은 record에서 A→B→A로 값이 돌아와도 세 revision의
ID가 모두 다르다. dataset ID도 포함하므로 같은 원천 행을 다른 dataset(`.ref`, 시간 규칙 세대 `.r<N>`)에
승격해도 시장 테이블의 `UNIQUE(record_id, revision_id)`와 충돌하지 않는다. 승격 시각은 어떤 열에도 들어가지 않으므로 같은 명세를 다른 날 다시 실행해도
같은 행이 나온다.

op는 원천 행과 head를 도메인 열로만 비교해 정한다. 자연키가 아닌 revision 속성(재무의
accession·`accepted_at`, 거시의 vintage 구간, `value_state`)도 도메인 열이다. 두 시점 열,
`source_row_hash`, `ingested_at_us`는 비교에 넣지 않는다. 같은 값을 다른 시각에 다시 수집해도
새 revision이 생기지 않고 head의 시점이 그대로 남는다. 시점만 다른 원천 행은 revision이 아니다.

| head | 원천 | 결과 |
| --- | --- | --- |
| 없음 | 있음 | ASSERT |
| ASSERT·SUPERSEDE, 비교 값 같음 | 있음 | 행 없음(멱등) |
| ASSERT·SUPERSEDE, 비교 값 다름 | 있음 | SUPERSEDE |
| TOMBSTONE | 있음 | SUPERSEDE(재등장) |
| ASSERT·SUPERSEDE | 없음, `absent_in_full_snapshot`이고 record가 명세의 `scope` 안 | TOMBSTONE |
| ASSERT·SUPERSEDE | 없음, 그 밖의 경우 | 행 없음 |

TOMBSTONE의 `source_row_hash`는 `sha256(정규 JSON ["aas-tombstone-v1", source_id, table, digest])`,
곧 그 행이 없다는 사실을 담은 원천 테이블의 증거다. TOMBSTONE의 시점은 아래
[revision 시점](#revision-시점)이 정한다. 같은 명세를 두 번 승격하면 두 번째 delta는 비어 있다.

`revision_id`, `source_row_hash`, TOMBSTONE 해시, `request_hash`, `source_id`의 형식은 고정 입력과
기대 16진수 값으로 각각 고정한다. 형식을 바꾸려면 새 이름(`-v2`)을 쓴다. 매퍼가 DuckDB SQL로 내는
`source_row_hash`는 같은 행의 Python 계산과 같아야 한다.

## 시간 규칙과 소비자 grant

날짜 단위로만 공개 시점을 알 수 있는 원천은 버전 있는 시간 규칙으로 `available_at_us`와
`revision_known_at_us`를 정한다. 규칙 값은 그 규칙의 전제 아래에서 실제 공개 시각보다 이르지
않은 **보수적 상한**이다. 수집 시각으로 빈 시점을 채우지 않는다.

| 규칙 | 계산 | 물리 기준 | 사용처 |
| --- | --- | --- | --- |
| `source_column@1` | 원천의 시각 열을 UTC microsecond로 그대로 사용(SEC `acceptanceDateTime` 등) | 계산 값 | 시각을 직접 싣는 원천 |
| `session_close_plus_lag@1` | pin한 `calendar_sessions` generation의 해당 세션 `close_at_us` + 명세의 `lag_us`(0 이상). 세션이 없거나 종료 시각이 알려지지 않았으면 null | 세션 `close_at_us` | 일봉 가격 |
| `local_day_end@1` | 명세의 IANA 시간대에서 그 날짜의 23:59:59.999999를 UTC로 변환. 행에 `time_precision_day` flag | 그 날짜의 현지 0시 | 날짜만 있는 공시·거시 vintage(`realtime_start`) |
| `exdate_open@1` | pin한 달력에서 ex-date 세션의 `open_at_us`. 세션이 없으면 null | 계산 값 | 기업행동 |
| `unknown_null@1` | 항상 null | 없음 | 근거가 없는 원천 |

기록되는 시점은 그 행을 받은 시각보다 늦을 수 없다. 받은 bytes는 받은 시각에 이미 공개돼 있었기 때문이다.
그래서 행의 `ingested_at_us`가 규칙의 물리 기준 이상이고 규칙 값보다 이르면 시점은 `ingested_at_us`로
내려가고 행에 `time_clamped_to_ingestion` flag가 남는다. 이 값도 상한이다. `ingested_at_us`가 물리
기준보다 이르면, 예를 들어 세션 종료 전에 받은 일봉이면, 그 행은 승격하지 않고 보류 행으로 보고한다.
수집 시각은 시점을 낮추는 상한으로만 쓰이며 null을 채우지 않는다.

명세는 시점 열마다 규칙 입력의 근거(`basis`)를 선언한다. `revision`은 입력 열이 그 행 자체의 공개
시각이나 공개일을 담는 경우다(SEC `acceptanceDateTime`, ALFRED `realtime_start`, DART 접수일).
`record`는 입력이 record의 날짜인 경우다(세션 날짜, ex-date). `session_close_plus_lag@1`과
`exdate_open@1`은 항상 `record`다.

규칙 ID와 버전은 명세에 있으므로 transform hash에 포함된다. 규칙의 계산을 바꾸면 새 버전이 된다.
한 chain의 모든 generation은 열마다 같은 시간 규칙(ID·버전·근거·인자)을 쓴다. op는 시점을 비교하지
않으므로 규칙이 바뀐 명세로 이어 승격하면 이전 규칙의 시점이 새 규칙의 것처럼 남는다. 그래서 parent
명세와 시간 규칙이 다른 승격은 거부한다. 규칙을 바꾸려면 [dataset 이름](#공급자별-dataset과-ordered-pin-cutover)에
규칙 세대 `.r<N>`을 붙인 새 dataset을 첫 generation부터 승격한다. reader는 행이 속한 chain의 명세에서
그 행의 시점이 어느 규칙에서 왔는지 안다.

`source_column`이 아닌 규칙에서 나온 시점을 strict PIT 경로에 쓸지는 소비자가 정한다.
입력 binding이 허용하는 규칙 `id@version` 목록(grant)을 들고, 그 목록은 bundle hash에 포함된다.
strict reader는 grant에 없는 규칙의 시점을 알 수 없는 시점으로 취급해 그 행을 고르지 않는다.
inspection과 연구 모드는 grant 없이도 행을 읽는다. run 영수증은 사용한 grant를 그대로 기록한다.
규칙 하나를 허용하는 일은 그 규칙의 상한을 시점 근거로 받아들인다는 기록이며, 다른 규칙이나
알 수 없는 시점을 허용하지 않는다.

### revision 시점

ASSERT가 아닌 행은 정정이나 삭제를 담은 원천보다 먼저 알려질 수 없다. 원천의 **증거 시각**은 행이
있으면 그 행의 `ingested_at_us`이고, 행이 없는 TOMBSTONE에서는 원천 snapshot 수집 시각 중 가장 늦은
값(없으면 commit manifest의 적재 시각)이다. TOMBSTONE의 `ingested_at_us`도 이 증거 시각이다.
두 시점 열은 열마다 다음과 같다.

| op | 시점 |
| --- | --- |
| ASSERT | 규칙 값(위 상한 적용) |
| SUPERSEDE, 근거 `revision` | 규칙 값(위 상한 적용) |
| SUPERSEDE, 근거 `record` | 원천 증거 시각. 규칙 값이 null이면 null |
| TOMBSTONE | 원천 증거 시각. record 날짜의 규칙은 쓰지 않는다 |

`record` 근거 규칙은 record의 날짜에서 계산하므로 정정이 언제 공개됐는지 알려 주지 않는다. 그 정정은
AAS가 정정 bytes를 받은 시각부터 알려진 것으로 본다. 실제 공개는 그보다 이를 수 있으므로 이 값도
보수적 상한이며, 행을 받은 시각을 넘지 않는다. `revision` 근거의 정정은 원천이 기록한 그 정정의
공개 시각을 쓴다.

새 revision의 시점이 직전 revision의 같은 열보다 이르면 그 원천 행은 head보다 오래된 관측이다.
승격은 그 행으로 head를 대체하지 않고 stale 행으로 보고한다. 그래서 원천을 수집 순서와 다르게
승격해도 시점이 거꾸로 가지 않는다.

## 숫자 규칙

원천 값이 `DECIMAL(38,12)`로 정확히 표현되지 않을 때 쓰는 변환은 명세가 열마다 이름으로
선언한다. 규칙이 원본을 바꾸면 행에 flag가 남고 원본은 `source_row_hash`와 원천 자료실에 있다.

| 규칙 | 계산 | flag |
| --- | --- | --- |
| `exact@1` | 원천 값이 정확히 표현될 때만 승인. 아니면 승격 거부 | 없음 |
| `krw_tick@1` | binary64 원천 값의 정확한 십진 전개를 원 단위 정수로 반올림(ROUND_HALF_EVEN). KRW 가격 열(open·high·low·close)에만 적용 | 정수가 아니던 행 `provider_float_reconstructed`, 나머지가 정확히 0.5이던 행 `decimal_rounding_tie` |
| `float_shortest@1` | 원천 저장 폭(float32 또는 float64)에서 왕복하는 가장 짧은 십진 표현. 소수 12자리를 넘으면 소수 12자리로 ROUND_HALF_EVEN | `provider_float_storage`, 버린 나머지가 정확히 절반이면 `decimal_rounding_tie` |
| `decimal_text@1` | 원천 텍스트의 십진 값을 그대로 사용. 지수 표기·유효숫자 7자리 이하이면 flag | `volume_precision_limited` |

`krw_tick@1`은 원화의 최소 화폐 단위로 맞추는 규칙이다. 시기마다 달랐던 거래소 호가 단위표는
적용하지 않는다. 공급자가 분할 이전 가격을 조정 계수로 다시 곱해 만든 값에는 float 잔재가
남으며, 반올림은 그 잔재를 원 단위로 되돌린다. 나머지가 정확히 0.5인 값은 두 이웃 정수가 똑같이
그럴듯하므로 짝수 쪽을 고르고 그 사실을 flag로 남긴다. DuckDB `round`(0.5에서 0에서 먼 쪽)와
Python `ROUND_HALF_EVEN`이 다르므로 구현은 SQL과 Python 결과의 parity를 검증한다.
모든 숫자 규칙은 결과가 `DECIMAL(38,12)` 범위(정수부 26자리)를 넘거나 원천 값이 NaN·무한대이면 그
승격을 거부한다.

## 품질 flag와 품질 검사

`quality_flags`(market v2)는 revision 단위 기록이다.

```text
quality_flags(generation_id, record_id, revision_id, rule_id, rule_version, flag, detail)
```

- flag는 값을 바꾸지 않는다. 원래 승격된 값과 flag가 함께 남는다.
- 같은 generation의 flag 행은 `aas-rowset-v1`으로 따로 해시해 그 generation의 manifest에 기록한다.
  flag를 추가·삭제하면 generation 검증이 실패한다. flag 정정은 새 generation이다.
- reader는 flag를 행과 함께 돌려준다. 소비자는 flag를 grant와 같은 방식으로 제외 목록에 둘 수 있고,
  그 목록도 bundle hash와 run 영수증에 들어간다.

| flag | 의미 |
| --- | --- |
| `provider_float_reconstructed` | 원화 가격이 정수가 아니어서 `krw_tick@1`으로 반올림함 |
| `decimal_rounding_tie` | 반올림 나머지가 정확히 0.5였음 |
| `provider_float_storage` | 원천이 float 저장값이어서 `float_shortest@1`을 적용함 |
| `time_clamped_to_ingestion` | 규칙 상한이 수집 시각보다 늦어 수집 시각으로 내려감 |
| `provider_reported_partial` | 공급자가 경고와 함께 보낸 부분 응답 파티션의 행 |
| `volume_precision_limited` | 원천 거래량의 유효숫자가 잘려 있음 |
| `time_precision_day` | 시점이 `local_day_end@1`의 날짜 단위 상한임 |
| `cross_provider_mismatch` | 같은 instrument·세션의 다른 공급자 값과 명세 품질 규칙의 허용오차를 넘게 다름. 허용오차 안이면 flag가 없다 |

dataset version 단위의 판정(행 수 대조, coverage 종료, 교차 대조율)은 기존 state
`quality_checks`가 맡는다. 부분 응답 파티션은 행마다 flag를 달고, 같은 파티션의 다른 원천과
행 수를 대조한 결과를 `quality_checks`에 남긴다. 잘못된 가격은 `value_state='invalid'`로
승격하고 값을 0이나 이웃 값으로 채우지 않는다.

## 공급자별 dataset과 ordered-pin cutover

dataset 이름은 `<domain>.<market>.<provider>[.ref][.r<N>]`다. 한 dataset의 chain에는 한 공급자의 자료만
들어간다. `.r<N>`은 시간 규칙 세대다. 첫 세대는 접미사가 없고, 시간 규칙을 바꿀 때마다 N이 2부터 오른다.
이전 세대 dataset은 그대로 남고 소비자는 새 binding으로 옮긴다. 공급자의 정정은 그 공급자의 chain 안에서 SUPERSEDE로 남고 다른 공급자의 이력을 바꾸지
않는다. `.ref`는 `price_role='reference'` 자료(공급자 조정 가격, 기준 지수)다.

여러 공급자를 잇는 일은 소비자의 binding이 한다. 한 역할이 순서 있는 pin 목록을 들고 각 pin은
`[from, to)` 날짜 구간을 가진다.

- 구간은 겹치지 않고 순서대로 이어진다. 각 날짜에는 정확히 한 pin만 적용된다.
- 구간 밖의 날짜는 다른 pin으로 대체하지 않고 coverage에서 누락으로 보고한다.
- 같은 날짜에 두 공급자 값을 섞거나 평균내지 않는다.
- cutover 날짜와 pin 목록은 bundle hash에 포함된다. 공급자를 바꾸려면 새 binding을 만든다.

FX처럼 여러 원천이 같은 시계열을 내는 경우에도 우선순위는 같은 방식으로 소비자 pin이 정한다.

## 동결 원천

구독이 끝났거나 공급이 멈춘 원천은 동결 원천이다. 보존한 원문과 원천 자료실 자료로만 generation을
만들고, coverage 종료일을 `quality_checks`에 기록한다.

- FMP는 동결 원천이다. FMP 자료는 `*.fmp.ref` dataset의 `reference` 역할로만 승격하며 canonical
  가격이나 strict 실행 입력이 되지 않는다. 수집기의 새 호출은 없다.
- Norgate의 지수 구성, 기준 시리즈, 기업행동은 마지막 내보내기 날짜에서 coverage가 끝난다.
  이후 구간은 다른 공급자의 dataset이 cutover로 잇는다.

## 대상 dataset

| dataset | 도메인·역할 | 원천과 규칙 |
| --- | --- | --- |
| `prices.kr.eodhd` | `prices`, canonical unadjusted | EODHD KR 일봉 이력과 이후 일간 수집. `krw_tick@1`, `session_close_plus_lag@1`, 부분 응답 flag, 잘못된 가격은 `invalid` |
| `prices.kr.eodhd.ref` | `prices`, reference `total_return` | 같은 원천의 adjusted close |
| `prices.us.norgate` | `prices`, canonical unadjusted | Norgate 비조정 일봉 내보내기(CSV). 거래량은 `decimal_text@1` |
| `prices.us.norgate.ref` | `prices`, reference `split_adjusted`·`total_return` | Norgate 조정 OHLC. float32 저장값은 `float_shortest@1` |
| `prices.us.eodhd` | `prices`, canonical unadjusted | EODHD US 일간 수집. Norgate와 겹치는 구간에 교차 대조 flag |
| `prices.us.fmp.ref` | `prices`, reference | FMP 동결 snapshot |
| `prices.ref.norgate` | `prices`, reference, `fields='close'` | Norgate 기준 시리즈·지수 |
| `sessions.xnys`, `sessions.xkrx` | `calendar_sessions` | 관측 거래일과 선언 문서. 임시 휴장은 SUPERSEDE |
| `actions.us.norgate`, `actions.us.fmp.ref`, `actions.{us,kr}.eodhd` | `corporate_actions` | `exdate_open@1` |
| `status.us.norgate`, `status.kr.kind` | `instrument_status` | 상장·상폐 이력 |
| `filings.us.sec`, `filings.kr.dart` | `filings`(v2) | SEC submissions(`source_column@1`), DART 공시 목록(`local_day_end@1`) |
| `fundamentals.us.sec` | `fundamentals` | companyfacts. 시점은 accession으로 조인한 `filings.accepted_at_us`, 조인 실패 시 `local_day_end(filed)` 또는 null. 재공시는 SUPERSEDE |
| `fundamentals.kr.dart` | `fundamentals` | 재무제표 응답. 연결·별도는 dimensions. 자료 없음 응답은 행 대신 coverage 기록 |
| `macro.us.alfred` | `macro_observations` | ALFRED vintage. known은 `local_day_end@1(realtime_start)` |
| `macro.kr.bok`, `macro.kr.oecd` | `macro_observations` | vintage가 없어 known은 null |
| `fx.usdkrw.norgate`, `fx.usdkrw.fred` | `fx_rates` | 우선순위는 소비자 pin |
| `classifications.*` | `classifications`(v2) | Norgate 분류, SEC SIC, KIND 업종. known은 snapshot 시각이며 과거로 소급하지 않음 |

identity는 `storage/identity.py`가 등록한다. issuer는 미국 CIK, 한국 DART 고유번호에서,
instrument는 `mint_instrument(anchor_namespace, token)`으로 Norgate asset ID나 KRX ISIN 같은
영구 anchor에서 만든다. 티커·경로·날짜는 anchor가 될 수 없다. ETF처럼 issuer가 없는 상품은
issuer null이다. 티커·EODHD 심볼·CUSIP·ISIN은 유효·지식 구간을 가진 `identity_assertions`다.
identity와 universe 문서가 커지면 1 MiB 이하 part로 나누고 part 해시 manifest로 묶는다.

dataset의 백필은 연도 단위 generation, 이후 유지보수는 세션 단위(재무는 일 단위) generation으로
게시한다.

## 대량 게시와 reader

- `storage/bulk_generation.py`가 대량 게시를 소유한다. 입력은 연결에 보이는 staging 테이블 하나이며,
  `generation_id`를 뺀 도메인의 typed 열(`record_id` 포함, 가격은 선택적으로 `fields`)을 정확히 같은
  타입으로 가진다. `plan_generation_bulk`는 아무것도 쓰지 않고 marker를 계산하며,
  `publish_generation_bulk`는 DuckDB 트랜잭션 하나에서 marker와 `INSERT … SELECT` 행을 함께 쓴다.
- delta hash는 기존 `aas-rowset-v1`과 바이트까지 같다. DuckDB가 각 행을 `aas-rowset-v1` 부호화로
  만들고 정렬하며, Python은 그 부호화를 배치로 받아 `rowset.RowsetStream`에 흘려보낸다. stream은 앞
  행보다 작은 행을 거부하므로 정렬 순서가 다르면 다른 hash가 아니라 오류가 된다. 그래서 대량
  generation은 `market.verify_generation`으로, Python 경로 generation은 대량 검증으로 그대로 검증된다.
  새 해시 형식은 만들지 않는다.
- 행 규칙은 `market.normalize_rows`와 같다. SQL 집계 한 번으로 같은 규칙을 검사하고, `record_id`는 JSON
  이스케이프가 필요 없는 자연키를 SQL로, 나머지를 `market.record_identity`로 다시 계산한다.
- revision 규칙은 `market._validate_revisions`와 같고, delta record의 직전 head만 SQL로 비교한다.
- 게시는 트랜잭션 안에서 계획을 다시 계산한다. dataset head가 계획한 parent가 아니면
  `ParentChangedError`로 거부되고 호출자가 새 head에서 다시 계획한다. 검토한 계획을 넘기면 다시 계산한
  marker가 그 계획과 같아야 하며 다르면 `PlanChangedError`다. 같은 generation·operation ID의 기존
  marker는 내용이 정확히 같은 요청에만 재사용되고, 다른 요청은 남아 있는 generation을 채택하지 못한다.
- `verify_generation_bulk`는 chain의 모든 marker link를 기록된 hash로 다시 계산하고, 대상 generation의
  행만 다시 해시하며 그 앞 head에 대한 revision 규칙을 검사한다. `deep=True`는 모든 delta를 다시 해시한다.
- 메모리는 [0016](../decisions/0016-maintenance-admission-budget.md)을 따른다. fetch 전에 SQL 집계로
  가장 넓은 행을 재고, 배치 과금이 호출자 할당의 비DuckDB 몫에 들어가도록 배치 행 수를 정한다. 과금은
  행 수와 무관하다. 한 행도 들어가지 않으면 `ComputeResourceError`다. 정렬·spill·색인 유지는 같은
  할당에서 유도한 DuckDB 몫이 맡는다. 도메인 테이블의 PK·UNIQUE 색인은 삽입 중 메모리에 올라오므로
  DuckDB 몫은 그 테이블의 전체 행 수에 비례해 커진다. 한도를 넘으면 트랜잭션 전체가 취소되고
  `ComputeResourceError`가 된다.
- `read_heads(pins, domain, instruments?, date_range?, cutoffs, roles, granted_rules)`는
  `QUALIFY row_number()`로 head를 투영하며 기존 `project_heads`와 결과가 같다. ordered pin과
  cutover를 해석한다.
- 분할조정·총수익 가격은 reader가 unadjusted 가격과 cutoff 시점까지 알려진 `corporate_actions`로
  계산한다. 공급자 조정 가격은 reference로만 남는다.

## 스키마 v2

state와 market 저장소는 core schema 버전을 가진다. `aas init`은 새 저장소에 v1과 v2를 한 트랜잭션으로
적용하므로 새 설치본과 migration한 설치본은 같은 객체와 같은 영수증을 가진다. `aas db migrate --to 2
--backup-output DIR`이 v1 설치본을 v2로 올린다. `storage/migration.py`가 이 순서를 소유한다.

1. 설치 잠금을 잡는다. 실행 중인 run이나 다른 PREPARED 작업이 있으면 거부한다.
2. `DIR`에 백업을 만들고 검증한다. `DIR`이 없으면 진행하지 않는다.
3. state에 migration intent를 기록한다. intent의 payload hash는 백업 manifest(`backup.json`)의 SHA-256이다.
4. market DDL을 DuckDB 한 트랜잭션으로, state DDL을 SQLite 한 트랜잭션으로 적용한다. 각 트랜잭션은
   `schema_migrations`에 새 버전 행을 더하고 `store_info.schema_version`을 바꾼다. v1 행은 그대로 남아
   `(1, v1), (2, v2)`가 된다.
5. 설치 영수증(`installation.json`)의 저장소 버전을 바꾼다.
6. intent를 완료한다.

intent가 PREPARED인 설치본은 migration-incomplete다. 앱은 그 설치본을 읽기·쓰기 어느 쪽으로도 열지
않고, 같은 명령을 다시 실행하면 남은 단계부터 재개한다. 재개는 백업을 다시 만들지 않으며
`--backup-output`을 쓰지 않는다. intent 전에 멈추면 설치본은 v1 그대로이고 새 `DIR`로 다시 실행한다.
`aas db quarantine`은 이 intent를 끝내지 않는다. `--plan`은 저장소를 읽기 전용으로 열어 현재 버전,
인식한 checksum, 남은 단계를 보고하고 아무것도 쓰지 않는다.

`schema_migrations`의 이력은 1부터 빈틈없이 이어지고 각 행의 checksum은 그 버전 DDL의 SHA-256과
같아야 한다. 알 수 없는 버전, 빈틈, 다른 checksum은 거부한다. v1 checksum은 market
`ab2383d7cb1181e7b98e7dc054042f82024a6aafc0c0fabb27ee5f94dbe0db7c`, state
`da574cef54b69961c341e3e5e92ee16334a5049911ee6b417db643f44bd881dc`로 고정돼 있다. migration하지 않은
v1 설치본도 정상으로 열리며 v1 기능을 그대로 쓴다. v2 테이블이나 close 전용 가격을 쓰는 게시는 v1
설치본에서 `aas db migrate --to 2`를 안내하며 거부된다. v1 행은 그대로 옮겨져 읽히고 기록된 delta·chain
hash로 검증된다. run 추가 스키마 버전은 이 migration과 독립이다.

market v2:

- `filings`: `issuer_id`, `filing_id`(accession 또는 접수번호), `form`, `filed_date`, `accepted_at_us?`,
  `period_end?` + 공통 열. 자연키는 `(issuer_id, filing_id)`다. 공동 제출자가 같은 accession을 나눈다.
- `classifications`: `subject_id`, `subject_kind`, `scheme`, `code`, `label`, `effective_from`,
  `effective_to?` + 공통 열. 자연키는 `(subject_kind, subject_id, scheme, effective_from)`이고
  `effective_to`는 `effective_from`보다 늦다.
- `quality_flags`: 위 절의 열. `generation_id`는 존재하는 generation을 가리킨다.
- `prices.fields`: `ohlcv` 또는 `close`이고 기본값은 `ohlcv`다. `ohlcv`는 v1과 같은 제약을 지킨다.
  `close`는 close만 값을 갖고 open·high·low·volume이 null이며 `price_role='reference'`다. 기존 행은
  `ohlcv`다. DuckDB는 기존 테이블에 CHECK를 더하지 못하므로 migration은 `prices`를 다시 만들고 행을 옮긴다.
  `ohlcv` 행은 v1과 같은 모양으로 읽히고 해시된다. close 행이 하나라도 있는 generation은 모든 행의
  `fields`를 해시 schema에 넣는다. 그래서 v1 generation의 기록된 hash는 그대로 검증되고, close 행의
  `fields`를 바꾸면 검증이 실패한다.

state v2:

- `source_retirements`: `source_id`, `digest`, `rows`, `reason`, `equivalent_to_source_id`,
  `equivalence_spec`, `equivalence_digest`, `backup_id`, `operation_id`, `retired_at_us`. 원천마다 한 행이고
  불변이다. `operation_id`는 `storage_operations`의 작업을 가리키고 동치 원천은 자기 자신일 수 없다.

`conventions`, `authority_records`, 수집 기록 테이블은 v1 그대로 쓴다. 시간 규칙은 convention이
아니라 명세와 binding grant가 들고 다닌다.

## 원천 은퇴와 동치 증명

`aas db source-retire --plan|--apply`는 다른 원천으로 대체된 원천 자료실 테이블을 지운다.
`--apply`는 다음이 모두 성립하는 원천만 처리하고, 성립하지 않는 원천은 이유와 함께 보고한다.

1. **참조 없음**: committed generation 명세의 원천 pin, generation 행의 `source_snapshot_id`,
   `dataset_sources`, 입력 binding 중 어느 것도 그 원천을 가리키지 않는다. 원천 자신의
   source-link 행(`sl:` snapshot)은 참조로 세지 않는다. 그 행은 은퇴 뒤에도 계보로 남는다.
2. **동치 증명**: `equivalence_spec`이 비교할 테이블과 열을 명시하고, 그 열들의 정규 행 multiset에
   대한 `aas-rowset-v1` digest가 은퇴할 원천과 `equivalent_to_source_id`에서 같다.
3. **다른 장치 백업**: `backup_id`가 가리키는 검증된 백업이 그 원천을 담고 있고, 설치본과 다른 장치에 있다.

세 조건을 통과한 원천은 한 번의 실행에서 일괄 은퇴하고 `source_retirements`에 기록하며 결과를
보고한다. 기록은 불변이다. 은퇴는 원천 자료실 테이블을 지울 뿐 `raw/`의 원본 bytes와 보관 archive는
지우지 않는다. 은퇴한 원천의 내용은 동치 원천과 백업에서 다시 얻는다.

물리 공간 회수는 `aas db compact --to NEW_ROOT`가 새 루트로 복원하듯 옮기고 deep verify를 통과한 뒤
설정을 바꾼다. 원래 파일은 그 전까지 그대로 남는다.

## 계약과 테스트 대응표

| 계약 | 문장 | 테스트 | 상태 |
| --- | --- | --- | --- |
| DV-01 | `aas-rowset-v1` 형식과 빈 rowset 해시는 고정돼 있다 | `tests/storage/test_rowset.py::test_format_identity_and_empty_rowset_are_frozen` | 구현 |
| DV-02 | 공급자 조정 가격과 시점을 모르는 행은 strict 조회에서 빠진다 | `tests/storage/test_market.py::test_unknown_availability_and_adjusted_reference_excluded` | 구현 |
| DV-03 | 정확한 십진 열은 float 입력과 모호한 revision을 거부한다 | `tests/storage/test_market.py::test_ambiguous_revision_and_float_value_rejected` | 구현 |
| DV-04 | TOMBSTONE은 그 시점 이후 관측을 제거하고 재시작 후에도 같다 | `tests/storage/test_market.py::test_revision_replay_tombstone_and_restart` | 구현 |
| DV-05 | strict 투영은 이후 revision을 새지 않는다 | `tests/storage/test_market_inputs.py::test_complete_chain_projects_original_and_corrected_without_future_leak` | 구현 |
| DV-06 | 기록과 다른 schema checksum은 거부한다 | `tests/storage/test_sqlite.py::test_schema_checksum_mismatch_and_read_only` | 구현 |
| DV-07 | 원천 ID는 원본 bytes의 완결 단위에서 나오며 코드만 바뀌면 같은 ID를 재사용한다 | `tests/storage/test_source_identity.py::test_code_change_reuses_content_id` | 구현 |
| DV-08 | 원본 bytes가 바뀌면 새 원천 ID가 나온다 | `tests/storage/test_source_identity.py::test_content_change_mints_new_id` | 구현 |
| DV-09 | source-link는 멱등이며 `sl:` snapshot 행을 만든다 | `tests/storage/test_source_identity.py::test_source_link_is_idempotent` | 구현 |
| DV-10 | 승격 명세는 알 수 없는 필드·`latest`·해시 불일치를 거부한다 | `tests/storage/test_promotion_spec.py::test_spec_rejects_unknown_fields_and_moving_refs` | 예정 |
| DV-11 | 같은 승격 요청은 같은 generation을 재사용한다 | `tests/storage/test_promotion_engine.py::test_same_request_reuses_generation` | 예정 |
| DV-12 | 같은 명세의 재승격은 빈 delta다 | `tests/storage/test_promotion_engine.py::test_repromotion_yields_empty_delta` | 예정 |
| DV-13 | op는 head 비교로 ASSERT·SUPERSEDE·TOMBSTONE·skip을 정한다 | `tests/storage/test_promotion_engine.py::test_head_diff_decides_operation` | 예정 |
| DV-14 | TOMBSTONE은 전체 snapshot과 명세 허용이 있을 때만 생긴다 | `tests/storage/test_promotion_engine.py::test_tombstone_requires_full_snapshot_policy` | 예정 |
| DV-15 | `revision_id`는 직전 revision과 dataset을 포함해 A→B→A에서도, 같은 원천을 다른 dataset에 승격해도 유일하다 | `tests/storage/test_promotion_engine.py::test_revision_id_is_unique_across_value_return` | 예정 |
| DV-16 | 승격 시각은 어떤 열에도 들어가지 않는다 | `tests/storage/test_promotion_engine.py::test_promotion_is_independent_of_wall_clock` | 예정 |
| DV-17 | 승격 도중 중단은 게시 단계만 재개하고 공급자를 호출하지 않는다 | `tests/storage/test_promotion_engine.py::test_interrupted_promotion_resumes_publication_only` | 예정 |
| DV-18 | 시간 규칙은 수집 시각으로 null을 채우지 않고 근거가 없으면 null이다 | `tests/storage/test_time_rules.py::test_rules_never_fill_null_from_ingestion` | 예정 |
| DV-19 | `session_close_plus_lag@1`은 pin한 세션 종료 + lag이며 세션이 없으면 null이다 | `tests/storage/test_time_rules.py::test_session_close_plus_lag` | 예정 |
| DV-20 | `local_day_end@1`은 현지 날짜 끝이며 `time_precision_day` flag를 단다 | `tests/storage/test_time_rules.py::test_local_day_end_flags_day_precision` | 예정 |
| DV-21 | grant에 없는 규칙의 시점은 strict에서 제외되고 영수증에 grant가 남는다 | `tests/storage/test_read_heads.py::test_ungranted_rule_rows_excluded_from_strict` | 예정 |
| DV-22 | `krw_tick@1`은 정확한 십진 전개를 원 단위로 HALF_EVEN 반올림하고 flag를 단다 | `tests/storage/test_decimal_rules.py::test_krw_tick_rounds_half_even_and_flags` | 예정 |
| DV-23 | 숫자 규칙의 SQL 결과와 Python 결과가 같다 | `tests/storage/test_decimal_rules.py::test_sql_and_python_rounding_parity` | 예정 |
| DV-24 | flag는 값을 바꾸지 않고 generation manifest에 해시로 고정된다 | `tests/storage/test_promotion_engine.py::test_quality_flags_are_hashed_with_generation` | 예정 |
| DV-25 | 대량 게시의 delta·chain hash는 Python `aas-rowset-v1` 경로와 같다 | `tests/storage/test_bulk_generation.py::test_streaming_hash_matches_python_rowset` | 구현 |
| DV-26 | `read_heads`는 `project_heads`와 같은 head를 돌려준다 | `tests/storage/test_read_heads.py::test_read_heads_matches_project_heads` | 예정 |
| DV-27 | cutover 구간 밖 날짜는 다른 pin으로 채우지 않고 누락으로 보고한다 | `tests/storage/test_read_heads.py::test_cutover_gap_is_reported_not_filled` | 예정 |
| DV-28 | 유도 조정 가격은 cutoff 이후 기업행동을 쓰지 않는다 | `tests/storage/test_read_heads.py::test_adjustment_ignores_actions_after_cutoff` | 예정 |
| DV-29 | 티커로 instrument를 만들 수 없다 | `tests/storage/test_identity_mint.py::test_ticker_anchor_is_refused` | 예정 |
| DV-30 | v1→v2 migration은 백업 없이 거부하고 중단 후 재개한다 | `tests/storage/test_migration.py::test_migration_requires_backup_and_resumes` | 구현 |
| DV-31 | migration 후 v1 checksum 행이 남고 알 수 없는 버전은 거부한다 | `tests/storage/test_migration.py::test_migration_keeps_v1_receipt_and_rejects_unknown` | 구현 |
| DV-32 | `fields='close'` 가격은 reference만 될 수 있다 | `tests/storage/test_migration.py::test_close_only_prices_are_reference` | 구현 |
| DV-33 | 참조 중이거나 동치가 아니거나 백업이 없는 원천은 은퇴하지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_requires_proof` | 예정 |
| DV-34 | 은퇴는 `raw/` 원본을 지우지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_keeps_raw_bytes` | 예정 |
| DV-35 | 대응표의 구현 행은 존재하는 테스트를, 예정 행은 아직 없는 테스트를 가리킨다 | `tests/tools/test_data_vertical_contract.py::test_contract_rows_match_tests` | 구현 |
| DV-36 | `record` 근거 규칙의 SUPERSEDE 시점은 정정을 담은 원천의 증거 시각이다 | `tests/storage/test_promotion_engine.py::test_superseding_revision_is_not_known_before_its_source` | 예정 |
| DV-37 | 명세 `scope` 밖의 record는 원천에서 빠져도 TOMBSTONE되지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_stays_within_declared_scope` | 예정 |
| DV-38 | TOMBSTONE 시점은 부재를 증명한 snapshot의 증거 시각이며 record 날짜 규칙을 쓰지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_time_comes_from_absence_snapshot` | 예정 |
| DV-39 | 같은 값을 다른 수집 시각에 다시 수집해도 revision이 생기지 않는다 | `tests/storage/test_promotion_engine.py::test_recollection_at_new_ingestion_time_is_not_a_revision` | 예정 |
| DV-40 | head보다 이른 시점의 원천 행은 head를 대체하지 않고 stale로 보고된다 | `tests/storage/test_promotion_engine.py::test_older_source_row_does_not_supersede_newer_head` | 예정 |
| DV-41 | 참조가 없고 동치이며 백업된 원천은 자기 source-link 행이 있어도 은퇴하고 그 행은 남는다 | `tests/storage/test_source_retirement.py::test_unreferenced_equivalent_backed_up_source_is_retired` | 예정 |
| DV-42 | `source_id` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_source_identity.py::test_source_id_format_is_frozen` | 구현 |
| DV-43 | `revision_id` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_revision_id_format_is_frozen` | 예정 |
| DV-44 | `source_row_hash` 형식은 float·bytes·null을 포함한 고정 입력과 기대 값으로 고정되고 `_aas_ordinal`을 제외한다 | `tests/storage/test_promotion_formats.py::test_source_row_hash_format_is_frozen` | 예정 |
| DV-45 | TOMBSTONE 해시 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_tombstone_hash_format_is_frozen` | 예정 |
| DV-46 | `request_hash` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_request_hash_format_is_frozen` | 예정 |
| DV-47 | 매퍼의 SQL `source_row_hash`는 Python 계산과 같다 | `tests/storage/test_promotion_formats.py::test_source_row_hash_sql_matches_python` | 예정 |
| DV-48 | `float_shortest@1`은 소수 12자리를 넘으면 HALF_EVEN으로 반올림하고 범위를 넘는 값은 거부한다 | `tests/storage/test_decimal_rules.py::test_float_shortest_rounds_half_even_and_rejects_overflow` | 예정 |
| DV-49 | 매퍼는 자연키가 겹치는 원천 행을 거부한다 | `tests/storage/test_promotion_engine.py::test_mapper_rejects_overlapping_natural_keys` | 예정 |
| DV-50 | identity로 해석하지 못한 행은 승격하지 않고 미해결 보고에 남는다 | `tests/storage/test_promotion_engine.py::test_unresolved_identity_rows_are_reported_not_promoted` | 예정 |
| DV-51 | flag 제외 목록은 bundle hash와 run 영수증에 들어간다 | `tests/storage/test_read_heads.py::test_flag_exclusions_enter_bundle_hash_and_receipt` | 예정 |
| DV-52 | `cross_provider_mismatch`는 명세의 허용오차를 넘을 때만 달린다 | `tests/storage/test_promotion_engine.py::test_cross_provider_mismatch_uses_spec_tolerance` | 예정 |
| DV-53 | 같은 parent에 다른 요청이 먼저 게시되면 부모 CAS가 실패한다 | `tests/storage/test_promotion_engine.py::test_competing_request_fails_parent_cas` | 예정 |
| DV-54 | migration-incomplete 설치본은 정상으로 열리지 않는다 | `tests/storage/test_migration.py::test_incomplete_migration_refuses_normal_open` | 구현 |
| DV-55 | 수집 시각보다 늦은 규칙 시점은 물리 기준 이후에 받은 행에서만 수집 시각으로 내려가 flag를 달고, 물리 기준 전에 받은 행은 보류로 보고된다 | `tests/storage/test_time_rules.py::test_rule_after_ingestion_is_clamped_above_physical_base` | 예정 |
| DV-56 | parent 명세와 시간 규칙이 다른 승격은 거부되고 규칙 변경은 `.r<N>` 새 dataset으로만 한다 | `tests/storage/test_promotion_engine.py::test_time_rule_change_requires_new_chain` | 예정 |
| DV-57 | 대량 게시의 SQL 셀 부호화는 Python `aas-rowset-v1` codec과 같다 | `tests/storage/test_bulk_generation.py::test_sql_cell_encoding_matches_python_codec` | 구현 |
| DV-58 | 대량 게시의 SQL `record_id`는 `aas-record-v1` Python 계산과 같다 | `tests/storage/test_bulk_generation.py::test_sql_record_identity_matches_python` | 구현 |
| DV-59 | 대량 게시는 `normalize_rows`가 거부하는 행을 같은 이유로 거부한다 | `tests/storage/test_bulk_generation.py::test_row_rules_match_normalize_rows` | 구현 |
| DV-60 | 계획 뒤 dataset head가 바뀌면 대량 게시는 부모 CAS로 거부되고 새 head에서 다시 계획한다 | `tests/storage/test_bulk_generation.py::test_parent_cas_mismatch_requires_replan` | 구현 |
| DV-61 | 같은 ID의 기존 generation은 내용이 같은 요청에만 재사용되고 다른 요청이 채택하지 않는다 | `tests/storage/test_bulk_generation.py::test_leftover_generation_is_not_adopted` | 구현 |
| DV-62 | 증분 검증은 모든 chain link와 대상 generation의 행을, `deep`은 모든 delta를 다시 해시한다 | `tests/storage/test_bulk_generation.py::test_incremental_verify_checks_links_and_leaf_rows` | 구현 |
| DV-63 | 대량 게시의 Python 배치 과금은 가장 넓은 행으로 정해지고 행 수와 무관하다 | `tests/storage/test_bulk_generation.py::test_batches_fit_the_default_budget_at_ten_million_rows` | 구현 |
