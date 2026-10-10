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
| L1 원천 자료실 | `sl_*` 테이블. 원래 열·값·행 순서를 그대로 보존하는 `source_only` 자료, legacy 원본 편입, 수집 job 적재 | `storage/source_library*.py`, `storage/legacy_import/`, `storage/qveris_import.py` |
| L2 승격 | 승격 명세 → 매퍼 → head 비교 → 품질 flag → generation 게시, 유지보수의 chain 이어 붙이기 | `storage/promotion/`, `storage/maintain_promotion.py` |
| L3 저장 | `market.duckdb`의 typed generation과 `state.sqlite3`의 카탈로그·identity·품질·수집 기록, [dataset 카탈로그](#dataset-카탈로그) | `storage/market.py`, `storage/state.py`, `storage/identity.py`, `storage/dataset_catalog.py` |
| L4 소비 | exact pin, cutoff, grant로 head를 투영하는 reader, 조정 가격 유도와 실행 준비 | `storage/read_heads.py`, `storage/adjusted_prices.py`, `storage/market_inputs.py`, `application/backtest_prepare.py` |

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
- `aas db verify`·`backup`·`restore`·`compact`는 기본으로 저장된 행을 기록된 digest와 대조하고 행 값을
  다시 해시하지 않는다. 원천은 marker·intent·연결·은퇴 기록을 그대로 확인하고, 살아 있는 테이블마다
  commit manifest가 기록한 열(순서 포함)과 행 수가 저장된 테이블과 같아야 한다. 승격 chain은 모든 marker
  link와 leaf generation의 delta만 다시 해시한다. `--deep`(`verify_workspace(deep=True)`)은 원천 테이블마다
  행을 다시 해시해 기록된 digest와 비교하고 승격 chain의 모든 delta를 다시 해시한다. 그 밖의 확인
  (SQLite 무결성, publication 문서, `raw/`의 `source_files` bytes, 전략·run 산출물)은 두 방식이 같고 보고
  형태도 같으므로 기본 백업을 `--deep`으로 복원해도 논리 보고 대조를 통과한다. 백업 `backup.json`은
`logical` 밖의 `deep`에 검증 방식을 기록한다. `migrate`·`run-install`·`run-migrate`의 백업도 `--deep`을 받는다. 백업과 복원은 복사한
  파일마다 bytes를 해시하므로 복원본의 저장소는 검증된 백업과 bytes가 같다. compact는 행을 새로 쓰므로
  새 루트는 항상 deep으로 검증한다. 원천 import 재사용, 복구, `aas import legacy --verify`는 그대로 행을
  다시 해시한다.

## legacy 원천 편입

설치본 밖에 남은 legacy 원본(Norgate 내보내기·수집 기록, SEC bulk archive, KR 공개 응답, FRED 내려받기,
FMP 동결 snapshot, Norgate identity authority)은 `aas import legacy`로 한 번 `raw/`와 원천 자료실에
들어온다. 편입은 승격이 아니며 PIT 자격을 주지 않는다. 편입한 원천은 위의 내용 정체성을 그대로 따른다.

요청은 해시로 고정한 manifest 하나다. 1 MiB 이하의 엄격한 UTF-8 JSON이며 알 수 없는 필드·빠진 필드·
중복 키를 거부한다.

| 필드 | 의미 |
| --- | --- |
| `schema_version` | `aas-legacy-import-v1` |
| `entries` | 항목 목록. 항목마다 `name`(보고용 소문자 이름, 유일), `loader`(`name@major`), `path`(원본이 있는 절대 경로), `args`(그 loader가 정의한 인자), `expect`(loader 대조 지표 이름 → 기대 수), 선택 필드 `retain`·`exclude`(아래 파일 대조의 패턴 목록) |

loader는 원본 형식 하나를 읽는 등록된 코드다. loader가 원본의 완결 단위, 출력 테이블의 공급자·shape·열,
대조 지표를 정한다. major는 출력 schema major이며 열이 바뀔 때만 오른다. `name`과 `path`는 ID와 저장 값에
들어가지 않으므로 같은 bytes는 어느 위치·어느 manifest에서 읽어도 같은 원천 ID가 된다.

| loader | 완결 단위 | 원천 ID 접두어와 테이블 | 대조 지표 |
| --- | --- | --- | --- |
| `norgate.history_export@1` | `batch-NNN-result.json` 하나와 그것이 나열한 `batch-NNN/history/<sha256>.csv`. 내보내기의 `plan-<sha256>.json`이 `reused`로 재사용한 시리즈는 형제 수집 디렉터리(`<디렉터리>/history/`)의 checkpoint 하나(`history/checkpoints/<manifest_sha256>.json`)마다 한 단위이며, 그 checkpoint와 계획이 재사용한 CSV다 | `norgate-history-csv`, `bars`: result 또는 checkpoint 기록의 asset ID·심볼·database·종목명·CSV 해시, `date`와 값 열 원문 | `units`, `records`, `rows`, `planned_series`, `missing_series`*, `unplanned_series`*, `repeated_series`* |
| `norgate.index_membership@1` | `batch-results/*.json` 중 family `membership`이 가리키는 `batch-attempts/` 수집 디렉터리 하나(또는 `args.include`의 수집 디렉터리): `request.json`, `manifest-*.json`, `receipts.jsonl`, 그 journal이 나열한 `index_constituent_timeseries` gzip CSV | `norgate-index-membership`, `constituents`: job ID·asset ID·심볼·지수 이름·gzip과 CSV 해시, `date`, `index_constituent` 원문 | `units`, `pairs`, `rows`, `planned_pairs`, `missing_pairs`*, `unplanned_pairs`*, `repeated_pairs`* |
| `norgate.identity_authority@1` | identity authority JSON 한 파일 | `norgate-identity-mappings`(`mappings`), `norgate-identity-issuer-bindings`(`issuer_bindings`). 같은 파일이라 hex를 공유한다 | `mappings`, `issuer_bindings` |
| `sec.submissions_zip@1`, `sec.companyfacts_zip@1` | archive 하나와 `args.evidence`의 수집 영수증 | `sec-submissions-zip`, `sec-companyfacts-zip`, `members`: central directory 순서의 member 이름·압축·원래 크기·CRC-32·수정 시각·압축 방식·member bytes의 SHA-256 | `members`, `json_members`, `expanded_bytes`, `receipts` |
| `sec.submissions_filings@1` | submissions archive 하나와 `args.evidence`의 수집 영수증 | `sec-submissions-filings`, `filings`: 제출자 문서와 쪽 문서가 싣는 공시마다 한 행. member 이름, member의 10자리 CIK, SEC의 병렬 배열 값 원문(아래) | `members`, `json_members`, `expanded_bytes`, `receipts`, `filers`, `pages`, `filings`, `other_members`, `missing_pages`*, `unlisted_pages`*, `miscounted_pages`*, `unknown_members`* |
| `fred.series_csv@1` | `observation_date,<SERIES>` CSV 한 파일 | `fred-series-csv`, `observations`: series ID, 날짜와 값 원문 | `rows` |
| `korea.public_response@1` | 요청 디렉터리 하나(`request.json`, `response.json`, `response.raw`, `normalized.v1.json`) | KIND `kind-listings`(`listings`), BOK `bok-observations`, OECD `oecd-observations`(`observations`). 정규화 문서의 행이며 중첩 값은 정규 JSON 문자열 | `units`, `listings`, `observations` |
| `fmp.price_eod_non_split@1` | `run_id=*/fmp_price_eod_non_split_adjusted`의 `history-index.json`과 `part-*.parquet` | `fmp-price-eod-non-split`, `bars`: Parquet 행을 part 순서 그대로 | `units`, `rows` |

`*`를 붙인 지표는 불일치 수(계획했지만 읽지 못한 항목, 계획 밖 항목, 두 번 읽은 항목)다. manifest `expect`가
다른 수를 기록하지 않으면 기대 수는 0이므로, 기록되지 않은 불일치는 `reconciled`를 거짓으로 만든다. 알려진
누락은 그 수를 `expect`에 적어 받아들인다. SEC archive의 영수증은 선택이며 `receipts`로 그 수를 고정할 수 있다.

`sec.submissions_filings@1`은 SEC submissions archive를 공시 행으로 읽는다. 제출자 문서
`CIK##########.json`은 최근 공시를 `filings.recent`의 병렬 배열로 싣고 이전 공시를 담은 쪽 문서를
`filings.files`(쪽 이름과 `filingCount`)로 나열한다. 쪽 문서 `CIK##########-submissions-###.json`은 같은
배열을 최상위에 싣는다. 배열 위치 하나가 행 하나이며 member는 central directory 순서, 위치는 배열 순서다.
열은 `member`, `cik`(member 이름의 10자리 CIK)와 SEC의 배열 `accessionNumber`, `filingDate`, `reportDate`,
`acceptanceDateTime`, `act`, `form`, `fileNumber`, `filmNumber`, `items`, `core_type`, `primaryDocument`,
`primaryDocDescription`(텍스트), `size`, `isXBRL`, `isInlineXBRL`, `isXBRLNumeric`(정수)이다. 문서에 없는
배열은 그 행에서 null이다. 이 밖의 키, 길이가 다른 배열, 다른 JSON 타입의 값, member 이름과 다른 `cik`를
싣는 제출자 문서, 자기 CIK의 `CIK##########-submissions-###.json`이 아닌 쪽을 나열하는 제출자 문서는 단위를
거부한다. 제출자가 나열했지만 archive에 없는 쪽(`missing_pages`), 어느 제출자도
나열하지 않은 쪽(`unlisted_pages`), 공시 수가 나열과 다른 쪽(`miscounted_pages`), 그 밖의 이름을 가진 JSON
member(`unknown_members`)는 불일치 지표다. 쪽의 행은 수가 달라도 모두 읽는다. 같은 archive를
`sec.submissions_zip@1` 항목과 함께 편입할 수 있으며 bytes는 `raw/`에 한 번 보존된다.
FMP loader는 run마다의 데이터셋을 모두 보존하고 run 사이의 선택은 승격이 정한다. `history-index.json`은 수집기의
run 누적 최근 날짜 캐시이므로 part 목록과 대조하지 않고 데이터셋 이름만 확인한다. 배치 없이 재사용 시리즈만
있는 Norgate 내보내기도 checkpoint 단위로 편입한다.

행 규칙은 모든 loader가 같다.

- 행은 단위의 bytes에서만 나온다. CSV 값은 원문 문자열이고 날짜·숫자를 다른 타입으로 해석하지 않는다.
  따라서 `1.6357e+06` 같은 잘린 거래량도 원문 그대로 남고, 승격에서 `decimal_text@1`이 그 원문을 읽는다.
  비어 있는 값은 빈 문자열이고, 원본에 없는 열만 null이다. 행을 채우거나 다듬거나 중복 제거하거나 다시
  정렬하지 않는다.
- 단위의 bytes가 자기 색인과 다르면(기록한 해시·행 수·헤더·날짜 범위, journal과 manifest 해시, 영수증의
  archive 해시·크기·member 수) 그 단위를 거부하고 고쳐 읽지 않는다. 알 수 없는 열이나 형식도 거부한다.
- loader가 읽은 파일은 모두 단위의 파일 목록에 있어야 하고 목록의 파일은 모두 읽혀야 한다. 원천 ID는
  그 파일 전체의 `[raw 주소, 크기, SHA-256]`에서 나온다.
- Norgate 내보내기의 계획 시리즈는 `plan-<sha256>.json`의 `pending_symbols`와 `reused`의 합집합이다.
  재사용 시리즈는 계획의 해시·행 수·asset ID와 checkpoint 기록이 모두 같아야 읽는다. 계획과
  `acquisition-<sha256>.json`은 각자 이름의 해시와 같아야 하고, acquisition은 그 계획을 가리키며 batch 수와
  재사용 수가 맞아야 한다. 하나라도 다르면 항목 전체를 거부한다. 읽지 못한 계획 시리즈는 `missing_series`로 센다.
- 원본은 `raw/` 보존과 같은 기준으로 받아들인다. 사용자가 소유한 단일 link의 비공개(그룹·기타 권한 없음)
  일반 파일이어야 하며, 그렇지 않은 원본은 거부하고 그 이유를 보고한다.

`--plan`은 설치본을 열지 않고 원본만 읽으며 아무것도 쓰지 않는다. 단위마다 행을 끝까지 읽고 검증해
테이블별 행 수, 원천 자료실 digest, 원천 ID를 계산한다. 실패한 단위는 이유와 함께 보고하고 나머지를
계속 계획한다. 보고는 항목마다 단위·파일·bytes 수, 원천 목록, 대조 지표, `expect`와의 대조, 거부를
싣고, 모든 기대 수가 맞고 거부가 없을 때 `reconciled`가 참이다.

실행은 manifest bytes를 `raw/`에 두고 단위마다 원본을 모두 `raw/`에 스트리밍으로 보존한 뒤, `raw/`의
사본에서 다시 읽은 행을 `import_content_arrow`로 commit한다. 출력 테이블마다 commit 하나이고 `sl:` 연결을
함께 기록한다. 적재 코드·항목 이름·단위 이름·manifest 해시는 `lineage`에만 남는다. 이미 commit된 원천은
행을 다시 계산해 기록과 같을 때 재사용한다. 단위는 하나씩 commit하므로 중단된 편입은 같은 명령으로
끝내고, 처음 거부된 단위에서 실행이 멈춘다. 공급자는 호출하지 않는다.

항목 경로 아래의 일반 파일(경로가 파일이면 그 파일)은 모두 대조한다. 따라가지 않은 symlink도 파일로 센다.

| 분류 | 파일 | 처리 |
| --- | --- | --- |
| 단위 | 어떤 단위의 파일 목록에 있는 파일 | 원천 ID에 들어가고 `raw/`에 보존된다 |
| 보존 | loader가 단위를 찾으려고 읽은 색인 파일(내보내기 계획·acquisition, 지수 구성 계획, 다른 family의 batch result)과 `retain` 패턴에 맞는 파일 | bytes 그대로 `raw/`에 보존되고 항목의 보존 목록 원천에 경로와 함께 기록되며 `--verify`가 다시 확인한다 |
| 제외 | `exclude` 패턴에 맞는 파일 | 편입하지 않는다는 운영자의 기록. 수와 bytes를 보고한다 |
| 미대조 | 그 밖의 파일 | 수, bytes, 앞의 경로 20개를 `uncovered`로 보고한다 |

패턴은 항목 경로 기준 상대 POSIX 경로에 맞추는 `fnmatch` 문법이며 `*`는 `/`도 넘는다. 분류는 단위, 보존,
제외 순으로 먼저 맞는 것을 따른다. 단위 밖 형제 디렉터리의 파일(재사용 CSV, SEC 영수증)은 단위가 나열한 파일만
보존되며 그 디렉터리의 나머지 파일은 대조하지 않는다.

보존 파일이 있는 항목은 보존 목록 원천 하나를 가진다. 보존 목록 문서 `aas-legacy-retained-v1`은 항목 이름,
loader, 보존 파일마다 `[상대 경로, SHA-256, 크기, 이유]`(이유는 `index` 또는 `retain`, 경로순)를 담은 정규 JSON이며
`raw/`에 보존된다. 이 문서와 모든 보존 파일이 원천 ID `legacy-retained-files-<hex>`의 원본이고, 테이블
`retained_files`(`path`, `sha256`, `size_bytes`, `reason`)가 문서의 행을 그대로 싣는다. 경로가 문서에 들어가므로
같은 bytes라도 경로가 다르면 다른 원천이다. 항목 경로를 지운 뒤에도 설치본은 보존 경로마다 bytes를 찾아 준다.

`--verify`는 설치본을 읽기 전용으로 열고 계획을 원본에서 다시 계산한 뒤, 계획한 원천마다 commit이 완료됐고
테이블 이름·행 수·digest가 같고 저장된 테이블을 다시 해시해도 그 행 수·digest이며 `sl:` 연결이 유도와 같고 연결된 원본이 `raw/`에서 다시 해시해 맞는지,
보존 파일마다 `raw/` 사본이 다시 해시해 맞는지, 보존 목록 원천이 같은 방식으로 commit·연결됐는지 확인한다.
그렇지 않은 원천·보존 파일과 거부된 단위는
`unmatched`로 센다. `unmatched`가 0이고 `reconciled`가 참이며 미대조 파일이 0이면 `complete`다. 설치본 밖
legacy 원본은 `complete`인 manifest의 항목 경로만 지울 수 있다. 항목 경로가 아닌 디렉터리(형제 수집 디렉터리,
내보내기 상위 디렉터리)는 그 자체를 항목으로 대조하기 전에는 지우지 않는다. CLI는 `--verify`가 `complete`가
아니거나 `--plan`·실행이 `reconciled`가 아니면 보고를 출력한 뒤 종료 코드 1을 돌려준다.

## Qveris 수집과 원천 적재

`aas collect qveris`는 Qveris 게이트웨이(하위 공급자 EODHD)의 수집 job을 계획·실행하고, 완료된
job을 원천 자료실의 내용 원천으로 적재한다. 수집 증거(`jobs/<fingerprint>/` 아래 page의 intent·raw·
response·billing과 `complete.json`)를 쓰는 규칙은 [qveris_acquisition.py](../../src/aegis_alpha/data/qveris_acquisition.py)와
[qveris_store.py](../../src/aegis_alpha/data/qveris_store.py)가, 명령은 [operations](../operations.md#qveris-원문-수집)가 소유한다.

**수집.** job 문서 하나가 cohort 하나다. 작업자 하나면 cohort를 순서대로 job 하나씩
수집하고(`data/qveris_batch.py`), 여럿이면 cohort들을 번갈아 섞어 단일 page 도구(EOD 일간 내려받기,
JSON 이력, SEC facts)를 작업자 수만큼 한 group으로 묶는다(`data/qveris_parallel_batch.py`). group은
모든 새 job의 견적을 받고, 서버 잔액에서 다른 예약을 뺀 값과 실행 예산(유료 호출 수·크레딧) 모두가
group 전체를 받아들일 때만 batch manifest와 job별 intent를 남긴 뒤 실행한다(`data/qveris_parallel.py`).
예산은 group 전체를 예약하거나 하나도 예약하지 않는다. 실행 중인 유료 future는 모두 기다려 기록하고
(job의 응답 bytes 상한을 넘는 응답은 순차 수집처럼 보존하지 않고 `JobResponseTooLarge`로 남긴다),
group 정산은 usage와 계정 ledger로 한 번 한다. 정산 중 인증·quota 응답이 있으면 정산된 group의 완료와
유료 호출을 센 뒤 `PROVIDER_STOP`으로 멈춘다. 정산되지 않은 group은 정산되거나 운영자가 예약을 남긴 채
격리할 때까지 그 계정의 새 실행을 막는다. 모든 요청은 실행 하나에 공유된 HTTP 시도 수·시간 한도를 통과하고
같은 간격으로 시작한다(`data/qveris_pacing.py`). 두 한도는 새 page를 시작하지 않게 할 뿐이다. 한도는
간격을 기다린 뒤 확인하므로 기다리는 사이 시간이 지나면 새 page는 시작하지 않는다. 한도에 닿으면 다음 page의 첫 사전 조회 요청(`/tools/by-ids`)이 intent 전에 `INVOCATION_HTTP_LIMIT`로
거부되고, 이미 시작한 page의 견적·실행·정산 요청은 세되 모두 통과한다. 그래서 한도는 intent를 실행되지
않거나 정산되지 않은 채로 남기지 않으며, 실제 HTTP 시도 수는 진행 중인 page의 요청만큼 한도를 넘을 수 있다.

- 정산된 실패(검증할 수 없는 응답 형태 포함)는 기록하고 다음 job으로 간다. 공급자 경고(`RAW_ACQUIRED_WITH_WARNINGS`)는 완료이며
  `warned`로 세고 수집을 멈추지 않는다.
- 결과가 불확실한 유료 호출은 수집을 멈추고(`stopped`, 종료 코드 2) 자동으로 다시 호출하지 않는다.
- 예산 거부(유료 호출 수·크레딧·HTTP 시도 수·시간·서버 잔액)는 intent를 만들기 전에 일어나므로 시도한
  것이 없다. 남은 job은 `pending`이고 상태는 `budget_exhausted`(종료 코드 0)다. 다음 실행은 완료된 job을
  HTTP 없이 재사용한다. 예산 거부가 났어도 raw root에 정산되지 않은 page나 group이 있으면 그 증거가 계정을
  막으므로 `stopped`(종료 코드 2)로 보고한다.
- 정산될 수 없는 page나 group(나타나지 않는 usage, 불완전한 intent 묶음)은 운영자가
  `aas collect qveris quarantine`으로 사유와 함께 격리한다. 격리는 최악의 경우 예약을 남기고 자동 재호출하지
  않으며, 그 뒤 계정의 새 실행이 다시 가능하다.

**일간 요청.** `daily-jobs`는 거래소(`US`는 XNYS, `KO`·`KQ`는 XKRX)의 [선언 달력](#선언-달력)에서
관측일 이전의 열린 세션마다 `prices`·`splits`·`dividends` 일간 내려받기 요청을 만든다. 요청의 정체는
도구·하위 공급자·시장·dataset·요청 변수이고 관측일과 job ID는 아니다. raw 수집 root의 완료된 job이
같은 요청을 가지면 상태와 관측일이 무엇이든 그 요청은 `covered`다. 완료 없이 시도만 있는 요청은
`held`로 보고하고 계획하지 않으므로 불확실한 시도가 자동 재호출되지 않는다. 창은 366일 이하이고 선언
기간 안이어야 한다.

**적재.** 완료된 job 하나가 원본 단위 하나다. 단위의 bytes는 `complete.json`과 그것이 pin한 page 파일
넷이다. 원천 ID는 이 bytes와 schema major의 `aas-source-id-v1`이므로 실행 순서·묶음·적재 코드 버전·identity
문서가 달라도 같은 job은 같은 ID가 되고, 이미 commit된 job은 다시 적재하지 않는다. 행이 instrument를
이름 붙일 때 그 행을 해석한 identity 문서(`{"identities": {"<CODE>.<EXCHANGE>": {instrument_id, venue,
instrument_type, currency, ...}}}`)의 SHA-256은 적재 코드·변환 해시와 함께 commit `lineage`
(`identity_sha256`)에 남는다. 그래서 새 상장으로 identity 문서가 커져도 적재된 job이 두 번째 원천이 되지
않고, `aas data kr-prices`가 같은 job의 행을 두 번 pin하지 않는다.

| job | 행 원천(shape, 테이블) | 보류 행 원천(shape) |
| --- | --- | --- |
| KR·US 단일 종목 `price_history` | `<m>-history-bars`, `bars` | `<m>-history-quarantine` |
| 거래소 일간 `prices` | `bulk-bars`, `bars` | `bulk-quarantine` |
| 거래소 일간 `splits` | `splits`, `splits` | `splits-quarantine` |
| 거래소 일간 `dividends` | `dividends`, `dividends` | `dividends-quarantine` |
| FX 통화쌍 `fx_history`(`<BASE><QUOTE>.FOREX`) | `fx-history-bars`, `bars` | `fx-history-quarantine` |

- 원천 ID는 `qveris-<shape>-<hex>`이고 한 job의 두 원천은 `hex`를 공유한다. 보류 테이블 이름은 모두
  `quarantine`이며 열은 `ordinal`, `reason`, `source_row_json`이고 이력은 앞에 `source_fingerprint`를
  둔다. 가격 `bars` 열은 `eodhd.*` 매퍼가 읽는 열 그대로다.
- 행 테이블은 비어 있어도 commit한다(분할이 없는 거래일도 사실이다). 보류 테이블은 행이 있을 때만,
  행 테이블보다 먼저 commit하므로 행 원천이 있으면 단위가 완결이다. 다시 적재하면 행 원천이 있는 단위는
  다시 만들지 않고 `reused`로 센다.
- 공급자 경고는 기록만 한다. 경고가 붙은 내려받기의 모든 행은 `provider_reported_partial` 사유로 보류되고
  적재는 계속된다. 승격은 그 행에 같은 flag를 단다([KR 가격](#kr-가격)).
- 분할 비율, 배당 금액·통화·날짜·주기는 공급자 텍스트 그대로(숫자는 가장 짧은 십진 표기) 남긴다.
  identity가 없는 종목, 양수 십진수가 아닌 비율·금액은 그 사유로 보류하고 고치지 않는다. 통화쌍
  이력은 instrument가 아니므로 identity 문서 없이 쌍·기준 통화·호가 통화를 남긴다.
- 읽거나 검증할 수 없는 job은 `failures`에 남고 다음 job을 적재한다. 완료 문서가 없는 요청 job은
  `missing`의 `no_completion`, 읽을 수 없거나 다른 job을 가리키는 완료 문서는 `missing`의
  `unreadable_completion`으로 남고 다른 job은 계속 적재한다. 적재기가 없는 종류(FRED, SEC, 종목 목록, 연구 이력)는 `unsupported`로 센다. 적재는
  공급자를 호출하지 않는다.

## 승격 명세 `aas-promotion-v1`

승격은 해시로 고정한 명세 문서 하나로 요청한다. 명세의 정확한 bytes는 `raw/`에 보존하고
SHA-256을 호출자가 함께 준다. 64 MiB 크기 상한은 명세 문서에만 적용되며 승격되는 행은
DuckDB 안에서 흐르고 Python으로 통째로 올라오지 않는다. 엄격한 UTF-8 JSON이며 알 수 없는
필드·빠진 필드·중복 키·BOM·NaN과 어느 위치에서든 `latest` 같은 이동하는 참조는 거부한다.

| 필드 | 의미 |
| --- | --- |
| `schema_version` | `aas-promotion-v1` |
| `target` | `domain`, `dataset_id`, `parent`(직전 generation ID, 첫 generation은 null) |
| `sources` | 순서 있는 원천 pin 목록. 각 항목은 `source_id`, `source_sha256`, `table`, `digest`(commit manifest의 테이블 digest). 같은 원천 테이블은 한 번만 pin한다 |
| `mapper` | `name`(`name@major`)과 그 매퍼가 정의한 인자 `args`. 매퍼가 조인하는 다른 dataset의 generation pin(`dataset_id`, `version`, `generation_id`, `chain_hash`, `manifest_hash`)도 인자다 |
| `partition` | null 또는 `from`·`to` 날짜. 매퍼의 파티션 날짜가 `[from, to)`인 원천 행만 승격한다. 백필은 구간 하나가 generation 하나다. |
| `time_rules` | `available_at_us`, `revision_known_at_us` 각각의 `rule`(`id@version`), 입력 근거 `basis`(`revision`·`record`), 규칙이 읽는 매퍼의 시간 입력 `input`(없으면 null), 그 규칙의 인자 `args` |
| `decimal_rule` | 매퍼가 내는 숫자 열마다 `id@version` 하나 |
| `quality_rules` | `rule`(`id@version`)과 `args`의 목록. 같은 규칙은 한 번만 쓴다 |
| `tombstone_policy` | `{"mode": "never"}`, 또는 `absent_in_full_snapshot`과 전체 snapshot인 pin 하나(`source`: `source_id`, `table`), 그것이 빠짐없이 담는 범위(`scope`: instrument ID 목록 또는 모든 instrument인 null, `from`·`to` 날짜 구간). 범위는 `partition` 안에 있다. 부재는 그 snapshot의 행으로만 판단하고, 다른 pin의 행이 범위 안에 있으면 계획이 거부하므로 그 행은 별도 generation으로 승격한다 |
| `identity_snapshot` | 매퍼의 identity key를 해석할 identity snapshot pin(`snapshot_id`, `content_hash`). instrument가 필수인 도메인은 늘 pin한다. identity key가 없는 매퍼(거시·FX·달력, 영구 anchor에서 주체 ID를 발급하는 분류 매퍼, 발행인 단위의 재무·공시)는 null |

`request_hash = sha256(정규 JSON ["aas-promotion-request-v1", 명세 SHA-256, 원천 digest 목록, parent])`다.
그 정규 JSON 요청 문서도 `raw/`에 보존한다. generation ID는 `prm-<request_hash>`, intent의 operation
ID는 `promotion:<request_hash>`, dataset version은 그 generation의 chain sequence를 10진수로 쓴 문자열이다.
같은 요청은 기존 operation과 generation을 그대로 돌려준다. 같은 parent에 다른 요청이 먼저
게시됐으면 부모 CAS가 실패하고 호출자가 새 parent로 다시 계획한다.

명세의 해시는 `dataset_versions.transform_hash`가 된다. 따라서 매퍼 major, 시간 규칙 버전,
숫자 규칙 버전이 바뀌면 transform hash도 바뀐다. 게시된 generation은 자기 명세를 raw 증거로
다시 읽어 검증할 수 있다.

## 승격 실행과 보고

`aas data promote --spec FILE --sha256 SHA256 [--plan]`이 실행 진입점이고 `aas data promotions`가
승격 intent와 그 generation·카탈로그 행을 나열한다. 승격은 공급자를 호출하지 않는다.

순서는 원천 확인 → 매퍼가 참조하는 generation 확인·적재 → 원천 stage → 매퍼 → identity 해석 →
숫자·시간 규칙 → head 비교 → quality flag → 대량 게시 계획이다. 원천 확인은 pin한 테이블마다 완료된 commit과 테이블 digest를 다시
계산해 대조한다. `--plan`은 같은 계산을 하고 아무것도 쓰지 않는다. 읽기 전용으로 연 설치본에서
돌며, 계산에 쓰는 것은 그 연결의 임시 테이블뿐이다. 보고는 원천 행 수, 행 상태(`ok`, `held`,
`unresolved`, `ambiguous`, `refused_*`), 매핑 행 수, 매퍼가 고르지 않은 원천 행 수(`unselected_rows`, 매핑
행이 하나도 없는 원천 행: 여러 시계열을 담은 원천에서 한 시계열만 읽는 매퍼, 분류가 없는 회사, 자료 없는 응답처럼
매퍼가 정의상 고르지 않는 행), 원천 행의 결과 분포(`source_outcomes`, 아래 매퍼 절), 미해결 token 표본, 해석 전
매핑 행 전체의 숫자 flag 분포,
시간 규칙별 null·상한 적용 수, 반복된 자연키, op 분포, 변하지 않은 행과 stale 행 수,
head와 시점이 다르게 계산되는 변하지 않은 행 수(`time_drift`), delta의 flag 분포, 부분 응답 행이 있으면
그 행 수 대조(`partition_row_count`), 계획한 marker를 담는다.

보고는 두 종류의 거부 이유를 따로 싣는다. `blocking`은 설치본이 아직 갖추지 않은 전제다(core
schema v2, 원천의 `sl:` 연결, 등록되지 않은 identity snapshot). `refusals`는 자료 자체의 문제다(규칙이
변환하지 못한 숫자, 비어 있는 필수 열, 수집 시각이 없는 행, 반복된 자연키, 부재를 증명할 수 없는
미해결 행, `partition`이 있는 명세에서 파티션 날짜가 없는 원천 행. 결과를 선언한 매퍼는 예외로 아래 매퍼 절).
필수 열이 빈 행은 identity 해석 결과와 무관하게 `refused_required`이므로 형식이 잘못된 행이 미해결 행으로
빠지지 않는다. 실행은 둘 중 하나라도 있으면 아무것도 쓰지 않고 거부한다. delta가 비어 있으면
generation을 게시하지 않는다. 다만 원천 행의 결과를 선언한 매퍼의 빈 delta는 원천이 자료 없음·실패를
말한 coverage이므로, parent가 있으면 그 parent의 dataset version에 `promotion_coverage@1` 품질 검사
하나(요청·명세 해시, 원천 pin, 원천 행의 결과 분포, 행 상태, 변하지 않은 행과 stale 행 수)를 기록하고
명세와 요청 문서를 `raw/`에 남긴다. 검사 ID는 요청 해시에서 나오므로 같은 요청을 다시 실행해도 새로
쓰지 않는다. parent가 없으면 검사를 붙일 version이 없고, 실행 결과의 `coverage_check`가 null이다.

실행은 명세, 요청 문서, 승격 manifest를 `raw/`에 쓰고, 그 manifest의 SHA-256을 payload hash로 한
`promotion` intent를 기록한 뒤, marker·행·quality flag를 DuckDB 트랜잭션 하나에서 게시하고, 마지막에
state 카탈로그를 한 트랜잭션으로 쓰고 intent를 완료한다. 카탈로그는 `datasets`(owner `promotion`),
`dataset_versions`(`manifest_hash`는 marker의 request hash, `transform_hash`는 명세 해시,
`normalizer_version`은 매퍼 `name@major`, `identity_snapshot_hash`, `coverage`는 partition), 원천마다
`dataset_sources`의 `sl:` 행, `quality_checks`의 `promotion_report@1` 행(op·행 상태·flag 수, 결과를 선언한
매퍼면 원천 행의 결과 분포)과 부분 응답 행이 있으면 `partition_row_count@1` 행([품질 검사](#품질-flag와-품질-검사)), 그리고
(매퍼 공급자, dataset, partition)마다 delta의 가장 늦은 `ingested_at_us`까지 앞으로만 가는 `watermarks`다.
`committed_version`은 그 시각까지 처음 나아간 version이며, 시각을 넘지 못한 정정 generation은 바꾸지 않는다.

승격 manifest(`aas-promotion-manifest-v1`, 정규 JSON)는 요청·명세 해시, marker의 dataset·version·
generation·parent·sequence·delta hash·chain hash·행 수, op 분포, 행 상태 수, flag 행의 rowset digest와
수, 원천 pin과 identity pin, 부분 응답 행이 있으면 `partition_row_count`를 담는다. 결과를 선언한 매퍼의
승격 manifest는 원천 행의 결과 분포(`source_outcomes`)도 담고, 다른 매퍼의 manifest에는 그 키가 없다. 중단된 승격은 같은 명령을 다시 실행하거나 `aas db recover`로
끝낸다. market에 commit된 generation이 있으면 manifest와 대조해 카탈로그만 쓰고, 없으면 보존한
명세로 다시 계산해 manifest가 intent의 payload hash와 정확히 같을 때만 게시한다. 다시 계산한 결과가
다르면 intent는 PREPARED로 남고 `aas db quarantine`의 대상이 된다. 중단된 요청의 `--plan`은 아무것도
복구하지 않는다. market commit 전이면 보고를 다시 계산해 intent의 manifest와 같은지(`recomputes_intent`)를
싣고, commit 뒤면 카탈로그만 남았다고(`market_committed`) 보고한다. `aas db verify`는 승격 chain의
generation마다 카탈로그, 완료된 intent, 명세·요청·manifest의 raw 증거, flag digest를 확인하고 chain
link와 마지막 generation의 행을 다시 해시한다.

## 매퍼

매퍼는 원천 relation을 받아 도메인 열을 가진 DuckDB relation을 돌려주는 순수 함수다.
네트워크·현재 시각·난수·환경 변수를 읽지 않는다. 이름은 `<provider>.<shape>`, 버전은 major
하나다. 같은 입력에 대한 출력이 한 행이라도 달라지면 major가 오른다. 매퍼마다 합성 원천
fixture와 독립 기대값으로 검증한다. 원천 행 해시, identity 해석, 숫자·시간 규칙, record와
revision 정체성, head 비교, flag는 매퍼가 아니라 승격 엔진(`storage/promotion/engine.py`)이 모든
매퍼에 같게 적용한다.

매퍼의 relation은 원천 행 위치와 해시를 그대로 넘기고, 행의 수집 시각(없으면 null), identity key가
있으면 그 token과 token을 해석할 시각, key가 해석하는 열(`instrument_id`, 분류는 `subject_id`)을 뺀
도메인 열, 원천 값 그대로의
숫자 열, 선언한 시간 입력 열, 원천 행 자체가 싣는 품질 flag의 불리언 열을 낸다. 매퍼는 원천 열 이름과
허용 타입, 숫자 열의 원천 타입, identity assertion key(provider, namespace), 파티션 날짜(원천 열에서
날짜를 내는 SQL 식), tombstone 범위가 쓰는 도메인 날짜 열, 행 flag를 선언한다. 행 flag는 그 행이 만든
revision에 매퍼의 `name@major`를 규칙으로 해서 달리고 `detail`은 null이다. 필수 도메인 열이 빈 행은
identity 해석과 무관하게 `refused_required`이므로, 모양이 잘못된 행이 미해결 행으로 조용히 빠지지 않는다.
모든 행이 instrument를 가리키는 도메인의 매퍼는 identity key를 가진다. 분류의 주체는 원천이 주체의 영구
anchor를 실을 때 매퍼가 그 anchor로 ID를 직접 발급하고 identity key를 갖지 않는다.
매퍼는 pin한 원천의 commit manifest `metadata`에서 읽을 목록 이름 하나(`manifest_items`)를 선언할 수
있다. 엔진은 원천마다 그 목록의 원소를 정규 JSON 텍스트 한 행으로 펼쳐 넘기고, 목록이 없는 원천은
계획 거부로 보고한다. 엔진은 목록을 넘기기 전에 원천의 request hash를 manifest의 표와 `metadata`에서
다시 계산해 marker·완료 operation의 값과 맞춰 보고, 다르면 그 원천을 계획 거부로 보고한다. 그래서
매퍼가 manifest에서 읽는 값도 행과 같이 pin된다.
명세의 `partition`은 매퍼의 파티션 날짜 식으로 원천 행을 고른다. 날짜를 텍스트로 싣는 원천(FRED CSV,
KR 공개 응답, Norgate 내보내기 편입본)은 `YYYY-MM-DD`(KR은 기간의 첫날) 텍스트만 날짜로 읽고 다른 표기는
파티션 날짜가 없다. tombstone 범위의 날짜는 매퍼가 선언한 도메인 열이 DATE면 그 값이고, UTC
microsecond 시각(FX의 `fixing_at_us`)이면 그 UTC 날짜다. reader가 같은 날짜로 거르므로 범위와 읽기가
같은 날짜를 쓴다. 여러 공급자의 원천이 같은 형태를 공유하는 매퍼는 읽는 원천 ID 접두사를 선언하고, 명세
단계에서 접두사가 다른 원천 pin을 거부한다. 그래서 한 공급자의 행이 다른 공급자의 dataset에 들어가지
않는다. 접두사를 선언하지 않은 매퍼는 열이 맞는 원천을 모두 읽는다.

- 원천 행 하나가 공급자 응답 문서 하나이면 그 행은 도메인 행을 여럿 담거나 하나도 담지 않는다. 그런
  매퍼는 펼침(`expands`)을 선언하고 원천 행 안에서 서로 다른 번호 `_aas_item`을 낸다. 펼치지 않는 매퍼의
  행은 원천 행마다 많아야 하나이고 번호는 0이다. 엔진은 매핑 행을 (pin, 원천 위치, 번호)로 구별하고 한
  원천 행 안의 번호가 겹치면 매퍼 오류로 거부한다. 펼친 행은 모두 그 원천 행의 `source_row_hash`를 가진다.
- 원천 행이 사실이 아니라 응답인 매퍼는 원천 행의 결과(`outcome`, 예: 완료·자료 없음·실패)를 SQL로
  선언한다. 엔진은 결과별 원천 행 수를 보고와 manifest, `promotion_report@1` 품질 검사(빈 delta면
  `promotion_coverage@1`)에 남긴다. 자료가 없다는 응답은 행을 만들지 않고 이 결과 분포로만 기록된다.
  `partition`이 있는 명세는 파티션 날짜가 없는 원천 행을 거부하지만, 결과를 선언한 매퍼의 그런 행(다른
  endpoint, 읽을 수 없는 요청)은 모든 파티션에 들어가 결과로 세어진다. 그래서 손상된 요청이 어느
  파티션에서도 빠지지 않고, 읽을 수 없는 요청은 매퍼의 결과 규칙에 따라 거부된다.
- `instrument_id`가 선택인 도메인(재무, 공시)은 identity key가 없는 매퍼로 승격할 수 있고 그 행의
  instrument는 null이다. `instrument_id`가 필수인 도메인은 instrument를 해석하는 매퍼만 쓴다. 명세는
  해석하는 매퍼에만 identity snapshot을 pin한다.
- 매퍼는 다른 dataset의 generation을 참조(`references`)로 조인할 수 있다. 참조마다 도메인과 매퍼 인자의
  generation pin이 있고, 엔진은 pin을 달력 pin처럼 marker·카탈로그·chain으로 확인한 뒤 그 dataset의 도메인이
  참조의 도메인인지 보고, chain의 TOMBSTONE이 아닌 head 행의 도메인 열을 참조 테이블에 적재한다. 매퍼는
  원천 relation처럼 그 테이블을 읽는다. 참조 pin은 규칙이 아니라 매퍼가 읽는 근거 자료라서, 자식
  generation의 명세는 parent가 pin한 generation이나 같은 dataset chain의 후손만 pin할 수 있다. 참조가
  달라져 도메인 열이 바뀐 행은 새 revision이다.

`source_row_hash`는 원천 행 내용의 해시다.

```text
source_row_hash = sha256(정규 JSON ["aas-source-row-v1", [[열 이름, 값], ...]])
```

열은 원천 schema 순서이며 원천 자료실의 내부 열(`_aas_ordinal`)은 제외한다. 값의 표현은 원천
자료실 digest와 같다(`source_library_digest.scalar`: float은 `{"float_hex": float.hex()}`, bytes는
`{"base64": ...}`). 시간 값은 지역 설정이나 시간대를 거치지 않도록 태그를 붙인다. 날짜는
`{"date": "YYYY-MM-DD"}`, 시간대가 있는 timestamp는 `{"utc_us": 정수}`, 시간대가 없는 timestamp는
`{"local_us": 정수}`, 시간대가 없는 nanosecond timestamp(DuckDB `TIMESTAMP_NS`, pandas가 쓴 Parquet 날짜)는
`{"local_ns": 정수}`다. 불리언·정수·문자열·null은 JSON 그대로다. 이 밖의 원천 타입과 1..9999년 밖의
날짜는 해시할 수 없어 승격을 거부한다. 엔진은 해시를 SQL로 계산하고, JSON이 escape하는 문자가 든
행만 같은 형식으로 Python에서 계산한다. 원본 값은 숫자 규칙이 바꾼 뒤에도 이 해시와 원천 자료실에
그대로 남는다.

등록된 매퍼는 다음과 같다. `eodhd.bars@1`은 EODHD 일봉(`provider_symbol`, `date`, binary64 OHLCV,
`currency`, `retrieved_at`)을 canonical unadjusted `prices`로 옮긴다. instrument는 assertion key
(`eodhd`, `eodhd_symbol`, `provider_symbol`)를 세션 날짜의 현지 0시에 해석하고, `interval`은 `1d`,
`bar_end_us`는 인자 `timezone`에서 그 세션 날짜의 마지막 microsecond, 수집 시각은 `retrieved_at`이다.
다섯 값이 모두 유한하고 음수가 아니면 `present`, 모두 비었으면 `missing`, 그 밖은 값 없이
`invalid`다. 시간 입력은 `session_date` 하나다.

`eodhd.bulk_quarantine@1`은 EODHD 거래소 전체 일간 내려받기에서 수집기가 보류한 행(내려받기 하나의
테이블, `reason`과 공급자 행의 JSON 텍스트 `source_row_json`)을 같은 canonical unadjusted `prices`로
옮긴다. 공급자가 그 거래소 응답이 부분이라고 경고한 `provider_reported_partial` 행만 세션 날짜를
갖고, 모든 행에 행 flag `provider_reported_partial`이 달린다. 다른 보류 이유의 행, `YYYY-MM-DD`가
아닌 날짜, 읽을 수 없는 JSON은 세션 날짜가 없어 거부되거나 파티션 날짜 없음으로 거부된다. token은
`<code>.<exchange_short_name>`이고, JSON에 통화가 없으므로 인자 `currencies`가 거래소 코드마다 ISO 통화를
선언하며 선언하지 않은 거래소의 행은 통화가 없어 거부된다. 행에 수집 시각이 없으므로 수집 시각은 그
원천의 `sl:` 연결 시각이다. JSON 숫자는 binary64로 읽고, 2^53을 넘는 정수나 숫자가 아닌 JSON 값은
`invalid`다. 나머지(해석, `bar_end_us`, 값 상태, 시간 입력)는 `eodhd.bars@1`과 같다.

`eodhd.bars_quarantine@1`은 EODHD 일봉 이력 내려받기에서 수집기가 보류한 행(내려받기 하나의
테이블, 수집 작업 `source_fingerprint`, `reason`, 심볼 없는 공급자 행 JSON `source_row_json`)을 같은
canonical unadjusted `prices`로 옮긴다. 심볼과 작업 완료 시각은 그 원천 manifest의 `jobs` 목록
(`fingerprint`, `symbol`, `completed_at_utc`)에서 온다. 목록에 한 번만 나오는 fingerprint만 심볼을 갖고,
심볼이 없는 행은 통화도 없어 거부된다. 보류 이유가 `invalid_price_or_volume`이나 `inconsistent_ohlc`인
행만 세션 날짜를 갖고, 모두 값 없는 `invalid` bar가 된다(가격이 모두 0인 행, 시가가 저가보다 낮은 행).
다른 보류 이유의 행은 세션 날짜가 없어 거부된다. 수집 시각은 시간대가 붙은 ISO 완료 시각이고, 그렇지
않으면 원천의 `sl:` 연결 시각이다. 통화는 심볼의 거래소 접미어에 대한 인자 `currencies` 값이다.

`eodhd.bars_adjusted@1`과 `eodhd.bulk_quarantine_adjusted@1`은 같은 두 원천 모양에서 공급자
`adjusted_close`를 close 전용(`fields='close'`) reference 가격(basis `total_return`)으로 옮긴다. 숫자 열은
`close` 하나이고 값 상태 규칙은 그 한 값에 같게 적용한다.

`calendar.declared@1`은 [선언 달력](#선언-달력)의 원천 테이블(날짜마다 `calendar_id`, `venue`,
`timezone`, `session_date`, `status`, 현지 `open_local`·`close_local`, `declared_at`)을
`calendar_sessions`로 옮긴다. `open_at_us`·`close_at_us`는 현지 시각을 그 행의 `timezone`에서 DuckDB
ICU 시간대 자료로 푼 값이고, `timezone_version`은 그 자료를 가리키는 매퍼 인자다. 개장 행은 두 현지
시각이 그 날짜 안에서 개장이 먼저여야 하고 휴장 행은 둘 다 비어야 하며, 그 밖의 행은 `status`가 비어
필수 열 누락으로 거부된다. 시간 입력은 `public_by` 하나다.

`dart.fnltt@1`과 `dart.fnltt_filings@1`은 [DART 재무제표 응답](#dart-재무제표-응답)을 발행인 단위
`fundamentals`와 `filings`로 옮긴다.

US 가격 매퍼는 모두 instrument를 Norgate asset ID나 공급자 심볼로 인자 `timezone`(US는
`America/New_York`)의 세션 날짜 0시에 해석하고, `interval`은 `1d`, `bar_end_us`는 그 날짜의 마지막
microsecond, 시간 입력은 `session_date` 하나다. 값 일부만 있는 bar는 값 없이 `invalid`이고 이웃 값으로
채우지 않는다.

- `norgate.prices_none@1`은 `norgate.history_export@1`로 편입한 비조정 내보내기(`norgate-history-csv`의
  `bars`)를 canonical unadjusted USD `prices`로 옮긴다. `database`가 `US Equities`·`US Equities Delisted`인
  행만 세션 날짜를 가지며, 날짜는 실제 날짜인 `YYYY-MM-DD` 원문일 때만 읽는다. 그 밖의 행은 필수 열 누락으로
  거부된다. 다섯 값은 CSV 원문 그대로 `decimal_text@1`에 넘기므로 `1.6357e+06` 같은 잘린 거래량은 그 값과
  `volume_precision_limited` flag로 남는다. 이 flag는 열 이름과 함께 기록되며, `2.914e+06`처럼 지수로 쓴
  가격 열에도 붙는다(flag의 열이 그 값을 가리킨다). 다섯 원문이 모두 음수가 아닌 십진수이면 `present`, 모두
  비었으면 `missing`이다. 행에 수집 시각이 없으므로 수집 시각은 원천의 `sl:` 연결 시각이다.
- `norgate.prices_adjusted@1`은 Norgate 조정 가격 part(`assetid`, 0시의 nanosecond `date`, binary32 OHLCV,
  `adjustment_type`)를 reference USD `prices`로 옮긴다. `CAPITAL`은 `split_adjusted`, `TOTALRETURN`은
  `total_return`이고, 다른 조정 유형은 basis가, 시각이 0시가 아닌 날짜는 세션 날짜가 없어 거부된다. binary32
  값은 `float_shortest@1`에 넘긴다. 다섯 값이 모두 유한하고 음수가 아니면 `present`다.
- `norgate.reference_closes@1`은 원천 자료실의 Norgate 기준 시리즈 표(`assetid`, `date`, binary64 `close`,
  내보내기 행 `raw_row_json`)를, `norgate.reference_history@1`은 미국 주식이 아닌 Norgate 데이터베이스(지수,
  경제 지표, 외환 현물, 상품)의 history 내보내기를 close 전용(`fields='close'`) reference `prices`로 옮긴다.
  close는 내보내기의 `Close` 원문을 `decimal_text@1`에 넘긴다. 기준 시리즈 표의 close는 그 원문이 저장된
  double과 같고 원문 `Date`가 행 날짜와 같을 때만 `present`이다. history 내보내기의 주식 행은 세션 날짜가
  없어 거부되므로 주식 내보내기가 기준 시리즈로 승격되지 않는다. 가격 도메인은 음수를 담지 않는다. 음수
  수준이 있는 시리즈(등락 종목 수, 스프레드, 금리, 변화율)의 음수가 아닌 날만 남기면 값이 있는 곳에서는
  완전해 보이는 검열된 시리즈가 되므로, `norgate.reference_history@1`은 인자 `signed`(증가 순서의 asset ID
  목록, `signed_series`가 내보내기에서 계산)에 든 시리즈의 행을 하나도 고르지 않고 계획은 그 행을
  `unselected_rows`로 센다. 그 시리즈는 부호 있는 값 도메인의 몫이다(Linear AAS-67). 그 밖의 음수 close는
  값 없이 `invalid`다. 시리즈 수준은 금액이 아니므로 통화는
  `XXX`(ISO 4217 "통화 없음"), basis는 Norgate가 낸 그대로인 `unadjusted`다. 1970년 이전 날짜(1890년대
  지수)의 시간 입력은 1970-01-01로 올린다. 더 늦은 날짜도 그 행이 공개된 시점의 상한이다.
- `fmp.eod_non_split@1`은 FMP 동결 snapshot의 non-split-adjusted 일봉(`symbol`, `date`, binary64
  `adjOpen`..`adjClose`, 정수 `volume`, `retrieved_at_utc`)을 unadjusted USD reference `prices`로 옮긴다.
  instrument는 (`fmp`, `fmp_symbol`, `symbol`)로 해석하고 수집 시각은 `retrieved_at_utc`다. FMP는 같은 bar를
  여러 번 수집했으므로, bar마다 응답을 수집 시각순으로 놓고 값이 같은 연속 응답을 한 revision으로 본다.
  인자 `revision`(1부터)은 bar마다 그 번호 revision의 첫 응답을 고른다. revision 1, 2, …를 이어진
  generation으로 승격하면 정정은 그것을 수집한 시각부터 알려진 SUPERSEDE가 되고 바뀌지 않은 bar는 다시 쓰지
  않는다. 같은 시각에 수집한 서로 다른 응답은 revision 1에 모두 들어가 자연키 반복으로 거부되고, 그 bar는
  이후 revision에 들어가지 않는다.

텍스트 값 원문을 읽는 매퍼(FRED·KR 거시, 텍스트 FX, Norgate history 내보내기 가격)는 값 판정 하나를
함께 쓴다. 원문이 `decimal_text@1`이 바꾸는 십진수(부호, 소수점, 지수 허용)이고 매퍼의 부호 범위(거시 값은
제한 없음, 가격과 거래량은 0 이상, FX 환율은 0 초과) 안이면 `present`, 비었거나 없거나 FRED의 `.`이면
`missing`, 그 밖은 `invalid`다. 같은 Norgate history 행은 `norgate.fx_history@1`과
`norgate.reference_history@1`에서 0인 값만 달리 판정된다. 같은 내보내기 모양을 읽는 세 매퍼
(`norgate.prices_none@1`, `norgate.reference_history@1`, `norgate.fx_history@1`)는 `norgate-history-csv-`
원천만 받는다.

분류 매퍼 `norgate.classification@1`, `sec.sic@1`, `kind.industry@1`, `kind.industry@2`는 [분류](#분류)가 소유한다.

기업행동과 상장 상태 매퍼(`norgate.dividends@1`, `norgate.capital_adjustments@1`, `fmp.dividends@1`,
`fmp.splits@1`, `norgate.status@1`)는 [기업행동과 상장 상태 매퍼](#기업행동과-상장-상태-매퍼)가 소유한다.

`sec.submissions@1`과 `sec.companyfacts@1`은 [SEC 공시와 재무](#sec-공시와-재무)를 발행인 단위
`filings`와 `fundamentals`로 옮기며, 재무 매퍼는 pin한 공시 generation을 참조로 조인한다.

예정된 매퍼(Linear AAS-77, 대응표 DV-413~416): `actions.us.eodhd`·`actions.kr.eodhd`를 만드는 Qveris
일간 `splits`·`dividends` 매퍼, `status.kr.kind`를 만드는 KIND 상장 상태 매퍼, `filings.kr.dart`에
공시 목록(`list.json`)을 더하는 `dart.list`. identity 원천을 읽는 매퍼는 typed generation이
아니라 등록 문서를 만든다. `eodhd.kr_symbol`, `kind.listings`, `dart.corp_codes`는
[KR 등록](#kr-등록)이, `norgate.master`, `eodhd.us_symbol`, `fmp.profile`, `sec.tickers`는
[US 등록](#us-등록)이 소유한다. universe 원천을 읽는 `norgate.index_membership`과 `norgate.listings`도
generation이 아니라 universe 문서를 만들며 [universe 등록](#universe-등록)이 소유한다.

자연키가 겹치는 원천 행 두 개는 승격을 거부한다. 어느 쪽을 고를지 추정하지 않는다.
instrument는 pin한 identity snapshot에서 매퍼의 assertion key와 token이 같고, 해석 시각이 유효
구간 안이며, 아직 정정되지 않은(`known_to_us`가 null인) member로 해석한다. 해석되는 instrument가
없거나(`unresolved`) 둘 이상인(`ambiguous`) 행은 승격하지 않고 수와 원천 token을 미해결 보고에
남긴다. 분류 매퍼의 identity key는 같은 방법으로 `subject_id`를 해석한다. 티커·경로·날짜로
instrument ID를 만들지 않는다.

### 거시와 FX 매퍼

거시·FX 매퍼는 숫자를 원천 텍스트 그대로 내고 명세는 `decimal_text@1`을 쓴다. 텍스트가 일반 십진수일
때만 `present`이고, 텍스트가 없거나 비었거나 FRED의 `.`이면 `missing`, 그 밖의 텍스트는 값 없이
`invalid`다. 다듬거나 고치지 않는다. 시장 시각은 0 이상이므로 시간 입력 날짜가 1970-01-01보다
이르면 1970-01-01을 시간 입력으로 낸다. 더 늦은 날짜도 그 행이 공개된 시각의 상한이므로 규칙의
주장은 그대로 참이고, 원래 날짜는 도메인 열과 원천 행에 남는다.

`fred.alfred@1`은 ALFRED 관측 테이블(`series_id`, `observation_date`, vintage의 `realtime_start`,
`value` 텍스트, 수집 시각 `retrieved_at_utc`)을 `macro_observations`로 옮긴다.

- record는 시계열 하나의 관측 하나다. 관측 행은 단위를 싣지 않고 FRED는 단위를 시계열 metadata로
  공표하므로 `unit`은 `as_published`(그 vintage에 FRED가 공표한 단위)다. 기준 연도나 배율이 바뀐 vintage도
  같은 record의 새 revision이다.
- `source_vintage_start`는 `realtime_start`이고 `source_vintage_end`는 null이다. vintage는 다음 vintage가
  시작할 때 끝나며 chain은 그것을 다음 revision으로 기록한다. 나중에 수집한 원천의 닫힌 `realtime_end`는
  그 vintage가 현재였던 동안에는 알 수 없던 사실이므로 그 revision에 옮기지 않는다. 원천 행과 해시에는 남는다.
- generation 하나는 record마다 revision 하나이므로 파티션 하나는 관측마다 vintage를 많아야 하나 담는다.
  파티션 열은 `realtime_start`다. 백필은 `mappers.fred.vintage_partitions`가 원천 테이블에서 계산한
  구간(같은 관측의 연속한 두 vintage 사이마다 경계가 있는 가장 적은 순서 있는 `[from, to)` 목록)을 순서대로
  하나씩 승격하므로 각 vintage는 직전 vintage의 SUPERSEDE가 된다. 유지보수는 vintage 날짜 하루를
  generation 하나로 승격한다.
- 시간 입력은 `vintage_start`(`realtime_start`) 하나다. 명세는 두 시점 열 모두 `local_day_end@1`(근거
  `revision`, FRED 시간대)를 쓰므로 첫 vintage와 정정 모두 자기 vintage 날짜의 끝부터 알려진다.

`bok.observations@1`과 `oecd.observations@1`은 legacy `korea.public_response@1`이 편입한 관측
테이블(`series_id`, `period`, `value`, `units`, `unit_multiplier`, `base_period` 텍스트)을 옮긴다.
`observation_period`는 `period`의 첫날이다(`YYYY-MM-DD`는 그날, `YYYY-MM`은 그 달, `YYYY-Qn`은 그 분기,
`YYYY`는 그해). 다른 형식은 필수 열이 비어 승격이 거부된다. `unit`은 `units` 뒤에 원천이 밝힌
`;base=<base_period>`, `;multiplier=<unit_multiplier>`, `;regime=<regime>`을 붙인 것이다. 기준 시점이나
배율이 다른 값, 다른 정의로 정한 값(BOK의 2008년 이전 콜금리 목표 `call_target`과 기준금리 `base_rate`)은
같은 수치의 정정이 아니라 다른 단위다. `units`가 없는 행은 거부된다. 응답에 vintage와 공개 시각이 없으므로
시간 입력을 선언하지 않고, 명세는 두 시점 열에 `unknown_null@1`을 쓴다. 그래서 strict 읽기는 이 행을
고르지 않는다. 수집 시각은 원천의 `sl:` 수집 시각이다.

FX 매퍼는 인자 `series`(원천의 시계열 이름), `base`·`quote`(대문자 세 글자 통화 코드, 서로 다름),
`timezone`(IANA)을 받아 한 통화쌍을 승격한다. 같은 원천의 다른 시계열 행은 고르지 않는다. `rate`는 base
한 단위당 quote 수량이다. `fixing_at_us`는 관측 날짜의 `timezone` 기준 마지막 microsecond이고 시간 입력은
그 날짜인 `fixing_date`다. reader가 FX를 거르는 날짜는 `fixing_at_us`의 UTC 날짜이므로 UTC보다 서쪽
시간대의 날짜는 다음 UTC 날짜로 읽힌다.

- `timezone`의 날짜 끝은 그 날짜의 고시나 종가가 그보다 앞설 때만 고시 시각의 상한이다. 고시 자체의
  시간대(FRED의 New York 정오 매입률은 `America/New_York`)를 쓰고, 공급자가 종가 시각을 밝히지 않으면
  어느 시간대에서든 그 날짜로 적힌 시각보다 늦은 `Etc/GMT+12`(가장 늦은 날짜 끝)를 쓴다. 호가 통화의
  시장 시간대는 그것만으로 상한이 아니다. 역내 USDKRW는 다음 서울 날짜 02:00까지 거래된다.
- 고시 날짜는 공개 시각을 말하지 않는다. 명세는 공급자가 그 날짜 끝까지 고시를 공개할 때만
  `fixing_date`에 근거 `record`의 `local_day_end@1`을 쓴다. FRED는 한 주의 고시를 다음 H.10 발표(며칠 뒤)에
  공개하므로 `fred.fx_series@1`은 두 시점 열에 `unknown_null@1`을 쓰고 strict 읽기는 그 행을 고르지 않는다.
  발표 지연을 싣는 규칙이나 시간 입력이 생기면 그 규칙으로 바꾼다. 수집 시각 clamp는 백필에서 상한을
  낮추지 못하므로 지연을 대신하지 않는다.

- `norgate.fx_closes@1`은 Norgate 기준 시리즈 테이블(`symbol`, `date`, binary64 `close`, 내보내기 행
  원문 `raw_row_json`)을 읽는다. `rate`는 binary64 값이 아니라 내보내기가 쓴 `Close` 텍스트다. 원문의
  `Date`가 행의 `date`가 아니면 `invalid`다. 그 밖에는 텍스트가 양의 십진수이고 그 double이 `close`와 같을
  때만 `present`이고, 둘 다 없으면 `missing`, 나머지는 `invalid`다. 원천에 수집 시각이 없으므로 수집 시각은
  `sl:` 수집 시각이다.
- `fred.fx_series@1`(`fred-series-csv-*` 원천)은 legacy `fred.series_csv@1`이 편입한 텍스트 행
  (`series_id`, `observation_date`, `value`)을, `norgate.fx_history@1`(`norgate-history-csv-*` 원천)은
  legacy `norgate.history_export@1`이 편입한 `bars`의 텍스트 행(`symbol`, `date`, `close`)을 읽는다. 0
  이하의 값은 `invalid`이고, `YYYY-MM-DD`가 아니거나 없는 날짜는 필수 열인 `fixing_at_us`를 비워 승격을
  거부한다.

### 기업행동과 상장 상태 매퍼

기업행동 매퍼는 `corporate_actions`를 낸다. `effective_date`는 가격 조정이 시작되는 ex-date이고
`ex_date`와 같으며, 시간 입력은 `ex_date` 하나다. 명세는 두 시점 열에 근거 `record`의
`exdate_open@1`(pin한 `sessions.xnys`의 ex-date 개장 시각)을 쓴다. `action_id`는
`<action_type>:<ex-date>`다(FMP 분할은 아래처럼 `split:<ex-date>`). 비율 행동(`split`, `stock_dividend`, `capital_adjustment`)의 `ratio`는 옛
주식 한 주당 새 주식 수이고, 현금 행동(`dividend`)의 `amount`는 그 ex-date에 지급한 주당 현금이다.

Norgate는 기업행동 표를 따로 내지 않는다. 두 Norgate 기업행동 매퍼는 Norgate 조정 가격 part(`assetid`,
0시의 nanosecond `date`, binary32 `close`·`unadjusted_close`·`dividend`, `adjustment_type`)의 `CAPITAL` 행만
asset마다 날짜순으로 읽는다. 한 asset의 시리즈가 다음 part로 이어질 수 있으므로 명세는 모든 `CAPITAL`
part를 함께 pin한다. `CAPITAL` close는 비조정 close를 그 뒤 모든 자본 사건의 주식 비율의 곱으로 나눈
값이고, `dividend`는 같은 자본 기준의 현금을 ex-date 직전 세션의 행에 싣는다. 매퍼는 이웃 행을 읽으므로
파티션 날짜가 늘 null이고, 파티션을 둔 명세는 모든 행이 파티션 날짜 없음으로 거부된다. instrument는
(`norgate`, `norgate_assetid`)로 ex-date의 현지 0시에 해석하고, 수집 시각은 원천의 `sl:` 연결 시각이다.

- `norgate.dividends@1`은 `dividend`가 0이 아닌 행을 고른다. ex-date는 그 시리즈의 다음 세션, 곧 Norgate
  총수익 시리즈가 배당을 뺀 가격을 쓰기 시작하는 첫 세션이다. 시리즈의 마지막 행에 실린 배당은 ex-date가
  없어 고르지 않는다(계획의 `unselected_rows`에 든다). `amount`는 지급한 그대로의 주당 현금이다. 자본
  기준 배당에 그 행의 `unadjusted_close / close`를 곱하고, 세 입력의 정밀도인 binary32로 반올림해
  `float_shortest@1`에 넘긴다. 통화는 `USD`다. 배당·close·비조정 close 중 하나라도 유한한 양수가 아니면
  `invalid`이고 금액을 남기지 않는다(음수 배당도 그렇다). 다음 행의 `date`가 0시가 아니면 ex-date가 없어
  그 행은 거부된다. 원천 자료실의 `market-canonical-market-data-6af78d02*` 배당 표는 같은
  배당을 배당 행 자신의 날짜와 자본 기준 금액으로만 담으므로 ex-date와 지급액을 낼 수 없어 승격하지 않는다.
- `norgate.capital_adjustments@1`은 자본 계수 `f = unadjusted_close / close`가 바뀌는 세션을 고른다.
  `ratio`는 계수가 있는 직전 행의 `f`를 이 행의 `f`로 나눈 값, 곧 Norgate가 `CAPITAL`에 접는 그 ex-date의 모든 자본
  사건(분할, 병합, 주식배당, 그 밖의 자본 분배)의 주식 비율이며 binary32로 반올림해
  `float_shortest@1`에 넘긴다. 비율이 1과 백만분의 1보다 더 다를 때만 사건이다. binary32 저장은 연속한
  두 행의 `f`를 그보다 적게 움직인다(라이브 part에서 저장만으로 생긴 가장 큰 움직임은 2e-7 미만, 가장 작은
  실제 사건은 1e-5 초과). close나 비조정 close가 유한한 양수가 아닌 행은 계수가 없다. 두 계수 사이에
  계수 없는 행이 끼어 있으면 사건의 세션을 알 수 없으므로, 그 변화를 보인 행에 비율 없는 `invalid` 사건을
  낸다. `date`가 0시가 아닌 행의 사건은 ex-date가 없어 거부된다. `action_type`은 `capital_adjustment`다.
- `fmp.dividends@1`과 `fmp.splits@1`은 FMP 동결 배당·분할 응답(`symbol`, ex-date `date`,
  `retrieved_at_utc`)을 읽는다. instrument는 (`fmp`, `fmp_symbol`, `symbol`)로 해석하고 수집 시각은
  `retrieved_at_utc`다. 응답의 반복과 정정은 `fmp.eod_non_split@1`과 같은 revision 구간(`revision`)으로
  고른다. FMP는 한 회사가 같은 ex-date에 두 번 지급하면 한 응답에 서로 다른 배당 두 개를 싣는다. 뒤 응답이
  어느 항목을 정정하는지 알 수 없으므로, 한 시각에 서로 다른 값을 가진 적이 있는 (symbol, ex-date)는 어느
  revision도 고르지 않는다. 배당은 `dividend`(유한한 양수가 아니면 `invalid`), `recordDate`,
  `paymentDate`를, 분할은 `numerator / denominator`(두 항이 유한한 양수가 아니면 `invalid`)를 binary64로
  `float_shortest@1`에 넘긴다. 분할의 `action_type`은 `splitType`에서 온다: `stock-split`은 `split`,
  `stock-dividend`는 `stock_dividend`, `spin-off`는 `spin_off`, `adr-change`는 `adr_change`, 다른 유형은
  `-`를 `_`로 쓴 그 이름, 없거나 빈 유형은 `unspecified_split`이다. 분할의 `action_id`는 유형과 관계없이
  `split:<ex-date>`이므로 유형을 고친 revision은 앞선 행동을 SUPERSEDE한다. FMP 배당 통화는 US registry가 해석한 US
  상장 심볼의 거래 통화인 `USD`다.

`norgate.status@1`은 Norgate security master(`assetid`, `is_delisted`, `first_date`·`last_date` 텍스트)를
`instrument_status`로 옮긴다. 인자 `event`가 master 행마다 사건 하나를 고른다. `listed`(모든 행)는
`first_date`의 현지 0시에 시작하며, 이 날짜는 Norgate 시리즈의 첫 세션이라 실제 상장보다 늦을 수 있다
(`reason`은 `norgate_first_date`). `delisted`(`is_delisted` 행)는 시리즈의 마지막 세션 `last_date` 다음 날의
현지 0시에 시작한다(`reason`은 `norgate_last_date`). 둘 다 끝이 없다. 시간 입력 `status_date`는 사건을 읽은
날짜이므로 명세가 `local_day_end@1`을 쓰면 상장폐지는 시리즈의 마지막 세션이 끝나기 전에는 알려지지 않는다.
instrument는 그 날짜의 현지 0시에 해석한다. `YYYY-MM-DD`가 아닌 날짜, `first_date`보다 이른 `last_date`,
마지막 표현 가능 날짜(`9999-12-31`)의 상장폐지는 사건의 시작이 없어 거부된다.
master는 snapshot 하나라 파티션 날짜가 없다.

## 결정적 공통 열

공통 열은 원천과 공개된 규칙만으로 재현된다. 값을 꾸며내지 않는다.

| 열 | 규칙 |
| --- | --- |
| `record_id` | 기존 `aas-record-v1`: `sha256(정규 JSON ["aas-record-v1", domain, 자연키 [이름, 값] 목록])` (`market.normalize_rows`) |
| `op` | parent chain의 현재 head와 비교해 정한다(아래) |
| `supersedes_revision_id` | head 조인에서 온다. ASSERT만 null |
| `revision_id` | `sha256(정규 JSON ["aas-revision-v1", dataset_id, record_id, op, supersedes_revision_id, source_row_hash])` |
| `available_at_us`, `revision_known_at_us` | [시간 규칙](#시간-규칙과-소비자-grant)과 그 절의 revision 시점 규칙으로 정한다. 규칙이 없으면 null |
| `ingested_at_us` | 원천 행의 수집 시각 열, 없으면 그 원천의 `sl:` snapshot `retrieved_at_us`(원천 자료실 commit을 완료한 시각) |
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
| ASSERT·SUPERSEDE | 전체 snapshot에 없음, `absent_in_full_snapshot`이고 record가 명세의 `scope` 안 | TOMBSTONE |
| ASSERT·SUPERSEDE | 없음, 그 밖의 경우 | 행 없음 |

TOMBSTONE의 `source_row_hash`는 `sha256(정규 JSON ["aas-tombstone-v1", source_id, table, digest])`,
곧 그 행이 없다는 사실을 담은 원천 테이블의 증거다. TOMBSTONE의 시점은 아래
[revision 시점](#revision-시점)이 정한다. 같은 명세를 두 번 승격하면 두 번째 delta는 비어 있다.

`revision_id`, `source_row_hash`, TOMBSTONE 해시, `request_hash`, `source_id`, `dimensions_hash`의 형식은 고정 입력과
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
| `declared_session_end@1` | 매퍼가 넘긴 선언 상한: 선언 시각과 선언된 그 날짜 session의 끝(개장 session은 마감, 휴장일은 현지 날짜의 마지막 microsecond) 중 이른 값 | 계산 값 | [선언 달력](#선언-달력) |
| `unknown_null@1` | 항상 null | 없음 | 근거가 없는 원천 |

기록되는 시점은 그 행을 받은 시각보다 늦을 수 없다. 받은 bytes는 받은 시각에 이미 공개돼 있었기 때문이다.
그래서 행의 `ingested_at_us`가 규칙의 물리 기준 이상이고 규칙 값보다 이르면 시점은 `ingested_at_us`로
내려가고 행에 `time_clamped_to_ingestion` flag가 남는다. 이 값도 상한이다. `ingested_at_us`가 물리
기준보다 이르면, 예를 들어 세션 종료 전에 받은 일봉이면, 그 행은 승격하지 않고 보류 행으로 보고한다.
수집 시각은 시점을 낮추는 상한으로만 쓰이며 null을 채우지 않는다.

명세는 시점 열마다 규칙 입력의 근거(`basis`)를 선언한다. `revision`은 입력 열이 그 행 자체의 공개
시각이나 공개일을 담는 경우다(SEC `acceptanceDateTime`, ALFRED `realtime_start`, DART 접수일).
`record`는 입력이 record의 날짜인 경우다(세션 날짜, ex-date). `session_close_plus_lag@1`,
`exdate_open@1`, `declared_session_end@1`은 항상 `record`다.

규칙 ID와 버전은 명세에 있으므로 transform hash에 포함된다. 규칙의 계산을 바꾸면 새 버전이 된다.
한 chain의 모든 generation은 열마다 같은 시간 규칙(ID·버전·근거·입력과 달력 pin을 뺀 인자)을 쓴다.
op는 시점을 비교하지 않으므로 규칙이 바뀐 명세로 이어 승격하면 이전 규칙의 시점이 새 규칙의 것처럼
남는다. 그래서 parent 명세와 시간 규칙이 다른 승격은 거부한다. 달력 pin은 규칙이 읽는 근거 자료라서
규칙에 속하지 않는다. 명세는 parent가 pin한 달력 generation이나 같은 달력 dataset chain의 후손
generation을 pin할 수 있다(연말 세션 연장, 임시 휴장 SUPERSEDE). 새 달력으로 시점이 달라지는 기존
행은 다시 쓰지 않고 `time_drift`로 보고하며, 새 행만 새 달력의 시점을 받는다. 규칙을 바꾸려면 [dataset 이름](#공급자별-dataset과-ordered-pin-cutover)에
규칙 세대 `.r<N>`을 붙인 새 dataset을 첫 generation부터 승격한다. reader는 행이 속한 chain의 명세에서
그 행의 시점이 어느 규칙에서 왔는지 안다.

`source_column`이 아닌 규칙에서 나온 시점을 strict PIT 경로에 쓸지는 소비자가 정한다.
입력 binding(`aas-head-binding-v1`, [reader](#대량-게시와-reader))이 허용하는 규칙 `id@version`
목록(grant)을 들고, 그 목록은 binding hash에 포함된다. strict reader는 grant에 없는 규칙의 시점을
알 수 없는 시점(null)으로 취급한다. 그래서 그런 `revision_known_at_us`의 행은 고르지 않고, 그런
`available_at_us`의 정정 행은 알려진 시점부터 이전 head를 지운다. `source_column@1`(기록된 원천 시각)과
`unknown_null@1`(항상 null)은 grant가 필요 없다. inspection과 연구 모드는 grant 없이도 행을 읽는다.
읽기 영수증(`aas-head-read-v1`)은 binding과 grant, generation마다 두 시점 열의 규칙, 그 읽기가
기댄 규칙(`applied_rules`)과 막은 규칙(`withheld_rules`)을 기록하고, run은 그 영수증을 그대로 남긴다.
규칙 하나를 허용하는 일은 그 규칙의 상한을 시점 근거로 받아들인다는 기록이며, 다른 규칙이나
알 수 없는 시점을 허용하지 않는다.

reader는 generation의 규칙 출처를 호출자에게서 받지 않고 그 generation의 보존 증거에서 읽는다.
`dataset_versions.transform_hash`의 raw 문서가 `aas-promotion-v1` 명세이면 그 `time_rules`의 `rule`이
출처다. 그 문서가 `aas-{price,sessions,proxy,observation}-transform-v1` 변환이거나 marker
`request_hash`의 raw 문서가 봉인된 `aas-market-import-v1`이면 시점은 원천 열이나 문서에 기록된
값이므로 `source_column@1`이다. 어느 것도 아닌 generation은 출처가 보존되지 않았으므로 읽지 않는다.
raw에 없는 객체, 출처 형식의 크기 한도(64 MiB)를 넘는 객체, JSON 객체가 아닌 바이트만 "문서 없음"이다.
주소와 hash가 다른 raw 객체는 손상으로 거부하고, 해석한 문서가 호출자 할당에 들어가지 않으면
`ComputeResourceError`로 거부한다.

### revision 시점

ASSERT가 아닌 행은 정정이나 삭제를 담은 원천보다 먼저 알려질 수 없다. 원천의 **증거 시각**은 행이
있으면 그 행의 `ingested_at_us`이고, 행이 없는 TOMBSTONE에서는 pin한 원천들의 `sl:` snapshot
`retrieved_at_us` 중 가장 늦은 값이다. TOMBSTONE의 `ingested_at_us`도 이 증거 시각이다.
두 시점 열은 열마다 다음과 같다.

| op | 시점 |
| --- | --- |
| ASSERT | 규칙 값(위 상한 적용) |
| SUPERSEDE, 근거 `revision` | 규칙 값(위 상한 적용) |
| SUPERSEDE, 근거 `record` | 원천 증거 시각. 규칙 값이 null이면 null |
| TOMBSTONE | 원천 증거 시각. record 날짜의 규칙은 쓰지 않는다. 규칙이 `unknown_null@1`이면 null |

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
| `exact@1` | 원천 값(binary64·binary32·정수)이 정확히 표현될 때만 승인. 아니면 승격 거부 | 없음 |
| `krw_tick@1` | binary64 원천 값의 정확한 십진 전개를 원 단위 정수로 반올림(ROUND_HALF_EVEN). KRW 가격 열(open·high·low·close)에만 적용. 통화가 KRW가 아닌 행은 반올림하지 않고 `refused_number`로 거부 | 정수가 아니던 행 `provider_float_reconstructed`, 나머지가 정확히 0.5이던 행 `decimal_rounding_tie` |
| `float_shortest@1` | 매퍼가 낸 저장 폭(binary32 `FLOAT` 또는 binary64 `DOUBLE`)에서, 저장 값을 유효숫자 p자리로 올바르게 반올림한 십진 값이 같은 폭으로 되돌아오는 가장 작은 p(binary32는 9, binary64는 17까지)의 표현. 소수 12자리를 넘으면 소수 12자리로 ROUND_HALF_EVEN | 결과가 저장된 binary 값과 다르면 `provider_float_storage`, 버린 나머지가 정확히 절반이면 `decimal_rounding_tie` |
| `decimal_text@1` | 원천 텍스트의 십진 값을 그대로 사용. 소수 12자리를 넘으면 승격 거부 | 지수 표기이고 가수의 유효숫자가 7자리 이하면 `volume_precision_limited` |

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
- flag 행은 revision과 (규칙 ID, 규칙 버전, flag)마다 하나이고 `detail`은 그 flag가 가리키는 열
  이름을 정렬해 쉼표로 이은 것이다. 행은 generation marker와 같은 DuckDB 트랜잭션에서 들어간다.
- 같은 generation의 flag 행은 (record, revision, 규칙 ID, 규칙 버전, flag, detail)을 `aas-rowset-v1`으로
  따로 해시해 그 generation의 승격 manifest에 기록한다. flag를 추가·삭제하면 generation 검증이
  실패한다. flag 정정은 새 generation이다.
- reader는 flag를 행과 함께 돌려준다. 소비자는 flag를 grant와 같은 방식으로 binding의 제외 목록에 둘
  수 있고, 그 목록도 binding hash와 읽기 영수증에 들어간다. 제외한 flag가 달린 revision은 공개되지
  않은 것으로 취급한다. 그 값은 고르지 않고, 그 revision이 알려진 시점부터 그것이 대체한 이전 head도
  지운다. 이미 대체된 값을 대신 내주지 않기 위해서다. 연구 모드에서도 제외는 같다.

| flag | 의미 |
| --- | --- |
| `provider_float_reconstructed` | 원화 가격이 정수가 아니어서 `krw_tick@1`으로 반올림함 |
| `decimal_rounding_tie` | 반올림 나머지가 정확히 0.5였음 |
| `provider_float_storage` | 원천이 float 저장값이어서 `float_shortest@1`을 적용함 |
| `time_clamped_to_ingestion` | 규칙 상한이 수집 시각보다 늦어 수집 시각으로 내려감 |
| `provider_reported_partial` | 공급자가 경고와 함께 보낸 부분 응답 파티션의 행. 규칙은 그 행을 읽은 매퍼(`eodhd.bulk_quarantine@1` 등), `detail`은 null |
| `volume_precision_limited` | 원천 거래량의 유효숫자가 잘려 있음 |
| `time_precision_day` | 시점이 `local_day_end@1`의 날짜 단위 상한임 |
| `cross_provider_mismatch` | 같은 instrument·세션·interval·bar_end·basis·currency(가격 키에서 role만 뺀 키)의 다른 공급자 값과 명세 품질 규칙의 허용오차를 넘게 다름(`cross_provider_mismatch@1`: 인자 `reference` generation pin, 비교할 숫자 열 `column`, 상대 허용오차 `tolerance` 십진 문자열로 `abs(값 - 기준) > tolerance × abs(기준)`). 허용오차 안이면 flag가 없다. 한 revision에 규칙마다 flag는 많아야 하나이고, delta 안에서 flag 키가 겹치면 계획이 거부한다 |

dataset version 단위의 판정(행 수 대조, coverage 종료, 교차 대조율)은 기존 state
`quality_checks`가 맡는다. 부분 응답 행은 막지 않고 flag와 함께 승격하며, 그 generation은
`partition_row_count@1` 검사를 남긴다. 세션 날짜마다 부분 응답 원천 행 수, 그중 해석된 행 수(`ok`·`held`),
기준은 parent chain의 완결 날짜, 곧 살아 있는 head 가운데 `provider_reported_partial` flag가 붙은 것이
없는 날짜에서만 나온다. 세션 날짜 이전(같은 날 포함)의 가장 늦은 완결 날짜를 끝으로 하는 31일 안에서
살아 있는 head 수가 가장 많은 완결 날짜가 기준이므로, 앞선 부분 응답 날이나 짧은 완결 날 하나가 기준을
낮추지 않는다(이력 뒤의 부분 응답 날은 모두 이력의 종목 수와 대조된다). 날짜마다 기준 날짜, 기준 수,
그 날짜에 마지막으로 head를 쓴 generation(`reference_generation`)이 남는다. 결과는 해석된 행이 기준보다 적은 날짜가 있으면 `below_reference`, 기준이 하나도 없으면 `no_reference`,
그 밖은 `at_least_reference`이고 `reason`은 날짜별 수의 정규 JSON이다. 검사는 승격 manifest에 들어가므로
복구도 같은 행을 쓴다. delta가 비어 게시되지 않은 계획은 보고에만 남는다. 소비자는
`provider_reported_partial`을 binding의 flag 제외 목록에 두어 그런 revision을 읽지 않을 수 있다. 잘못된
가격은 `value_state='invalid'`로 승격하고 값을 0이나 이웃 값으로 채우지 않는다.

## 공급자별 dataset과 ordered-pin cutover

dataset 이름은 `<domain>.<market>.<provider>[.ref][.r<N>]`다. 한 dataset의 chain에는 한 공급자의 자료만
들어간다. `.r<N>`은 시간 규칙 세대다. 첫 세대는 접미사가 없고, 시간 규칙을 바꿀 때마다 N이 2부터 오른다.
이전 세대 dataset은 그대로 남고 소비자는 새 binding으로 옮긴다. 공급자의 정정은 그 공급자의 chain 안에서 SUPERSEDE로 남고 다른 공급자의 이력을 바꾸지
않는다. `.ref`는 `price_role='reference'` 자료(공급자 조정 가격, 기준 지수)다.

여러 공급자를 잇는 일은 소비자의 binding이 한다. 한 역할이 순서 있는 pin 목록을 들고 각 pin은
`[from, to)` 날짜 구간을 가진다. 첫 pin의 `from`과 마지막 pin의 `to`만 열려 있을 수 있다.

- 구간은 겹치지 않고 빈틈 없이 순서대로 이어진다(앞 pin의 `to`가 다음 pin의 `from`). 각 날짜에는
  정확히 한 pin만 적용되고, pin은 자기 구간 밖의 행을 돌려주지 않는다.
- 같은 generation이 떨어진 두 구간의 pin이 될 수 있다(정규 공급자 → 공백 기간의 보충 공급자 → 정규
  공급자). 투영은 pin 순번마다 따로 한다.
- pin 구간 안의 날짜에 그 pin의 head가 없으면 다른 pin으로 대체하지 않고 누락(`missing_<domain>`)으로,
  어느 pin도 덮지 않는 날짜는 `outside_cutover`로 coverage에 보고한다.
- 같은 날짜에 두 공급자 값을 섞거나 평균내지 않는다.
- cutover 날짜와 pin 목록은 binding hash에 포함된다. 공급자를 바꾸려면 새 binding을 만든다.

FX처럼 여러 원천이 같은 시계열을 내는 경우에도 우선순위는 같은 방식으로 소비자 pin이 정한다.

## 동결 원천

구독이 끝났거나 공급이 멈춘 원천은 동결 원천이다. 보존한 원문과 원천 자료실 자료로만 generation을
만들고, coverage 종료일을 `quality_checks`에 기록한다.

- FMP는 동결 원천이다. FMP 자료는 `*.fmp.ref` dataset의 `reference` 역할로만 승격하며 canonical
  가격이나 strict 실행 입력이 되지 않는다. 수집기의 새 호출은 없다.
- Norgate의 지수 구성, 기준 시리즈, 기업행동은 마지막 내보내기 날짜에서 coverage가 끝난다.
  이후 구간은 다른 공급자의 dataset이 cutover로 잇는다.

## 선언 달력

거래소 달력은 `aas-calendar-declaration-v1` 선언 문서 하나가 한 venue의 `[from, to)` 모든 날짜를
진술한다. 문서는 8 MiB 이하의 엄격한 UTF-8 JSON이며 알 수 없는 필드·빠진 필드·중복 키를 거부한다.

| 필드 | 의미 |
| --- | --- |
| `calendar_id`, `venue` | 네 글자 대문자 MIC. dataset은 `sessions.<calendar_id 소문자>`다 |
| `timezone` | 현지 시각을 푸는 IANA 시간대 |
| `declared_at` | 선언 시각(UTC, `Z`로 끝남) |
| `from`, `to` | 선언 범위 `[from, to)` |
| `sources` | 선언의 근거 목록 |
| `regimes` | `[from, to)`를 빈틈없이 순서대로 덮는 구간. 구간마다 개장 요일(`mon`..`sun`)과 정규 현지 시간(`HH:MM` 개장·마감, 같은 날 안에서 개장이 먼저) |
| `closed` | 구간의 개장 요일인데 열지 않는 날짜, 오름차순 |
| `sessions` | 구간과 시간이 다르거나 구간이 열지 않는 요일에 여는 날짜와 그 시간, 오름차순 |

`closed`와 `sessions`는 겹치지 않고 구간이 이미 말하는 내용을 반복하지 않으므로 같은 일정은 한
가지로만 적힌다. 문서에 없는 날짜는 구간의 요일 규칙을 따르며, 구간이 열지 않는 요일은 휴장이다.

`aas calendar refresh`는 선언 bytes를 `raw/`에 두고 그 bytes를 원본 파일로 한 내용 원천
(`calendar-declared-sessions-<hex>`, 테이블 `sessions`)을 commit한다. 테이블은 범위의 날짜마다 한 행이고
현지 벽시계 시각만 담으므로 행은 문서만으로 정해진다. 그 원천을 `calendar.declared@1`로 head의 자식
generation에 승격하며, 두 시점 열은 `declared_session_end@1`(근거 `record`, 입력 `public_by`)이다.

- `public_by`는 선언한 행이 처음 공개됐을 수 있는 가장 늦은 시각이다. 선언 시각과 그 날짜 자체의 끝
  (개장 session은 마감, 휴장일은 현지 날짜의 마지막 microsecond) 중 이른 값이다. 한 날짜에 venue가
  열었는지와 그 시간은 그 session이 끝나면 사실이 되므로, 오늘의 선언은 과거 날짜에 대해 그보다 늦은
  시점을 진술하지 않고 미래 날짜에 대해서는 선언 자체가 공개한 시점만 진술한다. 이 상한은 원천이 기록한
  시각이 아니라 record 날짜에서 계산한 값이므로, strict reader는 binding이 `declared_session_end@1`을
  grant할 때만 쓰고 읽기 영수증에 그 grant가 남는다.
- 같은 선언을 다시 갱신하면 원천을 재사용하고 delta가 비어 아무것도 쓰지 않는다. 바뀐 날짜(임시 휴장,
  시간 변경, 정정된 과거 일정)는 새 generation의 SUPERSEDE이고 새 날짜(다음 해)는 ASSERT다. 이전
  generation과 그 pin은 그대로 검증되고 그대로 읽힌다. 새 선언으로 `public_by`만 달라지는 변하지 않은
  행은 다시 쓰지 않고 `time_drift`로 보고한다.
- 근거가 `record`이므로 SUPERSEDE의 두 시점은 [revision 시점](#revision-시점) 규칙대로 정정 선언을 담은
  원천의 증거 시각(AAS가 그 선언을 commit한 `sl:` link 시각)이다. 정정은 그 선언이 있기 전에 알려지지
  않고, 두 선언 사이 cutoff의 strict 읽기는 이전 선언의 일정을 돌려주며, 정정 revision의 시점은 그것이
  대체하는 revision보다 이르지 않다. 그래도 계획에 stale 행이 남으면 갱신은 아무것도 승격하지 않고
  거부한다. 이미 commit한 선언 원천은 내용 원천이라 남고, 오류가 그 원천 ID를 알리며 다음 갱신이 재사용한다.
- head를 만든 선언보다 `declared_at`이 이른 선언, 같은 `declared_at`의 다른 내용, 다른 달력·venue·시간대,
  현재보다 늦은 `declared_at`, head 선언이 진술한 날짜를 모두 덮지 않는 범위는 거부한다. 그래서 오래된
  선언이 새 선언의 정정을 되돌리지 못하고, 진술된 날짜가 새 선언 밖에 남지 않는다.
- `--plan`은 아무것도 쓰지 않는다. 원천이 commit돼 있으면 승격 계획을, 아니면 head 선언과의 날짜 단위
  비교를 보고하고, 선언이 갱신 시점의 다음 해 말까지 덮는지 함께 보고한다.

패키지는 XNYS·XKRX의 1990-01-01..2027-12-31 선언을 싣는다. 두 선언은
`scripts/calendar_declarations.py`가 `exchange_calendars` 일정에 AAS가 근거와 함께 기록한 정정(선거일
휴장, KRX 휴장일 목록, 원천 자료실 일봉의 거래 흔적, 수능일 시간)을 적용해 만든다. 1998-12-07 이전
KRX 토요일 session은 확인되지 않은 반일 마감 대신 평일 마감을 선언하며, 이 값은 실제 마감보다 늦은
상한이다. 원천 자료실의 관측 거래일은 선언의 대조 근거이고 선언에 섞이지 않는다.
`scripts/calendar_compare.py`는 market 파일을 읽기 전용으로 열어 패키지 선언과 일봉 원천의 거래일을
비교한다. 날짜와 거래량은 타입 값이든 내보내기 원문이든 읽으며, 수로 읽히지 않는 거래량은 없는 것으로,
날짜로 읽히지 않는 행은 session이 아닌 `undated_rows`로 센다. 거래일은 거래량이 있는 행 수가 앞뒤 30일 최대값의 5% 이상인 날짜이고, 보고는 선언 session 중
거래가 없는 날짜(행 없음과 얇은 거래), 선언이 닫은 거래일, 거래 흔적 없이 행만 있는 휴장일 수다.

## 분류

분류는 주체(instrument 또는 issuer)가 공급자의 분류 체계에서 갖는 값이다. 코드는
`storage/promotion/mappers/classifications.py`이고 `classifications`(v2) 도메인으로 승격한다.

- 분류 원천은 모두 snapshot이다. 주체의 지금 분류만 말하고 그 분류가 언제 시작됐는지는 말하지 않는다.
  그래서 행의 `effective_from`은 snapshot 날짜이고 `effective_to`는 null이며, 과거로 소급하지 않는다.
  이후 snapshot은 자기 날짜의 행으로 다시 승격한다. 자연키에 `effective_from`이 들어가므로 같은
  dataset의 다음 generation에서 ASSERT가 되고 앞의 행은 그대로 남는다. 읽는 쪽은 cutoff에 알려진 행
  가운데 묻는 날짜 이하에서 `effective_from`이 가장 늦은 행을 고른다.
- `scheme`은 공급자 분류 이름, `code`는 그 값, `label`은 공급자 원문이다. 체계 사이를 번역하지 않는다.
- 주체 ID는 [identity 등록](#identity-등록과-chunked-문서)과 같은 발급 규칙을 따른다. 주체의 영구
  anchor(Norgate asset ID, SEC CIK)를 싣는 원천은 매퍼가 SQL로 같은 `mint` 값을 계산하고 identity
  snapshot을 pin하지 않는다. 코드만 싣는 원천(KRX 단축코드)은 identity key로 pin한 snapshot에서 해석한다.
  티커·이름·날짜로 주체 ID를 만들지 않는다.
- 필수 값이 없거나 형식이 틀린 행은 주체나 코드를 비워 필수 열 누락으로 거부되고, 고치지 않는다.
  분류가 아예 없는 원천 행(SIC나 그 설명이 없는 SEC 회사, 업종이 빈 KIND 행)은 행을 내지 않고 보고의
  `unselected_rows`에 남는다.
- 시간 입력은 snapshot 날짜 `as_of`와, 원천이 기록하면 수집 시각 `observed_at`이다. 날짜만 있는
  snapshot의 시점은 `local_day_end@1(as_of)`이므로 strict 읽기는 그 규칙을 grant한 소비자에게만 보이고,
  수집 시각이 있는 snapshot은 `source_column@1(observed_at)`부터 알려진다. 어느 쪽이든 snapshot 전
  cutoff의 읽기는 그 행을 돌려주지 않는다.
- 파티션 날짜는 Norgate가 `first_date`, SEC가 `latest_filing_date`의 `YYYY-MM-DD` 텍스트이고, KIND가
  수집 시각의 Asia/Seoul 날짜다. 파티션 날짜가 없는 행은 `partition`이 있는 명세에서 거부된다.

| 매퍼 | 원천 | 행 |
| --- | --- | --- |
| `norgate.classification@1` | Norgate security master(`assetid`, `subtype1`..`subtype3`, `exchange`, `exchange_full`, 텍스트 `first_date`·`last_date`). 인자 `scheme`과 내보내기 날짜 `as_of` | instrument `mint('norgate_assetid', assetid)`. `norgate.security_type`: code는 유형 경로 `subtype1 > subtype2 > subtype3`(있는 단계만, 건너뛴 단계 없이), label은 가장 구체적인 단계. `norgate.exchange`: code는 `exchange`, label은 `exchange_full` |
| `sec.sic@1` | `aas import sec-companies`가 SEC submissions archive에서 commit한 `companies` 테이블. 인자 `as_of`는 archive 수집 날짜 | issuer `mint('sec_cik', member의 10자리 CIK)`. code는 네 자리 SIC, label은 `sicDescription` |
| `kind.industry@1` | `aas identity kr-import`가 commit한 KIND 상장 목록(`short_code`, `industry`, `retrieved_at_utc`). 인자 없음 | instrument는 identity key(`kind`, `krx_short_code`)를 수집 시각에 해석한 값. `as_of`는 그 수집 시각의 Asia/Seoul 날짜. KIND 업종은 코드가 없으므로 code와 label이 모두 업종 원문 |
| `kind.industry@2` | `kind.industry@1`과 같은 열. 인자 없음 | `kind.industry@1`과 같되, (`short_code`, `industry`, `retrieved_at_utc`)가 같은 원천 행은 한 번만 매핑한다. 그중 원천 행 hash가 가장 작은 행, 같으면 가장 낮은 (`_aas_pin`, `_aas_ordinal`)의 행이 매핑되고 provenance도 그 행의 것이다 |

- Norgate: asset ID가 양의 정수(18자리 이하)가 아니거나 `first_date`·`last_date`가 `as_of`보다 늦은
  행(선언한 내보내기 날짜와 모순)은 주체가 없고, 유형 단계가 비거나 건너뛴 행과 거래소 이름이 빈 행은
  코드가 없다. 날짜로 읽을 수 없는 날짜 텍스트는 모순의 근거로 쓰지 않는다.
- SEC: member 이름이 `CIK##########.json`이고 문서의 `cik`가 같은 CIK(0 채움 무시)를 말하며 최신
  공시일(`latest_filing_date`)이 `as_of`보다 늦지 않을 때만 issuer가 나온다. 쪽 나눈 이력 member와
  다른 CIK를 말하는 문서는 주체가 없다. 네 자리가 아닌 SIC는 코드가 없다. 설명 없이 SIC만 싣는 회사(미배정 `0000` 포함)는 분류가 없는 행이다.
- KIND: `retrieved_at_utc`가 `Z`로 끝나는 UTC 시각이 아니면 수집 시각과 날짜가 없어 거부된다.
  snapshot에서 해석되지 않는 단축코드는 다른 승격처럼 미해결 보고에 남는다.
- KIND 목록은 한 회사를 분류가 읽지 않는 열(행정구역 통합 전후의 `지역` 등)만 다른 두 행으로 싣기도 하고,
  `aas identity kr-import`는 원본 행을 모두 남긴다. `kind.industry@1`은 그런 행마다 분류 행을 내므로 같은
  natural key가 반복돼 승격이 거부된다. `kind.industry@2`는 그런 행을 한 번만 매핑하고 나머지는 선택되지
  않은 원천 행으로 남긴다. 원천 행 hash는 행 내용만으로 정해지므로 수집 순서가 달라도 같은 행과 같은 hash가
  나온다. 한 수집 시각에 같은 단축코드가 다른 업종으로 실린 두 행은 둘 다 매핑돼 반복된 natural key로
  거부되고, 어느 업종도 고르지 않는다.

`aas import sec-companies --source SOURCE_ID [--plan]`은 `sec.submissions_zip@1`로 편입한
`sec-submissions-zip-*` 내용 원천의 member 색인으로 `raw/`의 archive를 읽고, `CIK##########.json`
member마다 한 행을 내용 원천 `sec-submissions-companies-*`(테이블 `companies`)로 commit한다. 원본 파일은
같은 archive이므로 같은 bytes는 늘 같은 원천 ID가 되고 다시 실행하면 재사용한다. 공급자를 호출하지 않는다.

- archive는 원천이 기록한 크기·SHA-256, member는 색인 행의 크기·SHA-256과 같아야 하며 다르면 거부한다.
  UTF-8 JSON 객체가 아닌 CIK member는 archive가 색인과 다르다는 뜻이므로 편입 전체를 거부한다.
- 행은 문서 값을 텍스트로 보존한다: `member`, `member_sha256`, 문서의 `cik`, `name`, `entityType`
  (`entity_type`), `sic`, `sicDescription`(`sic_description`). 문자열은 그대로, 다른 JSON 값은 정규 JSON,
  없는 키는 null이다. `latest_filing_date`는 `filings.recent.filingDate` 텍스트의 최댓값(문서가 나열한
  가장 새 공시일)이고 없으면 null이다.
- `--plan`은 모든 member를 읽어 member 수, CIK 문서 수, SIC가 있는 문서 수, 원천 ID와 commit 여부를
  보고하고 아무것도 쓰지 않는다.

## SEC 공시와 재무

두 매퍼는 발행인 단위다. issuer는 10자리 CIK로 발급한 `mint_issuer('sec_cik', cik)`이고 instrument는
해석하지 않는다. 다른 표기의 CIK는 issuer가 비어 필수 열 누락으로 승격을 거부한다. 어떤 값도 고쳐
읽지 않는다.

`sec.submissions@1`은 `sec.submissions_filings@1`의 `filings` 테이블을 공시마다 `filings` 행 하나로 옮긴다.

| 열 | 값 |
| --- | --- |
| `filing_id` | `##########-##-######` 표기의 `accessionNumber`. 다른 표기는 null(거부) |
| `form` | `form` 원문 |
| `filed_date` | `YYYY-MM-DD` 표기의 `filingDate`. 다른 표기는 null(거부) |
| `period_end` | `reportDate`. 비었거나 다른 표기면 null |
| `accepted_at_us` | `acceptanceDateTime`(`YYYY-MM-DDTHH:MM:SS[.fff]Z`, UTC)의 microsecond. 다른 표기는 null |

EDGAR는 날짜로만 받은 공시(전자 접수 이전 공시 등)의 접수 시각을 그 날짜의 0시로 싣는다. 대부분은 UTC 0시
(`YYYY-MM-DDT00:00:00.000Z`)이고 나머지는 New York 현지 0시다. 두 시각 모두 실제 접수보다 이를 수 있으므로
매퍼는 `filingDate`의 UTC 0시나 New York 현지 0시와 같은 접수 시각을 null로 둔다. 그런 공시의 시점은 알 수 없음이고 공시일로 채우지 않는다. 시간 입력은 접수 시각 `accepted_at`
(`source_column@1`)과 `filed_date`이고 명세 파티션은 `filingDate`로 원천 행을 고른다. `filingDate`가
날짜 표기가 아닌 행은 어느 파티션에도 들지 않으므로 파티션 명세가 거부한다.

SEC는 일부 공시를 두 번 나열한다(제출자의 최근 공시와 이전 쪽, 또는 두 쪽). CIK, accession, 공시일·보고일,
접수 시각, 양식이 모두 같은 행은 한 나열이므로 원천 순서의 첫 행만 읽고 나머지는 원천에 남는다. 같은
accession이라도 그 값 중 하나라도 다른 행은 모두 매핑되어 겹치는 자연키가 승격을 거부한다. 행에 수집 시각이
없으므로 수집 시각은 원천의 `sl:` 연결 시각이다.

`sec.companyfacts@1`은 SEC companyfacts의 사실 테이블(사실마다 `cik`, `taxonomy`, `tag`, `unit`,
`period_start`, `period_end`, `accession_number`, `form`, `filed`, 십진 텍스트 `value`, 수집 시각
`retrieved_at`)을 사실마다 `fundamentals` 행 하나로 옮긴다. SEC의 `fy`, `fp`, `frame` 등 다른 열은 원천
행과 그 해시에 남는다.

| 열 | 값 |
| --- | --- |
| `concept` | `taxonomy:tag` |
| `unit` | SEC의 단위 |
| `period_start`, `period_end` | 사실의 기간. 시점 사실은 `period_start`가 null |
| `fiscal_period` | 그 날짜만으로 정한 기간 이름표. 시점 사실은 `instant`, 기간 사실은 양 끝을 포함한 일수 `n`의 `P<n>D` |
| `dimensions_hash` | `aas-dimensions-v1`(`accession`) |
| `form`, `accession` | 사실을 보고한 공시의 양식과 accession |
| `accepted_at_us` | pin한 `filings` generation에서 그 accession의 접수 시각. generation이 그 accession을 갖지 않거나 그 행들의 접수 시각이 다르면(공동 제출자) null |
| `value`, `value_state` | 십진수(지수 표기 포함)는 `present`이고 `decimal_text@1`로 옮긴다. 빈 텍스트는 `missing`, 그 밖의 텍스트는 값 없이 `invalid` |

SEC의 `fp`는 사실이 아니라 그 사실을 보고한 공시의 회계 기간이다(10-K의 전년 비교 값도 그 10-K의 `FY`를
싣는다). 공시가 `fp`를 싣지 않는 경우도 있으므로 `fiscal_period`는 사실의 날짜에서만 나온다.

공시마다 그 사실들이 따로 record다. 뒤의 공시가 같은 개념과 기간을 다시 보고하면(비교 값, 재작성,
정정 공시) 어느 이전 값을 고친 것인지 추정하지 않고 자기 accession 아래 record를 더하며, 이전 공시의 값은
그 공시의 사실로 남는다. 시점에 알려진 최신 값은 소비자가 그 시점까지 접수된 공시 중에서 고른다. 같은
accession의 값이 이후 수집에서 바뀌면 그 record의 SUPERSEDE다.

명세는 매퍼 인자 `filings`로 `filings.*` dataset의 generation 하나를 pin하고, 엔진은 그 chain의 head 행을
참조 테이블로 적재한다. 두 시점 열은 조인한 접수 시각 `accepted_at`의 `source_column@1`(근거
`revision`)이다. 조인되지 않은 사실은 시점이 null이라 strict reader가 고르지 않으며, 계획 보고의 시간 규칙
null 수가 그 수다. 그 accession을 담은 후손 filings generation을 pin한 다음 승격에서 그 사실은 접수 시각을
얻은 SUPERSEDE가 된다. 시간 입력 `filed`도 있으므로 공시일 상한(`local_day_end@1`, grant 필요)을 쓰는 명세는
새 dataset(`.r<N>`)으로 승격한다. 명세 파티션은 `filed`로 사실을 고른다.

```text
dimensions_hash = sha256(정규 JSON ["aas-dimensions-v1", {이름: 텍스트, ...}])
```

차원 이름은 정렬된 키이고 값은 텍스트다. 값 하나라도 알 수 없으면 해시가 null이 되어 행이 필수 열
누락으로 거부된다. 엔진처럼 매퍼도 이 해시를 SQL로 계산하며, SQL의 JSON 문자열 표기는 `json.dumps`의
ASCII escape와 바이트까지 같다.

## dataset 카탈로그

데이터 수직이 승격하는 market dataset은 아래 표가 전부다. 표는 `storage/dataset_catalog.py`의 `CATALOG`와
같고, `aas data datasets`는 그 항목을 `catalog`로 돌려준다(`tests/tools/test_dataset_catalog.py`가 표, 코드,
매퍼 레지스트리, 명령 출력을 대조한다).

- **도메인**은 generation이 채우는 market 테이블이고, 표의 매퍼는 모두 그 도메인을 낸다.
- **역할**은 `reference`(dataset 이름에 `ref`가 있음: 공급자 조정 가격, 기준 시리즈, 동결 공급자의 대조
  자료)이거나 `canonical`이다. `prices`에서는 이 역할이 행의 `price_role`이다.
- **매퍼**는 그 dataset의 명세가 쓰는 등록 매퍼다. `없음`인 dataset은 수집 원천이 있고 그 원천을 승격하는
  매퍼가 레지스트리에 없으므로 generation이 없다.
- **시간 규칙**은 명세가 두 시점 열에 쓰는 규칙, **숫자 규칙**은 열마다 고르는 규칙들, **품질 규칙**은 명세의
  `quality_rules`다. 한 chain의 시간 규칙은 바뀌지 않으므로 다른 규칙은 `.r<N>` 세대의 새 dataset이고, 그
  dataset도 같은 항목에 속한다.
- **flag**는 위 규칙과 매퍼의 행 flag에서 나온다(규칙이 붙이는 flag는 [숫자 규칙](#숫자-규칙),
  [품질 flag와 품질 검사](#품질-flag와-품질-검사)). 규칙이 수집 시각으로 내린 시점의
  `time_clamped_to_ingestion`은 `unknown_null@1`이 아닌 모든 시간 규칙에 생길 수 있어 열에 적지 않는다.
- **동결**은 [동결 원천](#동결-원천)의 dataset이다. 새 수집이 없고 coverage가 마지막 원천 날짜에서 끝난다.

| dataset | 도메인 | 역할 | 매퍼 | 시간 규칙 | 숫자 규칙 | 품질 규칙 | flag | 동결 | 원천 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `prices.kr.eodhd` | `prices` | canonical | `eodhd.bars@1`, `eodhd.bars_quarantine@1`, `eodhd.bulk_quarantine@1` | `session_close_plus_lag@1` | `krw_tick@1`, `float_shortest@1` | 없음 | `decimal_rounding_tie`, `provider_float_reconstructed`, `provider_float_storage`, `provider_reported_partial` | 아니오 | EODHD KR 일봉 이력, 보류된 이력 행(값 없는 `invalid` bar), 부분 응답 일간 내려받기. OHLC는 원 단위 정수, 거래량은 공급자 float 그대로. 부분 응답 generation은 `partition_row_count@1` 행 수 대조를 남긴다. [KR 가격](#kr-가격) |
| `prices.kr.eodhd.ref` | `prices` | reference | `eodhd.bars_adjusted@1`, `eodhd.bulk_quarantine_adjusted@1` | `session_close_plus_lag@1` | `float_shortest@1` | 없음 | `decimal_rounding_tie`, `provider_float_storage`, `provider_reported_partial` | 아니오 | 같은 두 원천 모양의 공급자 adjusted close. basis `total_return`, `fields='close'` |
| `prices.us.norgate` | `prices` | canonical | `norgate.prices_none@1` | `session_close_plus_lag@1` | `decimal_text@1` | 없음 | `volume_precision_limited` | 아니오 | Norgate 비조정 일봉 내보내기(`norgate-history-csv-*`의 미국 주식 `bars`). 다섯 값 모두 CSV 원문, 연도 partition마다 generation 하나 |
| `prices.us.norgate.ref` | `prices` | reference | `norgate.prices_adjusted@1` | `session_close_plus_lag@1` | `float_shortest@1` | 없음 | `decimal_rounding_tie`, `provider_float_storage` | 아니오 | Norgate 조정 가격 part. `CAPITAL`은 `split_adjusted`, `TOTALRETURN`은 `total_return`, float32 저장값 |
| `prices.us.eodhd` | `prices` | canonical | `eodhd.bars@1` | `session_close_plus_lag@1` | `float_shortest@1` | `cross_provider_mismatch@1` | `cross_provider_mismatch`, `decimal_rounding_tie`, `provider_float_storage` | 아니오 | EODHD US 일간 수집. 다운로드 하나가 generation 하나이고 같은 날짜의 다른 다운로드는 SUPERSEDE. 품질 규칙의 기준은 `prices.us.norgate` generation pin. identity가 해석한 US 보류 행의 재처리는 Linear AAS-69 |
| `prices.us.fmp.ref` | `prices` | reference | `fmp.eod_non_split@1` | `session_close_plus_lag@1` | `float_shortest@1`, `exact@1` | 없음 | `decimal_rounding_tie`, `provider_float_storage` | 예 | FMP 동결 legacy non-split 응답. 정정은 revision 번호순 generation. 원천 자료실 FMP normalized lineage(eod_full, dividend_adjusted)의 매퍼는 Linear AAS-68 |
| `prices.ref.norgate` | `prices` | reference | `norgate.reference_closes@1`, `norgate.reference_history@1` | `local_day_end@1` | `decimal_text@1` | 없음 | `time_precision_day`, `volume_precision_limited` | 예 | Norgate 기준 시리즈 표와 지수·기타 데이터베이스 내보내기, `fields='close'`, 통화 `XXX`. 공급자가 종가 시각을 밝히지 않으므로 날짜 끝은 `Etc/GMT+12`. 음수 수준이 있는 시리즈는 `signed`로 고르지 않는다(Linear AAS-67) |
| `sessions.xnys` | `calendar_sessions` | canonical | `calendar.declared@1` | `declared_session_end@1` | 없음 | 없음 | 없음 | 아니오 | XNYS [선언 달력](#선언-달력) 문서. 관측 거래일은 대조 보고의 근거, 임시 휴장은 SUPERSEDE |
| `sessions.xkrx` | `calendar_sessions` | canonical | `calendar.declared@1` | `declared_session_end@1` | 없음 | 없음 | 없음 | 아니오 | XKRX [선언 달력](#선언-달력) 문서. 관측 거래일은 대조 보고의 근거, 임시 휴장은 SUPERSEDE |
| `actions.us.norgate` | `corporate_actions` | canonical | `norgate.dividends@1`, `norgate.capital_adjustments@1` | `exdate_open@1` | `float_shortest@1` | 없음 | `decimal_rounding_tie`, `provider_float_storage` | 예 | Norgate 조정 가격 part의 `CAPITAL` 행 전체. 배당이 첫 generation, 자본 사건이 그 child이고 파티션 없이 이력 전체가 한 generation이다. 달력은 `sessions.xnys` |
| `actions.us.fmp.ref` | `corporate_actions` | reference | `fmp.dividends@1`, `fmp.splits@1` | `exdate_open@1` | `float_shortest@1` | 없음 | `decimal_rounding_tie`, `provider_float_storage` | 예 | FMP 동결 배당·분할 응답. 정정은 revision 번호순 generation. 달력은 `sessions.xnys` |
| `actions.us.eodhd` | `corporate_actions` | canonical | 없음 | `exdate_open@1` | 없음 | 없음 | 없음 | 아니오 | Qveris 거래소 일간 `splits`·`dividends` 수집 원천([Qveris 수집과 원천 적재](#qveris-수집과-원천-적재)). 매퍼는 Linear AAS-77 |
| `actions.kr.eodhd` | `corporate_actions` | canonical | 없음 | `exdate_open@1` | 없음 | 없음 | 없음 | 아니오 | Qveris 거래소 일간 `splits`·`dividends` 수집 원천(KO, KQ). 매퍼는 Linear AAS-77 |
| `status.us.norgate` | `instrument_status` | canonical | `norgate.status@1` | `local_day_end@1` | 없음 | 없음 | `time_precision_day` | 아니오 | Norgate master의 상장(`event` `listed`)이 첫 generation, 상장폐지(`delisted`)가 그 child. `America/New_York`, 근거 `record` |
| `status.kr.kind` | `instrument_status` | canonical | 없음 | 없음 | 없음 | 없음 | 없음 | 아니오 | KIND 상장법인목록 수집 원천(`kind-listings-*`). 상장 이력은 [KR 등록](#kr-등록)의 identity 구간으로 들어간다. 상태 매퍼는 Linear AAS-77 |
| `filings.us.sec` | `filings` | canonical | `sec.submissions@1` | `source_column@1` | 없음 | 없음 | 없음 | 아니오 | SEC submissions의 접수 시각 `acceptanceDateTime`. [SEC 공시와 재무](#sec-공시와-재무) |
| `filings.kr.dart` | `filings` | canonical | `dart.fnltt_filings@1` | `local_day_end@1` | 없음 | 없음 | `time_precision_day` | 아니오 | DART 재무제표 응답의 접수번호, 접수일 기준 Asia/Seoul. 공시 목록(`list.json`) 수집 원천의 매퍼(`dart.list`)는 레지스트리에 없다(Linear AAS-77) |
| `fundamentals.us.sec` | `fundamentals` | canonical | `sec.companyfacts@1` | `source_column@1` | `decimal_text@1` | 없음 | `volume_precision_limited` | 아니오 | companyfacts. 공시마다 자기 record(accession은 dimensions). 시점은 pin한 `filings.us.sec` generation에서 accession으로 조인한 `accepted_at_us`이고 조인되지 않은 사실은 null. 같은 accession의 값이 바뀌면 SUPERSEDE |
| `fundamentals.kr.dart` | `fundamentals` | canonical | `dart.fnltt@1` | `local_day_end@1` | `decimal_text@1` | 없음 | `time_precision_day`, `volume_precision_limited` | 아니오 | 재무제표 응답. 발행인 단위, 연결·별도는 dimensions, 접수일 기준. 정정 공시는 SUPERSEDE. 자료 없음 응답은 행 대신 결과 분포로 기록. [DART 재무제표 응답](#dart-재무제표-응답) |
| `macro.us.alfred` | `macro_observations` | canonical | `fred.alfred@1` | `local_day_end@1` | `decimal_text@1` | 없음 | `time_precision_day`, `volume_precision_limited` | 아니오 | ALFRED vintage, `realtime_start` 기준. vintage 구간마다 generation 하나, 정정은 SUPERSEDE |
| `macro.kr.bok` | `macro_observations` | canonical | `bok.observations@1` | `unknown_null@1` | `decimal_text@1` | 없음 | `volume_precision_limited` | 아니오 | legacy BOK 공개 응답. vintage와 공개 시각이 없어 strict 읽기는 고르지 않는다 |
| `macro.kr.oecd` | `macro_observations` | canonical | `oecd.observations@1` | `unknown_null@1` | `decimal_text@1` | 없음 | `volume_precision_limited` | 아니오 | legacy OECD 공개 응답. vintage와 공개 시각이 없어 strict 읽기는 고르지 않는다 |
| `fx.usdkrw.norgate` | `fx_rates` | canonical | `norgate.fx_closes@1`, `norgate.fx_history@1` | `local_day_end@1` | `decimal_text@1` | 없음 | `time_precision_day`, `volume_precision_limited` | 예 | 원천 자료실 Norgate 기준 시리즈(2026-09-08까지)와 legacy 내보내기 편입본, 시간대 `Etc/GMT+12`. 우선순위는 소비자 pin |
| `fx.usdkrw.fred` | `fx_rates` | canonical | `fred.fx_series@1` | `unknown_null@1` | `decimal_text@1` | 없음 | `volume_precision_limited` | 아니오 | FRED DEXKOUS(legacy CSV 편입본과 ALFRED 수집). H.10 발표 지연 때문에 strict 읽기는 고르지 않는다. 우선순위는 소비자 pin |
| `classifications.us.norgate` | `classifications` | canonical | `norgate.classification@1` | `local_day_end@1` | 없음 | 없음 | `time_precision_day` | 아니오 | Norgate 증권 유형(`norgate.security_type`)과 상장 거래소(`norgate.exchange`) snapshot, 과거로 소급하지 않음. [분류](#분류) |
| `classifications.us.sec` | `classifications` | canonical | `sec.sic@1` | `local_day_end@1` | 없음 | 없음 | `time_precision_day` | 아니오 | SEC SIC(`sec.sic`) snapshot, issuer 주체 |
| `classifications.kr.kind` | `classifications` | canonical | `kind.industry@1`, `kind.industry@2` | `source_column@1` | 없음 | 없음 | 없음 | 아니오 | KIND 업종(`kind.industry`) snapshot, 단축코드를 identity snapshot으로 해석, 수집 시각부터 알려짐 |

identity 원천(Norgate master, SEC submissions, FMP profile, DART 고유번호, KIND 목록)은 typed generation이
아니라 [identity 등록](#identity-등록과-chunked-문서)으로 state에 들어간다. 지수 구성과 상장 universe도
같은 방식으로 [universe 등록](#universe-등록)의 `universe_versions`·`universe_members`에 들어간다.
13F 보유 내역, 애널리스트 추정치, 연구 산출물은 `source_only`로 원천 자료실에 남는다.

dataset의 백필은 연도 단위 generation, 이후 유지보수는 세션 단위(재무는 일 단위) generation으로
게시한다.

`aas data datasets`는 committed dataset version 전체(`datasets`), 카탈로그 항목마다 위 열과 그 항목에 속한
dataset의 version 수와 head(`published`), 그리고 어느 항목에도 속하지 않는 committed dataset(`uncataloged`)을
돌려준다. 카탈로그는 dataset을 설명할 뿐 게시를 막지 않는다. `aas data import`나 연구 입력 등록이 다른 이름으로
게시한 generation도 그대로 읽히고 `uncataloged`에 이름이 남는다.

### KR 가격

`aas data kr-prices`(`storage/kr_prices.py`)가 `prices.kr.eodhd`를 채우는 승격을 순서대로 만든다. 한
단계가 generation 하나이며 각 단계의 명세는 그 단계의 파티션, pin한 테이블, 등록된 identity snapshot,
`sessions.xkrx`의 committed head, 그리고 dataset head를 parent로 적은 정규 문서다.

1. 이력: 원천 ID 접두어 하나(`--history-lineage`)의 모든 `bars` 테이블을 pin하고 달력 연도마다
   `eodhd.bars@1`로 승격한다. 행이 있는데 날짜가 있는 행이 하나도 없으면 명령이 거부한다.
2. 보류된 이력 행: 같은 접두어의 행이 있는 `quarantine` 테이블을 pin하고, 그 행들의 연도 전체를
   generation 하나로 `eodhd.bars_quarantine@1`로 승격한다. 행은 값 없는 `invalid` bar가 된다. 행이 있는
   테이블에 매핑된 이유와 날짜를 가진 행이 하나도 없으면 단계를 만들지 않고 그 행 수를
   `held_unmapped_rows`로 보고하므로 이력 연도는 막히지 않는다.
3. 부분 응답 일간 내려받기: 원천 ID 접두어 하나(`--bulk-lineage`)의 `quarantine` 테이블 중 행이 KR
   거래소(`KO`, `KQ` → `KRW`)를 가리키는 것을 세션 날짜마다 `eodhd.bulk_quarantine@1`로 승격한다. KR과
   다른 거래소를 섞은 테이블은 거부한다. 어느 행도 거래소를 말하지 않는 테이블(깨진 JSON이나
   `exchange_short_name` 없음)은 pin하지 않고 원천 ID와 행 수를 `bulk_unclassified_tables`로 보고한다.

테이블은 원천의 `sl:` 연결 시각, 같으면 원천 ID 순서로 놓인다. 보류 행과 부분 응답 모두 테이블 digest가
같은 반복 내려받기는 가장 이른 연결 하나만 pin하고, 그 연결 시각이 행의 수집 시각이 된다. 내용이 다른
테이블이 같은 거래소·날짜를 실으면 그 날짜는 연결 순서대로 이어지는 generation이 된다. k번째
generation은 거래소마다 k번째 내려받기(그보다 적으면 마지막 것)를 pin하므로 나중 내려받기가 자기 수집
시각과 flag로 앞의 것을 SUPERSEDE한다. 그런 추가 단계 수는 `bulk_superseding_steps`로 보고된다.

두 시점 열은 `session_close_plus_lag@1`(근거 `record`, 입력 `session_date`, `--lag-us`)이고 OHLC는
`krw_tick@1`, 거래량은 `float_shortest@1`이다. 공급자는 거래량도 분할 계수로 나눠 다시 계산하므로
(545540.77978275주처럼) 소수 12자리로 정확히 표현되지 않는 거래량이 있고, 그 행에는
`provider_float_storage`가 남는다. `--reference`는 이력과 부분 응답 단계를 adjusted close 매퍼로
`prices.kr.eodhd.ref`에 만들며 close는 `float_shortest@1`이다. 보류된 이력 행은 남길 adjusted close가
없으므로 reference에 단계가 없다. 이력과 겹치는 부분 응답 날짜는 이력
generation의 자식이므로 같은 값은 바뀌지 않고, 다른 값은 SUPERSEDE로 그 원천을 받은 시각부터 알려진다.

`--plan`은 아무것도 쓰지 않고 모든 단계를 현재 head의 자식으로 계획해 단계별 보고와 합계(원천 행,
행 상태, op, flag, 숫자 규칙 flag 행 수)를 낸다. 그래서 적용되지 않은 앞 단계가 있으면 뒤 단계는 그
단계가 없는 head에 대해 계획된다. 실행은 단계를 순서대로 앞 단계가 남긴 head의 자식으로 승격하고 첫
거부에서 멈춘다. delta가 빈 단계는 아무것도 게시하지 않으므로, 끝까지 실행한 뒤 다시 실행하면 아무것도
쓰지 않는다. identity snapshot에서 해석되지 않는 심볼의 행은 미해결로 보고되고 승격되지 않는다.

## DART 재무제표 응답

OpenDART 단일회사 전체 재무제표(`fnlttSinglAcntAll`) 응답은 원천 자료실의 DART receipt 테이블에
요청 하나가 한 행으로 들어 있다. 행은 `endpoint`, `outcome`, `request_json`(그 `parameters_json`이
`corp_code`, `bsns_year`, `reprt_code`, `fs_div`를 싣는다), 응답 bytes `raw_base64`와 그 `raw_sha256`,
수집 시각 `retrieved_at_utc`를 가진다. `dart.fnltt@1`과 `dart.fnltt_filings@1`은 이 행을 같은 규칙으로 읽는다.

`financials` 행의 결과는 아래에서 처음 맞는 하나다. 다른 endpoint의 행(같은 테이블의 고유번호 목록)은
`other_endpoint`다.

| 결과 | 조건 |
| --- | --- |
| `unreadable` | 요청이 8자리 `corp_code`, 4자리 `bsns_year`, 알려진 보고서 코드, `CFS`·`OFS` 중 하나라도 싣지 않음. 또는 완료된 요청의 응답 bytes가 UTF-8이 아니거나 그 SHA-256이 기록과 다르거나, 공급자 상태 `000`과 비어 있지 않은 `list`를 가진 JSON이 아니거나, 줄 하나라도 `sj_div`(`BS`·`IS`·`CIS`·`CF`·`SCE`), 앞 여덟 자리가 달력 날짜인 14자리 `rcept_no`, 숫자 `ord`, 세 글자 `currency`, `account_id`·`account_nm`·`account_detail`을 갖추지 않음 |
| `no_data`, `failed` | 공급자가 재무제표 없음으로 답했거나 요청이 실패함 |
| `mismatched` | 완료된 응답의 줄 하나라도 요청과 다른 회사·사업연도·보고서 코드를 싣거나, 줄들이 접수번호를 둘 이상 실음 |
| `year_end_unknown` | `dart.fnltt@1`만: 명세가 `december_year_end`에 선언하지 않은 회사의 완료된 응답 |
| `completed` | 그 밖의 완료된 응답. 이 결과만 행을 만든다 |
| `unknown_outcome` | `COMPLETED`·`NO_DATA`·`FAILED`가 아닌 결과 |

`unreadable`·`mismatched`·`year_end_unknown`·`unknown_outcome` 행은 issuer가 비어 있는 행 하나가 되어
필수 열 누락으로 승격 전체를 거부하므로, 손상된 응답이나 기간을 정할 수 없는 응답이 조용히 빠지지
않는다. 명세의 매퍼 인자 `accept`(그 결과 이름의 정렬된 목록)는 그런 행을 빼고 승격하도록 허용하는
grant이고, 뺀 행도 승격의 결과 분포에 그대로 세어져 기록된다.

- issuer는 요청의 8자리 `corp_code`로 발급한 `mint_issuer('dart_corp_code', corp_code)`다.
  instrument는 해석하지 않는다.
- 공시는 응답의 14자리 접수번호 `rcept_no`다. 앞 여덟 자리가 한국 날짜의 접수일이고 그것이
  `filed_date`이자 유일한 시간 입력이다. 두 시점 열은 `local_day_end@1`(Asia/Seoul, 근거 `revision`)이다.
  OpenDART는 한 보고서의 가장 늦은 공시(정정 포함)의 재무제표로 답하므로, 접수번호는 그 행의 값을 실은
  공시를 가리킨다. `form`은 OpenDART 보고서 코드(`11011` 사업, `11012` 반기, `11013` 1분기, `11014` 3분기)다.
- 한 승격 안에서 되풀이되는 완료 응답은 한 번만 읽고 나머지는 원천에 남는다. 재무는 요청(회사·사업연도·
  보고서·`fs_div`)마다 가장 늦은 공시(가장 큰 접수번호)의 가장 이른 수집 하나를 읽는다. 같은 공시를
  bytes만 다르게 다시 받은 응답도 그 하나로 읽힌다. 공시는 같은 회사·보고서·접수번호의 응답(한 공시의
  연결·별도 응답 포함) 중 가장 이른 수집 하나다.
- 명세 파티션은 요청의 사업연도로 원천 행을 고른다. 파티션 날짜는 `bsns_year`의 1월 1일이다.

`dart.fnltt@1`은 응답 줄과 그 줄이 재는 기간마다 `fundamentals` 행 하나를 낸다. DART 응답은 기간
날짜를 싣지 않으므로 기간은 발행인의 결산월에서 나온다. 매퍼 인자 `december_year_end`는 명세가 12월
결산으로 선언한 회사 고유번호의 정렬된 목록이다(예: KIND `결산월`과 DART 고유번호 목록의 종목코드로
만든다). 선언된 회사의 회계연도는 `bsns_year`의 1월부터 12월이고, 다른 회사의 응답은 `year_end_unknown`
이므로 매퍼는 결산월을 추정해 날짜를 쓰지 않는다. 보고서는 끝 달(3·6·9·12월)까지의 누적 기간과 그
마지막 세 달인 분기를 가진다. OpenDART의 필드 정의에 따라:

| 금액 | 기간 | `fiscal_period` | `period_start` |
| --- | --- | --- | --- |
| 손익계산서(`IS`·`CIS`)의 `thstrm_amount` | 분기(사업보고서는 연간) | `Q1`·`Q2`·`Q3`·`FY` | 분기 첫날(사업보고서는 1월 1일) |
| 반기·3분기 보고서 손익계산서의 `thstrm_add_amount` | 누적 | `H1`·`9M` | 1월 1일 |
| 현금흐름표·자본변동표(`CF`·`SCE`)의 `thstrm_amount` | 누적 | `Q1`·`H1`·`9M`·`FY` | 1월 1일 |
| 재무상태표(`BS`)의 `thstrm_amount` | 끝 달 말일의 시점 | `Q1`·`H1`·`9M`·`FY` | null |

`period_end`는 보고서 끝 달의 말일이다. 그 밖의 보고서와 재무제표의 `thstrm_add_amount`는
`thstrm_amount`와 같은 기간을 재거나 비어 있으므로 원천에 남고, 전기 비교 금액(`frmtrm_*`,
`bfefrmtrm_*`)도 원천에 남는다.

| 열 | 값 |
| --- | --- |
| `concept` | DART가 쓴 `account_id` 그대로. 표준 계정이 없는 줄은 `-표준계정코드 미사용-` |
| `unit` | 줄의 통화 코드 |
| `dimensions_hash` | `aas-dimensions-v1`(`fs_div`, `sj_div`, `account_nm`, `account_detail`, `occurrence`) |
| `form`, `accession` | 보고서 코드, 접수번호. `accepted_at_us`는 null |
| `value`, `value_state` | 십진수 금액은 `present`이고 `decimal_text@1`로 옮긴다. 빈 필드는 `missing`, 그 밖의 텍스트는 값 없이 `invalid` |

한 재무제표 안에서도 같은 계정과 이름이 되풀이되므로 `occurrence`는 응답 안에서 같은 재무제표·
계정·이름·상세를 가진 줄 중 `ord` 순서(같으면 응답 순서)로 몇 번째인지(1부터)다. 줄의 record는 공시를
이름에 넣지 않으므로 한 보고서의 모든 공시에서 같다. 정정 공시의 값은 이전 값을 SUPERSEDE하고 정정
공시의 접수일부터 알려지며, `accession`이 정정 공시의 접수번호다. 그래서 시점 읽기는 그 시점에 알려진
공시의 값을 head 하나로 얻는다. 정정 공시가 더 싣지 않는 줄의 이전 값은 tombstone 범위가 덮지 않는 한
head로 남는다.

```text
dimensions_hash = sha256(정규 JSON ["aas-dimensions-v1", {이름: 텍스트, ...}])
```

차원 이름은 정렬된 키이고 값은 텍스트다. 값 하나라도 알 수 없으면 해시가 null이 되어 행이 필수 열
누락으로 거부된다. 엔진처럼 매퍼도 이 해시를 SQL로 계산하며, SQL의 JSON 문자열 표기는 `json.dumps`의
ASCII escape와 바이트까지 같다.

`dart.fnltt_filings@1`은 공시마다 `filings` 행 하나를 낸다. 응답의 접수번호와 `form`, `filed_date`이고
`accepted_at_us`와 `period_end`는 응답이 말하지 않으므로 null이다. 다시 수집한 같은 공시를 따로
승격하면 변하지 않은 행이다.

## KR 공시·상장 수집

OpenDART와 KIND 수집은 설치본에 직접 기록한다. 고정된 작업 목록은 없다. 매 실행이 이미 아는 것과
Seoul 날짜에서 물을 것을 계산하므로, 한 번 답을 받은 요청도 기간·공시·재시도 규칙이 다시 묻게 한다.
코드는 `data/opendart.py`(요청·결과·HTTP 클라이언트), `data/opendart_cohort.py`(rolling cohort),
`data/kind.py`, `storage/collection_ledger.py`(state 수집 원장), `storage/kr_collection.py`(실행과
commit)이고, 명령은 `aas collect dart plan|run`과 `aas collect kind run`이다.

**요청.** OpenDART 요청은 endpoint와 정규 parameter JSON뿐이다.

```text
fingerprint = sha256(정규 JSON ["aas-opendart-request-v1", endpoint, parameters_json])
```

| endpoint | 공급자 경로 | parameter |
| --- | --- | --- |
| `corp_codes` | `corpCode.xml` | 없음 |
| `financials` | `fnlttSinglAcntAll.json` | `corp_code`(8자리), `bsns_year`(2015 이상), `reprt_code`(`11011`·`11012`·`11013`·`11014`), `fs_div`(`CFS`·`OFS`) |
| `list` | `list.json` | `bgn_de`·`end_de`(92일 이내), `pblntf_ty=A`, `last_reprt_at=N`, `page_count=100`, `page_no` |

관측일은 요청에 들어가지 않는다. 같은 질문을 다른 날 다시 묻는 것은 같은 요청의 다음 attempt이고,
관측일을 담은 legacy 요청 문서도 같은 요청으로 읽힌다. 응답 결과는 `COMPLETED`(내용 있는 답),
`NO_DATA`(공급자 상태 `013`), `FAILED`(그 밖의 답)이며 수집 경로만 정한다. corp code 답은
`CORPCODE.xml`이 읽히고 종목코드가 있는 회사를 나열할 때만, 목록 답은 요청한 `page_no`이고 공시 접수일이
요청한 날 안이며 1,000 page 이하를 셀 때만 `COMPLETED`다. key를 되풀이하는 JSON은 상태가 없는 답이다. 응답 bytes는 결과와 상관없이
보존하고 판정은 승격 매퍼가 한다. 키·IP·만료 거부와 일일 한도(`010`·`011`·`012`·`020`·`021`·`901`,
HTTP 401·403·429)는 실행을 멈춘다. 키는 공급자 URL에만 실리며 응답이 키를 되돌려 주면 그 응답을
보존하지 않는다.

**rolling cohort.** 계획의 입력은 가장 새로운 완료 corp code 목록에서 종목코드가 있는 회사(KIND의
유가증권·코스닥 목록이 둘 다 commit돼 있고 모든 코드가 KRX 단축코드면 그 단축코드로 좁힘)와 유가증권·코스닥(`corp_cls`
`Y`·`K`) 정기공시를 낸 회사, 원천 자료실의 모든 `opendart-*` receipts 테이블(legacy 편입분 포함), 공시
목록 page가 말하는 정기보고서 공시, 그리고 답을 보존하지 못한 원장 attempt다. 12월 결산 기준으로
기간이 끝난 사업연도(2015년부터)·보고서마다:

| 이유 | 연결(`CFS`) 요청을 묻는 조건 |
| --- | --- |
| `new_filing` | 그 보고서의 공시 접수일이 마지막 수집의 Seoul 날짜 이후(같은 날 포함)이고 그 수집이 오늘 전. 늦은 제출과 정정 공시다 |
| `never_asked` | 물은 적이 없음 |
| `season_retry` | 마지막 답이 `NO_DATA`이고 제출 기한(분기·반기 45일, 사업 90일)+30일 안에서 7일이 지남 |
| `failed_retry` | 마지막 답이 `FAILED`이거나 답을 보존하지 못한 attempt이고 하루가 지남 |
| `no_data_retry` | 마지막 답이 `NO_DATA`이고 그 시즌 뒤 90일이 지남. 직전 사업연도 이후의 보고서만이며 더 오래된 보고서는 공시가 먼저 알린다 |

별도(`OFS`) 요청은 연결 요청의 마지막 답이 `NO_DATA`인 동안 같은 규칙을 따른다. `COMPLETED`는 새 공시가
없으면 다시 묻지 않는다. 요청 순서는 위 표의 순서(`opendart_cohort.REASONS`)이고, 같은 이유 안에서는
최신 기간부터다. 간격은 `CohortPolicy`의
값이며 그 해시가 원장 job의 `policy_hash`(그 job을 처음 만든 실행의 정책)다.

공시 목록은 하루 단위로 읽는다. 오늘 전의 Seoul 날짜는 그 날이 끝난 뒤 받은 첫 page와 첫 page가 센
모든 page의 답이 있을 때 덮인 것이다(첫 page가 `NO_DATA`면 공시 없는 날). 처음에는 90일 전부터 읽고,
알려진 가장 이른 날부터 빈 날을 채운다. 실행 중 첫 page가 오면 그 page가 센 page를 바로 묻는다. 마지막
답이 `FAILED`이거나 답을 보존하지 못한 page는 하루 뒤 다시 묻는다. 응답이 없는 완료 목록 행은 읽히지 않은
행으로 세고 그 날을 덮지 않는다.
보고서 이름 `분기보고서 (YYYY.03|09)`, `반기보고서 (YYYY.06)`, `사업보고서 (YYYY.12)`(앞의 `[기재정정]` 같은
괄호 표시 포함)만 요청에 대응하고, 다른 결산월의 보고서는 대응하지 않은 수로 센다. corp code 목록은
7일마다 다시 받는다.

**원장.** state의 수집 표가 호출을 기록한다. job은 요청 하나(`job_id`·`idempotency_key` =
`opendart:<fingerprint>`, dataset `identity.kr.dart`·`filings.kr.dart`·`fundamentals.kr.dart`, 재무는 보고서
누적 기간, 목록은 그 날을 window로 가짐)이고 묻는 때마다 번호가 오르는 attempt다.

| 시점 | attempt | usage event |
| --- | --- | --- |
| 호출 전 | `reserved` | `reserved` |
| 호출 직전 | `started` | |
| 답과 receipt를 `raw/`에 보존한 뒤 | `succeeded`(공급자 오류 답 포함) | `charged`, receipt SHA-256 |
| 답을 보존하지 못함(전송 실패) | `uncertain` | `uncertain` |
| 중단 뒤 다음 실행: `reserved`로 남음 | `failed` | `released` |
| 중단 뒤 다음 실행: `started`로 남음 | `uncertain` | `uncertain` |

불확실한 attempt는 성공이나 미호출로 바뀌지 않는다. 일일 quota는 24시간 안에 `reserved`가 기록되고
`released`되지 않은 attempt 수이며 기본 19,000이다. 실행의 호출 상한은 `--max-calls`(기본 2,000)와 quota의
남은 수 중 작은 값이다. 전송 실패가 세 번 이어지면 실행을 멈춘다.

**보존과 commit.** 호출마다 응답 bytes와 정규 receipt(`aas-opendart-receipt-v1`: 요청, fingerprint, job과
attempt, HTTP 상태, 보존 header(`content-type`·`date`·`retry-after`), 요청·수집 시각, 결과, 공급자 상태,
응답의 크기·SHA-256)를 `raw/`에 둔다. 500개까지(응답 합계 256 MiB까지)의 receipt를 수집 순서로 나열한 batch 문서
(`aas-opendart-batch-v1`)와 그 receipt·응답이 완결 단위 하나이고, `opendart-receipts-<hex>` 원천의
`receipts` 테이블 하나로 commit된다. 행은 `fingerprint`, `endpoint`, `outcome`, `provider_status`,
`request_json`(`endpoint`·`parameters_json`), `receipt_json`, `receipt_sha256`, `raw_base64`, `raw_sha256`,
`retrieved_at_utc`(`YYYY-MM-DDTHH:MM:SS.ffffffZ`)의 텍스트이며 `dart.fnltt@1`, `dart.fnltt_filings@1`,
`dart.corp_codes@1`이 이 테이블을 읽는다. 공시 목록 행은 재무 매퍼에서 `other_endpoint`다. commit 전에
중단된 실행의 `charged` receipt 중 어느 commit에도 없는 것은 다음 실행이 먼저 commit하고, 계획 전에 그 답을
아는 것에 더하므로 그 요청을 다시 묻지 않는다. 한 batch에는 완료된 corp code 답이 많아야 하나다.

**KIND.** `aas collect kind run`은 KIND 상장법인목록 내려받기(`corpList.do`, 시장 `stockMkt`·`kosdaqMkt`)를
요청 `kind-kospi`·`kind-kosdaq`로 묻고, 응답과 receipt(`aas-kind-receipt-v1`: 요청 `source_id`, HTTP 상태,
응답 크기·SHA-256, 요청·수집 시각)를 `raw/`에 둔 뒤 그 둘을 [KR 등록](#kr-등록)의 `kind-listings` 원천
하나로 commit한다. 원장 provider는 `kind`, dataset은 `identity.kr.kind`다. 상장법인목록 표가 아닌 답은
`FAILED`로 정산하고 거부를 보고하며 bytes만 남긴다. KIND는 quota가 없으므로 commit 전에 멈춘 실행의 목록은
다음 실행이 다시 받는다.

**legacy 원장 재생.** `aas collect dart plan --legacy-root DIR`은 설치본을 열지 않고 legacy 수집 디렉터리의
`attempts.jsonl`이 나열한 요청마다 `receipts/<fingerprint>.json`의 결과와 수집 시각(검증 문서
`.validation-v1.json`이 `FAILED`를 다시 판정했으면 그 결과, receipt가 없으면 원장 시각의 `FAILED`)을, 가장
새로운 완료 corp code 응답에서 회사 목록을 읽어 같은 계획을 보고한다. `--kind-receipt`는 KIND 목록으로
회사를 좁힌다. 그 legacy 수집기는 요청 지문에 고정 cohort의 관측일을 넣고 모든 답(`NO_DATA` 포함)을
종결로 다뤘으므로, 목록을 다 물은 뒤에는 새 요청을 만들지 못한다.

## US 공시·거시 수집

SEC EDGAR와 FRED/ALFRED 수집도 설치본에 직접 기록한다. 고정된 작업 목록은 없고, 매 실행이 commit된
답과 공급자의 날짜(SEC는 New York, FRED는 St. Louis의 `America/Chicago`)에서 물을 것을 계산한다. 코드는
`data/provider_request.py`(요청·답), `data/sec_collect.py`·`data/fred_collect.py`(endpoint, 답 판정, 계획),
`storage/provider_collection.py`(원장 호출·receipt·batch), `storage/sec_collection.py`·
`storage/fred_collection.py`(실행과 commit)이고, 명령은 `aas collect sec plan|run`과
`aas collect fred plan|run`이다. 원장은 [KR 공시·상장 수집](#kr-공시상장-수집)의 `collection_ledger`를 같은
규칙(호출 전 `reserved`, 직전 `started`, 답 보존 뒤 `succeeded`와 `charged`, 답이 없으면 `uncertain`,
중단 뒤 정산)으로 쓴다.

**요청.** 요청은 공급자, endpoint, 정규 parameter JSON뿐이다.

```text
fingerprint = sha256(정규 JSON ["aas-<provider>-request-v1", endpoint, parameters_json])
```

| 공급자 | endpoint | 공급자 경로 | parameter |
| --- | --- | --- | --- |
| `sec` | `daily_index` | `Archives/edgar/daily-index/<연>/QTR<n>/master.<YYYYMMDD>.idx` | `day` |
| `sec` | `submissions` | `data.sec.gov/submissions/CIK##########.json` | `cik`(10자리) |
| `sec` | `companyfacts` | `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` | `cik` |
| `fred` | `vintage_dates` | `fred/series/vintagedates` | `series_id`, `realtime_start`, `realtime_end`, `limit=10000`, `offset`, `file_type=json` |
| `fred` | `observations` | `fred/series/observations` | `series_id`, `realtime_start`, `realtime_end`, `limit=100000`, `offset`, `file_type=json` |
| `fred` | `series_csv` | `fredgraph.csv` | `id` |

FRED 키는 `vintage_dates`·`observations`의 공급자 URL에만 실리고, SEC의 공정 접근 `User-Agent`(연락처
포함)는 header에만 실린다. 둘 다 요청·receipt·원천에 들어가지 않으며, 답이 그 값(SEC는 `User-Agent` 전체나
그 안의 연락처 주소)을 되돌려 주면 그 답을 보존하지 않는다. 두 명령은 commit에 쓰는 pyarrow(legacy extra)가
없으면 정산이나 호출 전에 멈춘다. SEC는 HTTP 403·429, FRED는 401·403·429와 키를 거부하는 400이 실행을 멈춘다. 전송 실패가
세 번 이어져도 멈춘다. 호출 상한은 `--max-calls`(SEC 기본 2,000, FRED 기본 500)이고, 호출 간격은 SEC
0.125초(초당 10회 한도), FRED 0.5초(분당 120회 한도)다.

**receipt와 batch.** 답마다 응답 bytes와 정규 receipt(`aas-<provider>-receipt-v1`: 요청, fingerprint, job과
attempt, HTTP 상태, 보존 header, 요청·수집 시각, 결과, 공급자 상태, 선택, 응답의 크기·SHA-256)를 `raw/`에
둔 뒤 attempt가 `succeeded`가 된다. 결과는 `COMPLETED`(읽을 수 있는 답), `NO_DATA`(SEC 404, FRED가 없다고
답한 시계열), `FAILED`(그 밖의 답)이며 수집 경로만 정한다. 한 실행의 receipt를 수집 순서로 나열한 batch
문서(`aas-<provider>-batch-v1`)와 그 receipt·응답이 완결 단위 하나다. SEC batch는 receipt 500개
또는 응답 256 MiB에 이르면 닫힌다. FRED batch는 receipt 500개 또는 응답
64 MiB에 이른 질의(observations 창의 모든 page)가 끝날 때 닫혀, 그 질의의 page만큼 두 한도를 넘을 수 있다.
그 단위의 모든 테이블은 같은 파일의 내용 원천이라 `hex`를 공유한다. 파생 원천을 먼저, `<provider>-collect-receipts`
원천을 마지막에 commit하므로 commit된 receipts 원천은 그 batch의 모든 파생 원천도 commit됐다는 표지다. `<provider>-collect-receipts` 원천의 `receipts`
테이블은 receipt마다 `fingerprint`, `endpoint`, `request_json`, `outcome`, `provider_status`, `http_status`,
`selection_json`, `receipt_json`, `receipt_sha256`, `raw_sha256`, `raw_size`, `retrieved_at_utc`의 텍스트 행이다.
receipts 원천을 commit하기 전에 중단된 실행의 `charged` receipt 중 어느 receipts 원천에도 없는 것은 다음
실행이 먼저 같은 한도의 batch로 commit한다(FRED는 한 질의의 page를 한 batch에 둔다). 중단된 commit이 이미 둔
파생 원천은 같은 bytes가 같은 원천 ID라 재사용되고, market에 commit됐지만 state에서 끝나지 않은 원천은 다음
실행이 먼저 완료한다. SEC의 `load_known`과 FRED의 수집기 자신의 observations 원천은 receipts 원천이 commit된
batch의 것만 읽는다.

**SEC.** 실행은 이 수집기의 commit된 batch만 읽는다. 읽은 색인 날, 색인이 말하는 공시, submissions 답이
나열한 공시, companyfacts 답이 사실을 준 공시다. 그 다음 세 단계로 묻는다.

1. **색인.** 처음 알려진 색인 날(없으면 오늘의 30일 전, `--since`가 있으면 그 날)부터 어제까지의 평일 중
   덮이지 않은 날의 `daily_index`를 묻는다. 날은 `COMPLETED` 답으로 덮인다. 404(주말이 아닌 EDGAR
   휴일)는 그 다음 날이 끝난 뒤(New York 날짜로 이틀 뒤) 받은 것일 때만 덮는다. 그보다 이른 404는 아직
   게시되지 않은 색인일 수 있다.
2. **submissions.** 색인 공시 중 양식이 `filing_forms`(10-K·10-Q·10-KT·10-QT·8-K·20-F·40-F·6-K와 각
   정정)에 들고 발행인 범위(`registered`: `mint_issuer('sec_cik', cik)`가 identity에 등록된 제출자, 또는
   `all`)에 드는 공시를 원한다. 아직 나열되지 않은 공시가 있는 제출자마다 문서를 한 번 묻는다. 이유는 둘이다.
   - `new_filing`: 그 색인을 읽은 뒤 묻지 않음.
   - `listing_retry`: 마지막으로 물은 지 하루가 지났고, 색인을 읽은 지 `listing_days`(7일) 안.
3. **companyfacts.** 양식이 `fact_forms`(정기 재무 보고서와 그 정정)인 원하는 공시에 사실이 아직 없는
   제출자마다 같은 규칙으로 묻는다(재시도 이유 `fact_retry`, 기간 `fact_days` 14일). XBRL 처리가 접수보다
   늦을 수 있기 때문이다.

창이 지난 공시는 `abandoned`로 센다. 답을 보존하지 못한 attempt도 물은 것으로 보아 하루 뒤 다시 묻는다.
문서는 그 제출자의 이전 공시를 모두 다시 싣는다. 그래서 문서 요청의 receipt는 원한 accession 목록을
`selection`(`{"accessions": [...]}`)으로 싣고, 그 공시의 행만 테이블에 옮긴다. bytes는 `raw/`에 그대로 있다.

| 원천 접두어 | 테이블 | 행 |
| --- | --- | --- |
| `sec-daily-index-entries-` | `entries` | 답한 색인의 header 뒤 모든 줄. `day`, 원문 `line`, 다섯 칸을 읽은 `cik`(10자리)·`company`·`form`·`date_filed`·`accession`(`edgar/data/<cik>/<accession>.txt`), `retrieved_at_utc`, `receipt_sha256`. 다섯 칸으로 읽히지 않는 줄은 읽은 칸이 null이다 |
| `sec-submissions-filings-` | `filings` | `sec.submissions_filings@1`의 열과 같은 열로, 문서의 `filings.recent` 중 원한 공시의 위치. `sec.submissions@1`이 읽는다 |
| `sec-companyfacts-facts-` | `facts` | 원한 공시의 사실. `cik`, `taxonomy`, `tag`, `unit`, `period_start`, `period_end`, `accession_number`, `form`, `filed`, 숫자의 JSON 원문 텍스트 `value`, `fy`, `fp`, `frame`, `retrieved_at`, `receipt_sha256`. `sec.companyfacts@1`이 읽는다 |

되풀이된 key, 알 수 없는 필드, 빠진 필수 필드, 십진수가 아닌 값, 다른 CIK의 문서는 그 답을 `FAILED`로 만든다.
색인 줄의 CIK와 `File Name` 경로의 CIK가 다르면 그 줄은 공시가 아니다. 행을
다듬지 않고, 공급자가 쓴 미래 기간 끝도 그대로 남긴다.

**FRED/ALFRED.** ALFRED는 실시간 구간을 질의 창으로 자른다. 창 시작보다 먼저 시작한 vintage는 창 시작에
시작한 것으로, 창 끝보다 늦게 끝나는 구간은 창 끝에 끝나는 것으로 답한다. FRED는 vintage 날짜가 2000개를
넘는 observations 창을 거부한다. 수집기는 창마다 1990개까지만 담아, FRED가 창 시작일을 세든 세지 않든 한도
안에 둔다. 그래서 시계열마다 다음처럼 묻는다.

1. `vintage_dates`가 알려진 vintage 날(`known`) 다음 날부터, 알려진 날이 없으면 ALFRED 원점(1776-07-04)부터,
   끝난 FRED 날(어제)까지의 vintage 날짜를 page마다 묻는다. 날짜가 없으면 그 시계열은 최신이다.
2. observations 창은 `known`(또는 원점)에서 그 날까지다. 창마다 vintage 날짜를 1990개까지(원점이 아닌
   시작일도 하나로 셈) 담고, 다음 창은 앞 창의 마지막 vintage 날짜에서 시작한다
   (`observation_windows`). 창마다 page를 끝까지 묻는다.
3. 원점이 아닌 창에서 창 시작일에 시작하는 행은 앞 창이나 앞선 수집이 이미 가진 구간을 다시 말한다(잘렸거나
   그날 실제로 시작). 다른 행은 FRED가 매긴 vintage다(`split_rows`). 앞의 행은 세기만 하고 뒤의 행을 옮긴다.

창은 끝난 날에서 끝나므로 반쯤 게시된 vintage 날을 수집하지 않는다. 창을 순서대로 묻고 처음 완결되지 않은
창에서 그 시계열을 멈추므로, 알려진 가장 늦은 vintage 날까지의 모든 vintage가 수집돼 있다. `known`은
`fred.alfred@1`이 읽을 수 있는 모든 commit된 테이블(legacy 편입분 포함)에서, 그 행을 받은 FRED 날보다
일찍 시작한 vintage의 가장 늦은 날이다. 창은 0부터의 모든 page가 같은 count로 `COMPLETED`이고 그 수의 행을
담을 때 완결이다. 완결되지 않은 창의 행은 옮기지 않으며 다음 실행이 같은 `known`에서 다시 묻는다.

| 원천 접두어 | 테이블 | 행 |
| --- | --- | --- |
| `fred-alfred-observations-` | `observations` | 완결된 창의 vintage 행. `series_id`, `observation_date`, `realtime_start`, `realtime_end`(창 끝으로 잘린 값), `value` 원문, `query_realtime_start`, `query_realtime_end`, `retrieved_at_utc`, `receipt_sha256`. `fred.alfred@1`이 읽는다 |
| `fred-series-csv-` | `observations` | `series_csv` 답 하나의 bytes만을 원본으로 한 원천. 행은 `fred.series_csv@1`과 같아 같은 bytes는 같은 원천이고, `fred.fx_series@1`이 읽는다 |

`series_csv`(기본 DEXKOUS, dataset `fx.usdkrw.fred`)는 FRED 날마다 한 번 받는다. 행이
`fred.series_csv@1`로 읽히지 않는 답은 `FAILED`라 그 날을 덮지 않는다. 기본 ALFRED 시계열은
legacy 거시 목록 35개와 DEXKOUS다. 수집기 자신의 watermark는 시계열마다의 `known`이고, 수집으로만
앞으로 간다. 승격된 dataset의 `watermarks`는 [승격 실행과 보고](#승격-실행과-보고)의 규칙대로 그 수집
원천을 승격할 때 앞으로 간다. 유지보수 승격은 수집된 원천마다 `vintage_partitions`의 구간을 순서대로
승격한다.

## 유지보수 실행

`aas maintain run`은 하루 한 번의 제한된 실행으로 공급자 답에서 승격된 head까지 간다. 코드는
`application/maintain.py`(단계와 보고), `application/maintain_config.py`(설정),
`application/maintain_qveris.py`(Qveris 요청·원장·적재), `storage/maintain_identity.py`(KR identity 증분),
`storage/maintain_promotion.py`(dataset chain 이어 붙이기), `application/install_receipt.py`(설치 receipt)이고,
명령은 `aas maintain plan|run|receipt`, 예약은 `config/systemd/aas-maintain.{service,timer}`다.

**단일 writer.** 실행은 설치본을 쓰기로 한 번 열고 그 저장소 잠금을 실행 끝까지 갖는다. 그래서 실행 중에는
유지보수가 시장 store의 유일한 writer이고, 다른 명령이나 두 번째 실행은 `installation_busy`로 거부된다.

**단계.** 순서대로 실행한다. 한 단계가 실패하면(DuckDB·SQLite 오류 포함) 그 사실을 기록하고 다음 단계는
commit된 것으로 계속한다. 공급자 하나의 거부나 실패가 다른 공급자를 멈추지 않는다.

| 단계 | 하는 일 |
| --- | --- |
| `recover` | 중단된 operation(승격, 원천 적재)을 보존한 증거로 끝내거나 남긴다. 공급자를 부르지 않는다 |
| `runtime` | 실행 중인 interpreter·package와 설치 receipt의 차이 |
| `calendars` | 패키지 선언마다 [선언 달력](#선언-달력) 갱신. 시계가 선언 시각보다 이르면 `declared_after_now`로 건너뛴다. 다음 해 말까지 덮지 않는 달력은 `renewal_due`에 남는다 |
| `collect` | `jobs.enabled`일 때 켜진 공급자마다 실행 상한 안에서 KIND 목록, OpenDART rolling cohort, SEC EDGAR, FRED/ALFRED, Qveris 순으로 묻는다 |
| `import` | 완료된 Qveris job과 KR 종목 목록을 내용 원천으로 commit한다 |
| `identity` | 새 KR 상장을 등록하고 유지보수 identity snapshot을 pin한다 |
| `promote` | 카탈로그 dataset chain마다 새 원천을 다음 generation으로 승격한다 |
| `heads` | 모든 committed dataset의 head version, 행 수, `watermarks` |

**설정.** `runtime.json`의 `jobs.enabled`는 예약 공급자 호출의 설치본 grant이고, `providers`의 공급자 절마다
`"enabled": true`와 자격 증명 파일(상대 경로는 설치본 `secrets/` 안), 실행 상한을 적는다. `jobs.enabled`가
`false`이면 수집 단계는 `jobs_disabled`이고 나머지 단계는 그대로 돈다. 알 수 없는 필드, 잘못된 타입, 빠진 필수
필드는 어느 단계보다 먼저 설정 전체를 거부하므로 오타가 상한을 조용히 끄지 않는다. 자격 증명 파일은 그 공급자를
부를 때만 읽는다.

| 절 | 필수 | 상한과 선택 |
| --- | --- | --- |
| `kind` | 없음 | 없음(quota 없음) |
| `dart` | `key_file` | `max_calls`(2,000), `daily_quota`(19,000) |
| `sec` | `user_agent_file` | `max_calls`(2,000), `issuers`(`registered`·`all`), `since` |
| `fred` | `key_file` | `max_calls`(500) |
| `qveris` | `key_file`, `raw_root`(절대 경로), `since`(거래소마다 첫 날), `max_calls`, `max_credits`(십진 문자열) | `extra_sessions`(거래소마다 `since` 이전에 따로 물을 session 날짜), `identity`(절대 경로의 identity 문서), `exchanges`, `datasets`, `symbol_lists`, `symbol_list_days`(7), `forex`, `forex_lookback_days`(10), `max_http_requests`, `time_limit_seconds`(3600), `request_interval`, `timeout_seconds`(45) |

Qveris의 유료 크레딧은 실행 하나의 호출 수·크레딧 상한과, page마다 다시 확인하는 계정 잔액(다른 예약을 뺀 값)
안에서만 쓴다. 상한이 막은 요청은 묻지 않은 채 다음 실행으로 남는다.

**Qveris 요청.** 요청은 공급자에게 묻는 것(도구, 하위 공급자, 시장, dataset, parameter)이고 관측일과 job ID는
들어가지 않는다. job은 한 관측일의 묻기 하나이므로 같은 요청을 다시 묻는 것은 그날 관측일의 새 job이다. raw
수집 root가 모든 묻기의 기록이며 묻기의 상태는 그 증거에서 읽는다.

| 묻기 상태 | 증거 |
| --- | --- |
| `completed`·`warned` | `complete.json`(`RAW_ACQUIRED`·`RAW_ACQUIRED_WITH_WARNINGS`). 공급자 경고도 완료다 |
| `failed` | intent가 있는 모든 page에 정산된 billing이 있고 완료가 없음. 답은 보존됐지만 쓸 수 없다 |
| `quarantined` | billing 없는 page를 운영자가 page나 병렬 group 단위로 격리함. 결과는 알 수 없고 격리가 최악의 예약을 남겼다 |
| `unsettled` | 격리되지 않은 billing 없는 page |

덮이지 않은 요청에서 `unsettled` 묻기는 그 요청을 잡아 둔다(`held`). 그 밖에는 가장 늦은 묻기가 정한다. 이전
관측일의 `failed`는 그 관측일 뒤 1, 2, 4, …, 최대 32일이 지나면 다시 묻는다(`failed_retry`). 간격은 연속한 정산된
실패마다 두 배이고, 둘 이상 연속 실패한 요청은 다음 시도일과 함께 `failing`에 남는다. `quarantined`는 다음 날
다시 묻는다(`uncertain_retry`). 격리된 page는 다시 실행되지 않고 요청이 새 job으로 물린다. 오늘 관측일의 묻기와
간격이 남은 실패는 기다린다(`waiting`). 물은 적이 없으면 `new`다. 종목 목록은 가장 늦은 완료의 관측일 뒤 `symbol_list_days` 동안 덮이고 그 뒤 다시 묻는다(`refresh`).

실행은 이 순서로 상한이 남는 동안 묻는다. (1) 거래소(`US`는 XNYS, `KO`·`KQ`는 XKRX) 선언 달력의 열린
session마다 일간 `prices`·`splits`·`dividends` 내려받기. 거래소의 `since`부터(관측일의 366일 전보다 이르지
않게) 관측일 전날까지와 그 거래소의 `extra_sessions`(그 이전의 명시한 공백)이고, 날짜, 거래소, dataset 순이다. 선언 범위 밖의 날은 `undeclared_days`로 보고하고 묻지
않는다. (2) KR 거래소 종목 목록(`KO`·`KQ`, 상장·상폐). 새 KR 상장의 identity 입력이다. (3) FX 쌍
이력(`<PAIR>.FOREX`, 관측일의 `forex_lookback_days` 전부터). 재시도(`failed_retry`, `uncertain_retry`)는 같은 순서로
모든 첫 묻기와 `refresh` 뒤에 오므로 상한이 남긴 만큼만 쓴다.

묻기 전에 실행은 앞선 실행이 남긴 `unsettled` 묻기와 완료 없이 정산된 묻기를 그 자신의 job으로 끝낸다. page를
정산·검증만 하고 새 page는 intent 전에 거부되므로 아무것도 실행하지 않는다. 중단된 실행이 유료 호출 뒤 남긴
묻기는 이렇게 다시 호출 없이 완료된다.

**Qveris 원장.** 실행의 묻기는 모두 `collection_ledger`의 attempt다(provider `qveris`, job
`qveris:<요청 SHA-256>`, 요청 SHA-256은 정규 JSON `["aas-qveris-request-v1", 도구, 하위 공급자, 시장, dataset,
parameter]`). cohort 전에 `reserved`(예약의 receipt는 그 묻기의 job fingerprint), 유료 실행 직전 `started`, 그 뒤
raw 증거로 정산한다. 완료와 정산된 실패는 그 묻기의 결정 증거(완료 문서나 billing의 SHA-256)를 나열한
`aas-qveris-attempt-v1` receipt를 `raw/`에 두고 `succeeded`, 운영자가 격리한 page(와 intent 없는 `started`
attempt)는 `uncertain`, 상한이나 잔액이 먼저 막아 실행하지 않은 묻기는 `released` event와 함께 `failed`다. 묻기가
`unsettled`인 attempt는 재개나 격리가 묻기를 정할 때까지 열린 채 남는다. 프로세스가 중단되면(KeyboardInterrupt,
SystemExit) 정산하지 않는다. 그 `started` attempt는 다음 실행이 묻기를 끝낸 뒤 같은 규칙으로 정산하므로 증거와
어긋나는 `uncertain`이 남지 않는다. 재개 뒤에도 `held` 묻기가 남은 실행의 Qveris 상태는 `stopped`다.

**적재.** 완료된 일간·FX job 중 어느 `qveris-*` 원천의 lineage에도 fingerprint가 없는 것을
[Qveris 적재](#qveris-수집과-원천-적재)로 commit한다. 가격·기업행동 행은 설정의 identity 문서가 있을 때만
적재하고, 없으면 `waiting_for_identity`로 보고한다. 완료된 KR 종목 목록 job은 [KR 등록](#kr-등록)의
`qveris-eodhd-exchange-symbols-*` 원천이 된다.

**identity 증분.** commit된 KR identity 원천(EODHD KR 종목 목록, KIND 목록, 완료된 `corp_codes` 답을 담은
OpenDART receipts 테이블) 중 `sl:` 연결이 가장 새로운 유지보수 snapshot보다 늦은 것이 있거나 유지보수 snapshot이
없으면, 등록된 KR assertion이 인용하는 모든 원천과 그 새 원천으로 KR registry 문서를 만들어 등록한다. 같은 행은
재사용되고 충돌은 문서 전체를 거부한다. 유지보수 identity snapshot은 등록된 모든 assertion의 snapshot이고 ID는
`maintain-`과 정렬한 assertion ID 목록의 SHA-256 앞 32자다. 그래서 registry가 그대로면 같은 snapshot이고
assertion 집합이 바뀐 실행만 새 snapshot을 등록한다. 아무것도 등록하지 않았고 유지보수 snapshot이 없으면 승격
template의 snapshot을 그대로 쓴다. US identity 원천(Norgate master, FMP profile, SEC archive)은 동결된 전환
입력이라 유지보수 증분이 없다.

**승격.** route 하나는 카탈로그 dataset 하나, 그것을 만드는 등록 매퍼, 수집 원천 모양(원천 ID 접두어와 테이블),
여러 dataset이 나누는 모양이면 route한 테이블의 모든 행이 만족할 조건이다. 일부 행만 만족하는 테이블은
`mixed`로 건너뛰고 빈 테이블은 `empty`다.

| dataset | 매퍼 | 원천 | 조건 |
| --- | --- | --- | --- |
| `filings.us.sec` | `sec.submissions@1` | `sec-submissions-filings-*` `filings` | |
| `fundamentals.us.sec` | `sec.companyfacts@1` | `sec-companyfacts-facts-*` `facts` | |
| `filings.kr.dart` | `dart.fnltt_filings@1` | `opendart-receipts-*` `receipts` | |
| `fundamentals.kr.dart` | `dart.fnltt@1` | `opendart-receipts-*` `receipts` | |
| `macro.us.alfred` | `fred.alfred@1` | `fred-alfred-observations-*` `observations` | |
| `fx.usdkrw.fred` | `fred.fx_series@1` | `fred-series-csv-*` `observations` | |
| `classifications.kr.kind` | `kind.industry@2` | `kind-listings-*` `listings` | |
| `prices.us.eodhd` | `eodhd.bars@1` | `qveris-bulk-bars-*` `bars` | 심볼이 `.US` |
| `prices.kr.eodhd` | `eodhd.bars@1` | `qveris-bulk-bars-*` `bars` | 심볼이 `.KO`·`.KQ` |
| `prices.kr.eodhd` | `eodhd.bulk_quarantine@1` | `qveris-bulk-quarantine-*` `quarantine` | 사유 `provider_reported_partial`, 거래소 `KO`·`KQ` |
| `prices.kr.eodhd.ref` | `eodhd.bars_adjusted@1`, `eodhd.bulk_quarantine_adjusted@1` | 위 두 모양 | 위와 같음 |

route는 chain을 이어 붙일 뿐 시작하지 않는다. template은 그 dataset에서 route의 매퍼를 쓴 가장 늦은 committed
generation의 명세(운영자의 백필이거나 앞선 유지보수 generation)다. head가 없으면 `no_head`, 그 매퍼의 generation이
없으면 `no_template`로 보고하고 아무것도 쓰지 않는다. 유지보수 명세는 template에서 다음만 바꾼다.

- `target.parent`: dataset head.
- `sources`: 새 원천 테이블 하나.
- `partition`: template이 null이면 null. `fred.alfred@1`이면 그 원천의 vintage partition을 순서대로(하나가
  generation 하나). 그 밖은 테이블의 매퍼 partition 날짜의 `[처음, 마지막 + 1일)`.
- 매퍼·시간 규칙·품질 규칙 인자의 모든 generation pin: 그 dataset의 지금 committed head. 갱신된 달력, 같은
  실행에서 먼저 게시된 SEC 공시가 그렇다. 그래서 route는 `filings.us.sec` 다음에 `fundamentals.us.sec`를 승격한다.
- `identity_snapshot`: 실행에 유지보수 snapshot이 있고 template이 snapshot을 pin하면 그 snapshot. 실행의
  `identity` 단계가 실패했으면 snapshot을 pin하는 template의 route는 아무것도 승격하지 않고
  `identity_unavailable`로 보고하며 그 원천은 다음 실행을 기다린다.
- `tombstone_policy`: `never`. 원천 하나는 전체 snapshot이 아니므로 유지보수 generation은 record를 지우지 않는다.

매퍼와 그 인자, 시간·숫자·품질 규칙은 template 그대로이므로 유지보수가 chain의 규칙을 바꾸지 않는다. 더 늦은
시간 규칙 세대 dataset(`<dataset>.r<N>`)도 자기 template으로 같은 방식으로 이어진다. 새 원천은 `sl:` 연결 시각,
같으면 원천 ID 순서다. 한 원천의 모든 단계가 승격되거나 변하지 않았으면 그 원천은 그 dataset에서 끝난 것이고,
유지보수는 dataset head version에 `maintain_source@1` 품질 검사 하나(원천 pin, 단계마다 partition·명세 SHA-256·
generation, 결과 `promoted`·`unchanged`)를 남긴다. 검사 ID는 (dataset, 원천, 테이블)에서 나오므로 다시 쓰지
않는다. pin한 snapshot이 행을 `unresolved`·`ambiguous`로 남긴 원천은 결과 `unresolved`로 기록하고, 그 snapshot과
매퍼 identity key(provider, namespace)로 그 snapshot이 해석에 쓰는 행(정정되지 않은 member의 token, instrument,
유효 구간)의 SHA-256을 함께 남긴다. 그 원천은 실행이 pin할 snapshot의 그 행이 달라지면 다시 계획되므로 뒤에 등록된 상장이 해석하는 행이
빠지지 않는다. 검사 ID는 그 SHA-256까지에서 나온다. 결과의 `unresolved`는 그런 원천이 있는 dataset이다. 거부되거나 막힌 단계는 아무것도 남기지 않으므로 그 원천은 다음 실행에 다시 계획된다. 단계 사이에서
멈춘 원천은 끝난 단계가 빈 delta로 다시 계획된다. 결과를 선언한 매퍼가 `promotion_coverage@1`을 남긴 원천도
끝난 것이다. 운영자의 승격이 pin한 원천은 첫 유지보수 실행에서 한 번 더 계획되어 `unchanged`로 기록되고, head보다
이른 내려받기는 `stale`이라 head를 대체하지 않는다.

매퍼가 아직 없는 dataset(`actions.us.eodhd`·`actions.kr.eodhd`의 Qveris `splits`·`dividends`, `status.kr.kind`,
identity가 해석한 US 보류 행의 `prices.us.eodhd` 재처리)의 수집 원천은 승격하지 않고 `unmapped`에 dataset별
테이블 수로 남는다.

**보고와 종료 코드.** 실행 보고(`aas-maintain-report-v1`)는 단계마다의 결과, 실패한 단계, 멈춘 공급자를 담고
정확한 bytes를 `raw/`에, 같은 bytes를 `<runtime>/maintain-report.json`에 둔다. 종료 코드는 모든 단계가
끝나면(예산 소진 포함) 0, 단계가 실패했거나 공급자가 거부로 멈췄으면 1, Qveris가 정산이나 격리가 필요한 불확실한
유료 호출로 멈췄으면 2다. `plan`은 설치본을 읽기 전용으로 열어 같은 단계를 계획만 한다. 자격 증명을 읽지 않고
공급자를 부르지 않으며 쓰지 않는다. `--promotions`는 새 원천마다의 승격 계획까지 현재 head에 대해 계산한다.

**설치 receipt와 예약.** 설치형 `aas`(태그된 dev 커밋을 정확한 CPython 경로로 `uv tool install`한 것)는
`aas maintain receipt [--lock uv.lock]`으로 `aas-install-receipt-v1`을 남긴다. 해석한 interpreter 경로,
`sys.version`과 버전 tuple, 구현, package version과 PEP 610 `direct_url.json`(저장소, 요청한 태그, 해석된
커밋), package `RECORD`의 SHA-256, import 경로의 모든 distribution `name==version` 목록의 SHA-256과 수, 설치에 쓴
lock의 SHA-256이다. receipt는 `<runtime>/install-receipt.json`에, 정확한 bytes는 `raw/`에 둔다. 실행마다 실행 중인
interpreter·package·환경을 receipt와 나란히 기록하고 다른 필드를 `differences`로 나열한다. 차이는 보고할 뿐
거부하지 않는다. 실행 기록이 무엇이 돌았는지의 기록이다. `config/systemd/aas-maintain.service`는
`%h/.local/bin/aas maintain run`을 실행하고(`Type=exec`, `RuntimeMaxSec=22h`, 중지는 `SIGINT`),
`aas-maintain.timer`는 매일 03:00 UTC에 그것을 시작하며 놓친 실행은 다음 부팅에서 따라잡는다. 사용자 unit은
system의 `network-online.target`에 순서를 걸 수 없으므로 service는 시작 전에 공급자 host 이름이 해석될 때까지
최대 5분 기다린다. 그래도 네트워크가 없으면 공급자 단계가 실패로 기록되고(종료 코드 1), 수집기는 빈 날짜에서
계획하므로 다음 실행이 놓친 날을 묻는다.

## 운영 전환 확인

운영 전환은 legacy 예약 unit이 쓰던 설치본을 `aas maintain`으로 옮기는 일회성 절차이고, 그 명령 순서는
[운영 문서](../operations.md#운영-전환)가 소유한다. 전환의 마지막 단계는 `aas maintain cutover-check`이며
`application/cutover.py`가 이 확인과 은퇴 기록을 소유한다. 확인은 설치본을 읽기 전용으로 열고, 백업은 전체를
다시 읽으므로 설치본 잠금을 잡기 전에 대조한다.

| 항목 | 통과 조건 |
| --- | --- |
| `schema` | core schema가 현재 버전(`inspect_core_schema`가 `current`) |
| `operations` | `PREPARED` storage operation이 없음 |
| `backup` | `validated_backup`이 모든 파일을 다시 해시해 받아들이고, manifest `deep`이 참이고, `installation_id`가 같고, 설치본의 `PREPARED`가 아닌 storage operation이 모두 백업 state 사본에 있고(사본은 `immutable`로 읽어 백업에 아무것도 만들지 않는다), 백업 루트의 장치가 설치본 루트·state·market·`raw/`의 장치와 모두 다름 |
| `units` | `systemctl --user list-unit-files`와 `list-units --all`의 `aas-*` unit이 `aas-maintain.service`와 `aas-maintain.timer`뿐이고, timer 파일이 `enabled`이고 timer가 `active`. `systemctl`을 실행할 수 없으면 실패 |
| `collectors` | `runtime.json`이 유지보수 설정으로 읽히고 `jobs.enabled`이며 `--expect-provider`로 이름 붙인 공급자 절이 모두 켜짐 |
| `install_receipt` | `aas-install-receipt-v1`이 있음. 실행 환경과의 차이는 `differences`로 보고만 한다 |
| `maintain_run` | `<runtime>/maintain-report.json`이 이 설치본의 `run` 보고이고 `status`가 `succeeded` |
| `legacy` | `--legacy-manifest` 파일마다 정확한 bytes가 `raw/`에 온전히 있고(`retained`), 같은 manifest SHA-256의 `--legacy-verify` 보고가 `mode: verify`이고 `complete: true`이며 그 보고가 나열한 원천(unit 원천과 보존 파일 목록 원천)이 하나 이상이고 모두 `committed`나 `retired`이고 이 설치본에 `COMPLETED` operation의 commit marker로 남아 있고(`verified`), 모든 항목 경로가 없으며, `--removed` 경로가 모두 없음. 읽을 수 없는 verify 보고가 있으면 실패 |

`aas import legacy` 실행은 첫 unit을 commit하기 전에 manifest bytes를 `raw/`에 남기므로 `retained`는 실행이
시작되었다는 것만 증명한다. 편입이 끝났다는 증명은 `--verify` 보고이고, 그 보고는 원본을 다시 읽어야 만들어지므로
원본을 지우기 직전에 만들어 둔다. 확인은 그 보고의 원천이 지금도 설치본에 commit으로 남아 있는지를 읽기 전용으로
대조한다(은퇴한 원천도 commit marker를 유지한다).

백업의 장치 조건은 `aas db source-retire`가 백업을 받아들이는 조건과 같다. 그래서 확인을 통과한 백업은 은퇴에도
쓸 수 있고, 백업 위치와 같은 장치에 설치본 루트나 state가 있는 설치본은 다른 장치로 옮겨야 통과한다. 같은
장치를 쓰는 경로 이름은 `shared_device_with`에 남는다.

보고 `aas-cutover-record-v1`은 확인 시각, `installation_id`, `passed`, 실패한 항목 이름(`failed`), 항목별 결과,
`source_retirements`를 백업 ID·operation·이유별로 묶은 원천·행 수(`retirements`), `raw/`의 파일 수와 bytes(`raw`)를
담는다. 은퇴와 compact는 `raw/`를 지우지 않으므로 은퇴한 원천의 원본 archive는 그 수에 남는다. `--record`는
모든 항목이 통과한 보고만, 보고가 `report_sha256`으로 이름 붙인 verify 보고의 정확한 bytes를 먼저 `raw/`에 남긴 뒤
(그 사이에 파일이 바뀌었으면 거부), 정규 JSON bytes로 `raw/`와 `<runtime>/cutover-record.json`에 남기고 그 SHA-256을
`record_sha256`으로 돌려준다. 실패한 보고는 출력만 하고 기록하지 않으며 종료 코드는 1이다.

## identity 등록과 chunked 문서

identity는 `storage/identity.py`가 state에 등록하고 `aas identity register|snapshot|show`가 CLI다.

**불투명 ID 발급.** issuer와 instrument의 ID는 영구 anchor 하나에서 나온다.

```text
instrument_id = "ins-" + sha256(정규 JSON ["aas-instrument-v1", anchor_namespace, token])
issuer_id     = "iss-" + sha256(정규 JSON ["aas-issuer-v1", anchor_namespace, token])
```

- 영구 anchor는 공급자가 재사용하거나 재배정하지 않는 식별자다. instrument는 `norgate_assetid`,
  `krx_isin`(KRX 상장 종목의 ISIN, 검사 숫자 확인. 국가 접두어는 발행인의 나라이므로 KRX에 상장한 외국
  기업은 자기 ISIN(예: `KY`)을 쓴다), issuer는 `sec_cik`(10자리 0 채움), `dart_corp_code`(8자리)다.
  namespace가 하나 늘 때는 이 목록과 정규 표기 검사를 함께 추가한다.
- 티커·EODHD 심볼·KRX 단축코드·경로·날짜는 anchor가 아니며 발급을 거부한다. 정규 표기가 아닌
  token(앞자리 0 누락 CIK, 검사 숫자가 틀린 ISIN 등)도 고쳐 쓰지 않고 거부한다.
- 티커·심볼·CUSIP·ISIN과 anchor 자체의 연결은 유효·지식 구간을 가진 `identity_assertions`다.
  `assertion_id`는 `"asr-" + sha256(정규 JSON ["aas-assertion-v1", 나머지 열 10개])`이므로 같은 주장은
  같은 ID다. ETF처럼 issuer가 없는 상품은 issuer null이다.
- instrument 행(issuer, asset_type, venue)은 처음 등록한 원천의 맥락이다. instrument와 issuer의
  시점별 연결은 namespace `issuer` assertion이고 token은 `<issuer_id>/<instrument_id>`
  (`issuer_link_token`)다. 여러 share class가 한 issuer를 가리킬 수 있으므로, 이 namespace의 겹침은
  token이 아니라 (provider, `issuer`, instrument)로 판정한다. 등록과 snapshot 문서 검증(v1 문서와
  part 사이) 모두 같은 키를 쓰므로 한 provider는 한 instrument를 같은 시점에 두 issuer에 연결하지 않는다. issuer null로 처음 등록한 instrument도 나중 원천이 이 assertion으로
  issuer를 연결한다.

**등록 문서 `aas-identity-registry-v1`.** `issuers`(anchor, name), `instruments`(anchor, issuer anchor
또는 null, asset_type, venue), `assertions`(instrument anchor, provider, namespace, token, 유효 구간,
`known_from_us`, `supersedes_assertion_id`, `source_snapshot_id`, `source_hash`) 세 배열이고 모든 키가
필수다. 모르는 키와 같은 anchor·같은 assertion의 반복은 거부한다. 파일은 SHA-256과 함께 받고 256 MiB까지다.

- 등록은 append-only다. 이미 있는 같은 행은 재사용하고 새 행만 한 트랜잭션에 넣는다. 같은 문서를
  다시 등록하면 아무것도 바뀌지 않는다. 어떤 경로도 identity 행을 UPDATE·DELETE하지 않는다.
- 정정은 바꿀 assertion을 `supersedes_assertion_id`로 가리키는 새 assertion이다. 정정의
  `known_from_us`는 앞 assertion보다 늦어야 하고, 정정을 실은 원천 snapshot의 `retrieved_at_us`보다
  이를 수 없다. 정정은 그 원천을 수집하기 전에는 알려지지 않았기 때문이다. 정정이 아닌 처음
  assertion은 선언한 과거 `known_from_us`를 그대로 쓴다(시간 규칙은 보수적 상한).
- `--plan`은 쓰지 않고 새 행·기존 행·충돌·누락 참조를 센다. 충돌이나 누락이 하나라도 있으면
  적용은 문서 전체를 거부한다.
  - 충돌: 같은 instrument ID에 다른 asset_type(`instrument_attributes`), 같은 provider
    key(provider, namespace, token)의 두 assertion이 유효·지식 구간 모두에서 겹치는데 한쪽이 다른 쪽의
    정정 chain에 있지 않음(`assertion_overlap`), 앞 assertion보다 늦지 않은 정정(`correction_not_later`),
    원천 수집 시각보다 이른 정정(`correction_before_source`).
    assertion의 지식 구간은 `known_from_us`에서 시작해 그것을 정정한 가장 이른 assertion의
    `known_from_us`에서 끝난다.
  - 누락: 등록되지 않은 원천 snapshot(보통 `aas db source-link` 전의 `sl:` 원천), issuer, instrument,
    정정 대상.
- issuer 이름은 처음 등록한 원천의 표시 이름이다. 다른 이름은 충돌이 아니라
  `issuer_name_differences`로 보고하고 저장된 이름을 유지한다. 같은 이유로 저장된 instrument와
  issuer·venue가 다른 선언은 `instrument_differences`로 보고하고 저장된 행을 유지한다. 시점별
  issuer는 `issuer` assertion, 상장 이전은 ticker@venue assertion이 기록한다.

**snapshot.** `aas identity snapshot --id ID [--provider P] [--namespace N]`은 등록된 assertion을
identity 문서로 투영한다. 선택한 assertion마다 member 하나이고, 유효 구간은 주장 그대로, 지식 구간은
위 정의 그대로다. 그래서 정정 이전 cutoff에서는 정정 전 주장이 보인다.

**chunked 문서.** identity와 universe 문서는 manifest 하나와 v1 part들로 등록한다. 전체 member 수에는
한도가 없지만 part 하나가 member가 참조하는 원천의 파일 목록 전체를 싣기 때문에, 파일 목록이 part
하나의 charge를 넘는 원천(내용 주소 raw 경로 기준 약 2,000개 파일)을 참조하는 member는 등록할 수 없다.
`--plan`과 등록은 그 원천 ID와 파일 수를 담아 거부한다.

- part는 [membership pins](membership-pins.md)의 v1 문서(`aas-identity-snapshot-v1`,
  `aas-universe-version-v1`) 그대로이고 이름은 `<root>#00000`부터 이어지는 다섯 자리 번호다.
  이 접미사는 part 전용이라 v1 단일 문서 등록은 그런 이름을 거부한다.
- 전체 문서를 먼저 v1 규칙으로 검증하고, member를 채움 순서대로 part마다 1 MiB 정규 bytes와 64 MiB
  materialization charge 안에서 탐욕적으로 채운다. 같은 내용은 항상 같은 part와 hash가 된다. 채움 순서는
  identity가 정규 순서, universe가 `source_snapshot_id` 다음 정규 순서다. universe 원천 하나가 수백 개
  파일을 싣고 part는 인용한 원천의 파일 목록 전체를 실으므로, 원천별로 채워야 part 하나가 여러 원천의
  목록을 함께 싣지 않는다.
- manifest는 `{"schema": "aas-identity-manifest-v1" | "aas-universe-manifest-v1", "hash_format",
  root 키, "parts": [{part 이름, "content_hash"}]}`의 정규 JSON이고 pin의 `content_hash`는 그
  SHA-256이다. root header는 기존 `identity_snapshots`·`universe_versions` 행이며 새 테이블은 없다.
- manifest root header는 자기 member 행을 갖지 않는다. member가 있는 root는 v1 문서로 읽으므로,
  manifest root 아래에 끼워 넣은 member 행은 재구성 hash가 맞지 않아 읽기와 검증에서 거부된다.
- 읽기는 part 전부의 charge 합을 호출자 allowance에서 받은 뒤 part마다 v1으로 재구성하고, part
  사이의 채움 순서, identity 구간 겹침, 두 part에 걸친 같은 universe member key(instrument,
  `valid_from_us`, `known_from_us`)를 확인한다. 읽은 universe member는 전체 문서의 정규 순서로 돌려준다. part는 64 MiB charge 가까이 채워지므로 manifest
  하나를 읽는 데 part 수 × 약 64 MiB의 allowance가 필요하다(US 등록 전체 129,998 member는 208 part ≈ 13 GiB).
  소비자 allowance를 이 크기에 맞추는 일은 소비자 연결(PR 27)의 몫이다. `aas db verify`는 part를
  각자 검증하고 manifest는 part header와 경계만으로 확인한다.

### KR 등록

KR identity는 세 원천을 identity 매퍼로 읽어 `aas-identity-registry-v1` 문서 하나로 만들고, 그 문서를
다른 등록 문서와 같은 `aas identity register`로 덧붙인다. 코드는 `storage/kr_identity.py`다.

| 매퍼 | 원천 | 만드는 행 |
| --- | --- | --- |
| `eodhd.kr_symbol@1` | EODHD 거래소 종목 목록(`KO` 유가증권, `KQ` 코스닥, 상장·상폐 목록) | `Isin`이 검사 숫자가 맞는 ISIN이고 원화인 행의 instrument `mint('krx_isin', Isin)`(venue는 두 시장의 운영자 `XKRX`), assertion `eodhd`/`eodhd_symbol`(`<Code>.<Exchange>`, `eodhd.bars@1`이 해석하는 token)과 `eodhd`/`krx_short_code`(`Code`가 6자리 단축코드일 때) |
| `kind.listings@1` | KIND 상장법인목록(유가증권, 코스닥) | assertion `kind`/`krx_short_code`, 유효 구간은 상장일의 Asia/Seoul 0시부터 |
| `dart.corp_codes@1` | DART `corpCode.xml` 응답(원천 자료실의 `corp_codes` receipt 한 행) | 종목코드가 있는 회사의 issuer `mint('dart_corp_code', corp_code)`(이름은 `corp_name`), 그 종목코드의 instrument에 대한 `dart`/`issuer` assertion과 instrument 행의 issuer |

- asset_type은 `etf`(EODHD `ETF`)와 `unclassified`(EODHD `Common Stock`·`Preferred Stock`)다. EODHD는
  KRX 우선주도 `Common Stock`으로 실으므로 주식의 종류를 단정하지 않는다. 등록된 asset_type은 바꿀 수
  없으므로 보통주·우선주 구분은 분류 dataset이 기록한다.
- KIND와 DART는 ISIN을 싣지 않으므로 EODHD가 정확히 하나의 ISIN에 묶은 단축코드로만 instrument에
  닿는다. 단축코드·종목코드·이름에서 ISIN이나 ID를 유도하지 않는다.
- 한 심볼은 모든 목록을 함께 보고 판정한다. ISIN이 비어 있는 행만 ISIN을 실은 목록에 판정을 맡긴다.
  ISIN을 실은 목록끼리 ISIN·asset_type·통화 중 하나라도 다르면, 한쪽이 틀린 ISIN이거나 알 수 없는
  유형이어도, 어느 쪽도 고르지 않고 `symbol_ambiguous`로 둔다. ISIN과 단축코드의 모호성은 해석되지 않은
  심볼의 행까지 포함해 발급 가능한 모든 행으로 판정한다.
- 다음은 해석하지 않고 이유와 함께 보고한다: ISIN이 없거나(`isin_missing`), 검사 숫자가 틀리거나
  (`isin_invalid`), 알 수 없는 종목 유형(`type_unknown`), 원화가 아님(`currency_not_krw`), 목록끼리 다름
  (`symbol_ambiguous`), 한 ISIN이 두 단축코드나 두 유형을 가짐(`isin_ambiguous`), 한 단축코드가 두 ISIN을
  가짐(`short_code_ambiguous`), 한 종목코드를 두 회사가 가짐(`stock_code_ambiguous`), 한 회사가 두
  종목코드를 가짐(`corp_code_ambiguous`), 종목코드가 ETF를 가리킴(`stock_code_is_etf`), 단축코드가
  instrument에 닿지 않음(`short_code_unresolved`), KIND가 한 단축코드에 두 상장일을 줌
  (`listing_ambiguous`), 다듬어지지 않은 회사명(`corp_name_invalid`).
- DART `corp_codes` receipt 여러 개를 함께 읽는다. 회사의 이름과 근거는 가장 이른 receipt의 것이다.
- EODHD 목록에는 날짜가 없으므로 그 assertion은 공급자 시계열 전체에서 유효하다(`valid_from_us`는
  int64 최소값, `valid_to_us`는 null). 모든 assertion의 `known_from_us`는 그 원천 receipt가 기록한
  수집 시각이다. AAS가 그 주장이 공개돼 있었다고 보일 수 있는 가장 이른 시각이며 보수적 상한이다.
  그래서 그 시각보다 이른 결정 시점의 strict 멤버십 판정은 이 assertion을 보지 않는다. 승격의 identity
  해석은 지식 구간이 아니라 유효 구간과 정정 여부로 하므로 이력 전체의 가격을 해석한다.
  `source_snapshot_id`는 원천의 `sl:` ID이고 `source_hash`는 그 주장이 나온 원천 행의
  `aas-source-row-v1` 해시다. ETF와 DART 종목코드가 가리키지 않는 종목(우선주 등)은 issuer가 null이다.
- 문서의 issuer는 corp code, instrument는 ISIN, assertion은 (provider, namespace, token) 순이므로 같은
  원천에서 늘 같은 bytes가 나온다.
- 빌드 입력은 누적이다. 등록된 KR assertion(provider `kind`, `dart`의 `issuer`, `eodhd`의
  `krx_short_code`와 `.KO`·`.KQ` 심볼)이 인용하는 원천은 모두 빌드 원천에 들어가야 하고, 빠지면
  `kr-build`는 더할 원천을 이름으로 보고하며 거부한다. 그러면 이미 등록된 주장은 같은 가장 이른 근거로
  같은 assertion ID가 되어 재사용되고, 새 수집물은 새 주장만 더한다. 새 원천만으로 빌드하면 같은 key가
  다른 근거로 나와 등록이 `assertion_overlap`으로 전체를 거부하기 때문이다.
- 정정되지 않은 등록 assertion을 이번 원천이 더는 내지 않으면(나중 목록이 그 key를 모호하게 만든 경우)
  보고의 `withdrawn`에 남긴다. 그 주장이 언제부터 틀렸는지 말하는 원천이 없으므로 빌더는 그것을 닫지
  않고, 정정은 그 assertion을 대체하는 별도 등록이다.

KIND 목록과 EODHD 종목 목록 수집물은 `aas identity kr-import`가 내용 원천으로 commit한다(KIND는
`aas collect kind run`이 수집하면서 같은 원천으로 commit한다, [KR 공시·상장 수집](#kr-공시상장-수집)). KIND는
receipt(`response.json`)와 그것이 크기·SHA-256으로 가리키는 응답 하나가 한 단위이고
(`kind-listings-<hex>`, 테이블 `listings`), EODHD는 수집 job 하나의 `complete.json`과 그것이 나열한
파일이 한 단위다(`qveris-eodhd-exchange-symbols-<hex>`, 테이블 `symbols`). 행은 응답의 셀 값을 텍스트
그대로(KIND는 EUC-KR 응답의 공백만 접은 셀 텍스트) 담고 요청 목록, 거래소, 상폐 요청 여부, job 상태,
수집 시각을 함께 싣는다. 공급자 경고(`RAW_ACQUIRED_WITH_WARNINGS`)는 행의 job 상태로 남는다.
receipt가 기록한 크기·해시와 다른 파일, 완료 문서에 없는 파일은 거부한다. `aas identity kr-build`는
commit된 원천을 pin과 대조해 읽고 문서와 보고를 새 파일에 쓰며 설치본에는 쓰지 않는다. 두 파일은
모두 없을 때만 온전히 쓴 뒤 함께 만들어지고, 실패하면 둘 다 남지 않는다.

### US 등록

US identity는 다섯 원천을 identity 매퍼로 읽어 `aas-identity-registry-v1` 문서 하나로 만들고, 그 문서를
`aas identity register`로 덧붙인다. 코드는 `storage/us_identity.py`다. Norgate는 동결 원천이므로 instrument는
Norgate security master와 그보다 늦은 Norgate history 내보내기에서만 나온다.

| 매퍼 | 원천 | 만드는 행 |
| --- | --- | --- |
| `norgate.master@1` | Norgate security master(원천 자료실 `observations`: `assetid`, `symbol`, `is_delisted`, `currency`, `is_etf`, `first_date`, `last_date` 등) | 행마다 instrument `mint('norgate_assetid', assetid)`(venue `XNYS`), assertion `norgate`/`norgate_assetid`(그 asset ID)와 `norgate`/`norgate_symbol`(Norgate 자신의 심볼) |
| `eodhd.us_symbol@1` | 같은 master의 상장(상폐 아님) 행 | 그 티커가 상장 행 하나에만 해당할 때 assertion `eodhd`/`eodhd_symbol`(`<티커>.US`, `eodhd.bars@1`이 해석하는 token) |
| `fmp.profile@1` | FMP company profile(`symbol`, `cik`, `cusip`, `isin`, `isEtf`, `currency`, `retrieved_at_utc`) | 상장 티커와 같은 심볼의 행들이 서로, 그리고 Norgate 행과 맞을 때 assertion `fmp`/`fmp_symbol`, `fmp`/`cusip`, `fmp`/`isin` |
| `norgate.export_listing@1` | `norgate.history_export@1`로 편입한 history 내보내기(`norgate-history-csv`의 `bars`). 시리즈(asset ID, 심볼, database)마다 첫 행과 날짜 범위 | master에 없는 asset ID의 instrument(주식은 `unclassified`·`XNYS`, 기준 시리즈는 database의 유형 `index`·`economic_series`·`fx_spot`·`commodity`·`continuous_future`과 venue `XXXX`)와 `norgate`/`norgate_assetid`, 상장 주식이 아닌 시리즈의 영구 심볼 `norgate`/`norgate_symbol`, master의 `through` 뒤 구간의 상장 티커 주장(아래) |
| `sec.tickers@1` | SEC submissions archive(`sec.submissions_zip@1`로 편입한 내용 원천의 member 색인과 `raw/`의 archive) | 티커를 하나의 CIK만 싣고 FMP가 같은 CIK를 줄 때 issuer `mint('sec_cik', cik)`(이름은 SEC `name`), `sec`/`issuer` assertion과 instrument 행의 issuer |

- venue는 모든 US 상장(Nasdaq, NYSE Arca, OTC 포함)이 따르는 세션 달력 `XNYS`다. 상장 거래소는
  분류 dataset의 몫이다. asset_type은 `is_etf`가 참이면 `etf`, 그 밖은 `unclassified`이며 주식·우선주·
  ETN 등의 구분은 분류 dataset이 기록한다.
- 상장 행의 US 티커는 Norgate 심볼의 class 구분자 `.`를 `-`로 쓴 것이다(SEC·FMP·EODHD 표기,
  `BRK.B` → `BRK-B`). 이 표기 규칙 말고는 티커를 바꾸지 않는다. 두 상장 행이 같은 티커가 되면 어느
  쪽도 고르지 않는다. 상폐 행은 Norgate가 붙인 접미사 심볼(`XYZ-201203`)만 갖고 현재 티커가 없으므로
  Norgate assertion만 받는다.
- 티커로 instrument나 issuer를 만들지 않는다. 공급자 사이의 티커 대조는 모든 공급자가 맞을 때만
  연결한다. 다음은 해석하지 않고 이유와 함께 보고한다: 두 상장 행의 같은 티커(`ticker_ambiguous`),
  상장 티커가 아닌 FMP 심볼(`not_a_listed_norgate_ticker`), CIK·CUSIP·ISIN이 정규 표기가 아니거나
  검사 숫자가 틀린 FMP 행(`fmp_identifier_invalid`), 한 심볼에 다른 내용을 주는 FMP 행들
  (`fmp_profile_ambiguous`), USD가 아닌 FMP 행(`fmp_currency_not_usd`), Norgate와 다른 ETF 여부
  (`fmp_type_differs`), 두 티커가 같은 CUSIP·ISIN(`cusip_ambiguous`, `isin_ambiguous`, 그 식별자
  assertion만 빠진다), SEC에 없는 티커(`sec_ticker_missing`), 두 CIK가 싣는 티커
  (`sec_ticker_ambiguous`), 합의된 FMP profile이 없는 티커(`fmp_profile_missing`: FMP 행이 없거나 위
  FMP 이유로 미해결인 경우), 합의된 FMP profile에 CIK가 없는 티커(`fmp_cik_missing`), SEC와 다른 FMP
  CIK(`fmp_cik_differs`).
  master 행 중 asset ID가 양의 정수가 아니거나(`assetid_invalid`), 심볼이 다듬어진 텍스트가 아니거나
  (`symbol_invalid`), USD가 아니거나(`currency_not_usd`), 상폐 여부가 불리언이 아닌 행
  (`listing_state_unknown`)은 거부하고, 같은 asset ID의 두 행은 둘 다 등록하지 않으며(`assetid_repeated`),
  같은 심볼의 두 행은 asset ID 주장만 받는다(`symbol_repeated`). `retrieved_at_utc`가 시간대 있는 시각이
  아닌 FMP 행은 그 심볼만 `fmp_retrieved_invalid`로 두고 빌드는 계속한다. master 날짜로 티커 구간을 정할
  수 없는 상장 티커도 미해결이다: `first_date`가 없는 상장 행(`listing_start_unknown`), 같은 티커의 상폐
  행에 `last_date`가 없는 경우(`ticker_reuse_unbounded`), 이전 보유 상폐 행이 master의 마지막 관측
  세션까지 거래된 경우(`ticker_reused`).
- SEC 매퍼는 member 색인의 `CIK##########.json` 행만 읽고 쪽 나눈 이력(`-submissions-NNN`)과 그 밖의
  member는 건너뛴다. archive는 내용 원천이 기록한 크기·SHA-256, member는 색인 행이 기록한 크기·SHA-256과
  같아야 하며 다르면 원천 전체를 거부한다. 문서의 `cik`가 member 이름의 CIK와 다르면 그 행을 거부한다.
- 유효 구간: 티커는 master가 보여 주는 동안만 그 상장을 가리킨다. 상장 행의 티커에서 나온 주장
  (Norgate 자신의 상장 심볼, EODHD `<티커>.US`, FMP 심볼)은 다음 구간에서 유효하다.
  - 시작: 상장 행의 `first_date`와, 같은 티커의 이전 보유자를 보여 주는 상폐 행(심볼 `<티커>-YYYYMM`,
    class 구분자는 같은 규칙으로 맞춘다) 중 가장 늦은 `last_date`의 다음 날 가운데 늦은 날의 New York 0시.
  - 끝: master의 마지막 관측 세션(모든 행의 `first_date`·`last_date` 중 최댓값, 보고의 `through`) 다음 날의
    New York 0시. Norgate는 동결 원천이므로 그 뒤에 누가 티커를 갖는지는 master가 말하지 않는다.
  - `eodhd.bars@1`은 세션 날짜의 New York 0시로 해석하므로 구간 밖의 bar는 미해결로 남는다.
  - history 내보내기는 master보다 늦게 끝날 수 있다. 그때 내보내기 창은 `through` 다음 날의 New York 0시부터
    내보내기의 마지막 주식 세션(보고의 `export_through`) 다음 날의 New York 0시까지다. `US Equities` 시리즈의
    티커(Norgate 심볼, EODHD `<티커>.US`)는 창 시작, 그 시리즈의 첫 날짜, 같은 티커의 상폐 시리즈
    (`<티커>-YYYYMM`) 마지막 날짜의 다음 날 중 가장 늦은 날부터 창 끝까지 유효하다. 기존 주장과 겹치지 않는
    구간이므로 충돌하지 않는다. 두 상장 시리즈가 같은 티커면 `export_ticker_ambiguous`, master가 그 티커를 준
    다른 상장을 내보내기가 그 티커의 이전 상폐 보유자로 보여 주지 않으면 언제 옮겨 갔는지 알 수 없으므로
    `export_ticker_moved`, 시작이 창 끝 이후면 `export_ticker_reused`로 미해결이다. 같은 asset ID의 시리즈가
    여러 원천에서 심볼이나 database가 다르면 그 asset ID 전체를 거부한다(`export_assetid_repeated`). 같으면
    모든 원천의 날짜를 합치고, 마지막 날짜에 닿는 원천 중 가장 이른 원천을 근거로 삼으므로 나중 원천이 늘린
    구간이 더 이른 시각부터 알려지지 않는다. master는 미국 주식만 담으므로 master의 asset ID를 기준 시리즈
    database로 내보낸 시리즈는 instrument도 이름도 만들지 않고 `export_database_differs_from_master`로
    미해결이다.
  - 티커로 대조하는 FMP profile과 SEC member는 그 수집 시각을 담은 티커 주장(master 구간 또는 내보내기 창)의
    시리즈만 가리킨다. FMP 행들의 합의도 주장마다 그 안에서 수집한 행만으로 판단하고, 합의한 주장마다 그
    구간의 FMP 심볼 주장이 생긴다. ETF 여부는 master 행의 것이며 master에 없는 시리즈는 ETF가 아니다. 어느
    주장에도 들지 않는 행은 다른 보유자를 말할 수 있으므로 해석하지 않고, 가장 이른 행의 위치로 이유를
    남긴다(`fmp_before_ticker_claim`, `fmp_between_ticker_claims`, `fmp_after_master_through`,
    `fmp_after_export_through`, 같은 접미사의 `sec_*`). SEC member의 수집 시각은 그 원천 `sl:` 연결의
    `retrieved_at_us`다. 한 시리즈는 issuer 연결을 하나만 가지며(가장 이른 유효 시작), 다른 주장이 그
    시리즈에 다른 CIK를 대면 `sec_cik_differs_across_claims`로 미해결이다. CUSIP·ISIN은 두 시리즈가 같은 값을
    가질 때 모호하고, 한 시리즈의 같은 값은 가장 이른 profile에서 한 번만 주장한다.
  - asset ID와 상폐 행의 접미사 심볼(그 상장만의 영구 이름)은 공급자 시계열 전체(`valid_from_us`는
    int64 최소값, `valid_to_us`는 null)다. FMP의 CUSIP·ISIN은 profile이 수집 시점의 현재 값만 말하므로
    그 수집 시각부터 유효하다. issuer 연결도 SEC와 FMP가 지금의 티커-CIK 대응만 말하므로 두 수집 시각 중
    늦은 시각부터 유효하다. 지주회사 재편처럼 그 전의 CIK가 따로 있으면 그 기간을 겹치지 않는 별도
    issuer 연결로 더한다.
  - 티커 구간은 master와 history 내보내기만 만들므로 `export_through`(내보내기가 없으면 `through`) 뒤의
    EODHD bar와 그 뒤에 수집한 FMP·SEC 행은 해석되지 않는다. `prices.us.eodhd` 승격은 그 다음 날부터의 구간을
    등록하는 날짜 있는 US 심볼 매퍼(이후 수집한 EODHD US exchange-symbol 목록 등)가 생기기 전까지 그 행을
    미해결로 둔다.
- 지식 시각: Norgate master와 SEC member 색인은 행에 수집 시각이 없으므로 그 원천 `sl:` 연결의
  `retrieved_at_us`(편입 intent가 완료된 시각)부터 알려진다. FMP 주장은 그 행의 `retrieved_at_utc`부터,
  issuer 연결은 기대는 세 근거(SEC, FMP, Norgate) 중 가장 늦은 시각부터 알려진다. 연결되지 않은 원천은
  빌드가 거부한다. `source_snapshot_id`는 주장이 나온 원천의 `sl:` ID, `source_hash`는 그 행의
  `aas-source-row-v1` 해시다. EODHD 심볼의 근거는 Norgate 행이고 issuer 연결의 근거는 SEC member 행이다.
- 원천 행은 Arrow로 읽으므로 `TIMESTAMP WITH TIME ZONE` 열은 시간대 자료 없이 같은 순간으로 읽힌다.
- 문서의 issuer는 CIK, instrument는 asset ID 수 순서, assertion은 (provider, namespace, token) 순이므로
  같은 원천에서 늘 같은 bytes가 나온다.
- 내보내기 주장의 근거는 그 시리즈의 첫 행이고 지식 시각은 그 원천의 `sl:` 연결 시각이다.
- 빌드 입력은 누적이다. 등록된 US assertion(provider `norgate`, `sec`, `fmp`와 `.US` EODHD 심볼)이
  인용하는 원천은 모두 빌드 원천에 들어가야 하고, 빠지면 `us-build`는 더할 원천을 이름으로 보고하며
  거부한다. 정정되지 않은 등록 assertion을 이번 원천이 더는 내지 않으면 `withdrawn`에 남기고 닫지 않는다.
- `--bindings`는 legacy identity bindings 테이블(`norgate`/`norgate_assetid`/`resolved` 행의
  `provider_identifier`)과 발급한 asset ID 집합을 비교해 보고하며 주장을 더하지 않는다. 두 집합은
  `aas-norgate-assetids-v1` 해시(정규 JSON `["aas-norgate-assetids-v1", 오름차순 asset ID 정수]`의
  SHA-256, 중복과 입력 순서는 무시한다)로도 보고한다.

### universe 등록

지수 구성과 US 상장 목록은 universe 매퍼로 읽어 [membership pins](membership-pins.md)의
`aas-universe-version-v1` 문서로 만들고, [chunked 문서](#identity-등록과-chunked-문서)로 등록한다.
코드는 `storage/universe.py`, 명령은 `aas universe`다.

| 매퍼 | 원천 | 만드는 universe |
| --- | --- | --- |
| `norgate.index_membership@1` | legacy 편입한 Norgate `index_constituent_timeseries`(원천 자료실 `norgate-index-membership-*`, 테이블 `constituents`: asset ID, 지수 이름, `date`, `index_constituent` 원문) | 지수마다 `index.us.norgate/<지수 이름>` |
| `norgate.listings@1` | Norgate security master(`norgate.master@1`이 읽는 같은 `observations`) | `listing.us.norgate` |

- 지수 구성 원천은 (asset ID, 지수) 쌍마다 그 쌍의 날짜별 `0`/`1` 값이다. 원천 행 순서로 연속한 `1`
  행 한 묶음이 member 하나이고, 유효 구간은 첫 날짜의 New York 0시부터 마지막 날짜 다음 날의 New York
  0시까지다. 이 구간을 쌍 자신의 날짜에 펼치면 원래 값이 그대로 나온다. 주말처럼 쌍에 행이 없는
  날은 묶음을 끊지 않는다. 압축은 DuckDB SQL(gaps-and-islands)이고 `universe.compress`가 같은 규칙의
  참조 구현이다.
- 쌍은 통째로 받거나 거부하며 고쳐 쓰지 않는다: asset ID가 양의 정수가 아님(`assetid_invalid`),
  지수 이름이 다듬어진 텍스트가 아님(`indexname_invalid`), 값이 `0`/`1`이 아님(`constituent_invalid`),
  날짜가 `YYYY-MM-DD`가 아니거나 다음 날을 나타낼 수 없는 `9999-12-31`임(`date_invalid`), 날짜가 원천 행 순서로 엄격히 증가하지 않음
  (`dates_not_increasing`, 같은 날짜의 반복 포함), 두 원천이 같은 쌍을 실음(`pair_repeated`, 이어 붙이지
  않는다). 두 원천이 실은 쌍은 한 사본이 다른 이유로 거부돼도 다른 사본을 받지 않는다: 이미 거부된 사본은
  제 이유를 유지하고 나머지 사본이 `pair_repeated`가 된다. 보고의 행 단위는 쌍이다.
- 상장 universe의 member는 master 행마다 `first_date`의 New York 0시부터 `last_date` 다음 날의 New York
  0시까지다. 상장 중이고 `last_date`가 없는 행은 master의 마지막 관측 세션(`through`, US 등록과 같은
  정의)까지다. `first_date`가 없거나(`listing_start_unknown`), 상폐 행에 `last_date`가 없거나
  (`listing_end_unknown`), 두 날짜가 거꾸로이거나(`listing_dates_reversed`), 끝 날짜가 다음 날을 나타낼
  수 없는 `9999-12-31`이거나(`listing_end_invalid`), 같은 asset ID의 두 행(`assetid_repeated`)은 member가
  되지 않는다. 그 밖의 master 거부 이유는 `norgate.master@1`과 같다.
- member는 identity 등록이 이미 가진 instrument `mint('norgate_assetid', assetid)`이고 문서의 instrument
  행은 등록된 행 그대로다. 등록되지 않은 asset ID는 문서에 넣지 않고 `unresolved`로 보고한다.
- 원천 행에는 수집 시각이 없으므로 member는 그 원천 `sl:` 연결의 `retrieved_at_us`부터 알려지고
  (`known_to_us`는 null), `source_snapshot_id`는 그 `sl:` ID다. 그래서 그 시각보다 이른 결정 시점의 strict
  멤버십 판정은 이 member를 보지 않는다. 더 이른 지식 시각은 다른 날짜 단위 원천처럼 버전 붙은 시간
  규칙과 소비자 grant의 몫이다.
- Norgate는 동결 원천이므로 member 구간은 마지막으로 내보낸 날짜 다음 날에 끝나며 그 뒤의 구성은 말하지
  않는다.
- universe version은 호출자가 정한다. 같은 원천과 identity 등록에서는 같은 문서와 pin이 나오고, 같은
  version으로 다시 등록하면 같은 pin을 재사용하고, 이미 있는 version에 다른 내용을 등록하면 아무것도
  바꾸지 않고 거부하므로 다른 내용은 새 version으로 등록한다.
- 보고는 universe마다 member·instrument·원천 수, 미해결 asset ID, 쌍·원천 행·한 번도 편입되지 않은 쌍의
  수, 원천 날짜 범위, 그리고 원천이 실은 날짜마다의 member 수(`daily_members`: 최소·중앙값·최대)다.

## 대량 게시와 reader

- `storage/bulk_generation.py`가 대량 게시를 소유한다. 입력은 연결에 보이는 staging 테이블이나 view
  하나이며, `generation_id`를 뺀 도메인의 typed 열(`record_id` 포함, 가격은 선택적으로 `fields`)을 정확히
  같은 타입으로 가진다. `plan_generation_bulk`는 아무것도 쓰지 않고 marker를 계산하며,
  `publish_generation_bulk`는 DuckDB 트랜잭션 하나에서 marker와 `INSERT … SELECT` 행을 함께 쓴다.
  테이블이 아닌 입력은 읽을 때마다 값이 달라질 수 있으므로, 삽입한 행을 COMMIT 전에 다시 해시하고
  계획과 다르면 `PlanChangedError`로 전체를 취소한다.
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
  행 수와 무관하다. 한 행도 들어가지 않으면 `ComputeResourceError`다. 가져올 행이 없는 빈 quality flag
  관계는 한 행 몫도 요구하지 않고 빈 rowset digest를 돌려준다. 정렬·spill·색인 유지는 같은
  할당에서 유도한 DuckDB 몫이 맡는다. 한도를 넘으면 트랜잭션 전체가 취소되고 `ComputeResourceError`가
  되며, 원래 DuckDB 오류는 그 cause로 남는다. 삽입은 들어갔는데 COMMIT이 색인 block을 할당하거나 pin하지
  못하면 DuckDB는 OOM이 아니라 `TransactionException`으로 보고하고, 고정 접두어 `Failed to commit: `
  바로 뒤의 원인이 `could not allocate`·`failed to allocate`·`failed to re-allocate`·`failed to pin block`·
  `failed to offload data block`이면 이것도 같은 오류다. 접두어는 그대로 맞추고 원인만 대소문자를 가리지 않으므로
  allocator의 `Failed to allocate block of N bytes (bad allocation)`도 용량 오류다. `max_temp_directory_size`를
  다 써서 spill하지 못한 `failed to offload data block ...`은 COMMIT 밖에서는 DuckDB의 OOM이라 그 자체로,
  COMMIT에서는 위 원인으로 용량 오류다. 그 밖의 COMMIT 실패(제약 위반, 쓰기 충돌)는 용량 오류가 아니므로 원래 오류 그대로 나간다.
  제약 위반이 인용한 key에 같은 문구가 들어 있어도 마찬가지다. 연결이 이미 쥔 메모리보다 낮은 한도로
  내리는 것을 DuckDB가 거절한 OOM도 이 경계 안에서 `ComputeResourceError`가 된다. COMMIT이 실패하면 DuckDB가 이미 트랜잭션을 끝내므로,
  뒤따르는 ROLLBACK의 "no transaction is active" 거절이 원래 오류를 덮지 않는다(`market.rollback`). 이
  경계(`market.budgeted`)는 `aas db migrate`의 market 단계와 `aas db compact`의 market 복사에도 쓰이며,
  둘 다 설치본 자체의 한도가 아니라 compute lease의 DuckDB 몫으로 낮춘 연결에서 실행한다. migration은
  admission 뒤 작업 공간에 lease를 걸어(`Workspace.market_lease`) PREPARED 승격의 carry 검사가 끝나며 한도를
  되돌릴 때도, 백업의 CHECKPOINT와 그 뒤 다시 연 연결, 이후 단계도 lease의 DuckDB 몫을 넘지 않는다.
- 기본 512 MiB 할당은 Python 몫만 행 수와 무관하게 보장한다. 1e7행 계획의 Python peak는 74 MiB다.
  core schema v3의 도메인 테이블과 `quality_flags`에는 key 색인이 없다([market v3](#core-schema-버전)).
  그래서 DuckDB 몫은 테이블에 쌓인 전체 행 수가 아니라 게시하는 delta를 부호화·정렬하고 기존 행과
  join하는 working set으로 정해지며, 그 연산자는 DuckDB 임시 디렉터리로 spill할 수 있다. 남는 색인은
  `FOREIGN KEY(generation_id)`의 색인과 `market_generations`·결과 테이블의 색인이다. 게시 트랜잭션의
  전역 `(record_id, revision_id)` 검사는 도메인 전체를 읽으므로 시간은 그 도메인의 행 수에 비례한다.
  `memory_limit`는 DuckDB 할당자의 설정이지 프로세스 RSS의 상한이 아니므로, 할당 grant는 Python 몫과
  DuckDB 몫을 따로 담는 [0016](../decisions/0016-maintenance-admission-budget.md)의 모델 그대로 정한다.
  할당 안에서 끝나지 않는 게시는 위와 같이 `ComputeResourceError`로 전체가 취소된다. v2 이하 설치본의
  `PRIMARY KEY(generation_id, record_id, revision_id)`와 `UNIQUE(record_id, revision_id)` 색인은 삽입과
  COMMIT 중 메모리에 통째로 올라와 DuckDB 몫이 그 테이블의 전체 행 수에 비례해 커진다. 합성 prices에서
  checkpoint된 8M행 테이블에 10k행 generation을 게시하는 데 1.5 GiB, 빈 테이블에 1e7행을 게시하는 데
  16 GiB 할당이 필요했다. 따라서 수천만 행 테이블의 백필과 유지보수 게시는 v3 설치본에서 한다(대응표 DV-75).
- `storage/read_heads.py`의 `read_heads(connection, binding, query, time_rules, budget)`가 pin한
  chain들의 head를 DuckDB 안에서 투영한다. 작업 공간 진입점은 `market_inputs.load_pinned_heads`이며,
  각 generation이 marker와 같은 committed catalog 버전인지 확인하고 시간 규칙 출처를 보존 증거에서
  읽은 뒤 `read_heads`를 부른다.
  - binding(`HeadBinding`)은 도메인 하나, `[from, to)` cutover 구간을 가진 순서 있는 exact pin 목록,
    grant 목록, flag 제외 목록이다. 정규 JSON 문서 `{"schema": "aas-head-binding-v1", "domain",
    "pins": [{"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash", "from", "to"}],
    "granted_rules", "excluded_flags"}`(목록은 정렬)의 SHA-256이 binding hash다.
  - query(`HeadQuery`)는 cutoff, 수집 cutoff, subject 목록, 날짜 구간, 가격 역할, coverage 격자다.
    cutoff가 있으면 strict PIT, 없으면 연구 모드다. 연구 모드의 query는 지식 상한(`known_ceiling_us`)을
    가질 수 있고, 그러면 `revision_known_at_us`가 상한보다 늦은 revision은 투영 전에 빠지고 그 시점이
    없는 revision은 남는다(관측 연구 패널의 `_Visibility.candidates`와 같다). 상한은 설정했을 때만
    query 문서에 들어가므로 상한 없는 읽기의 영수증 바이트는 그대로다. subject는 도메인의 주체 열(가격은
    `instrument_id`, 달력은 `calendar_id`, 재무·공시는 `issuer_id`, 거시는 `series_id`, FX는
    `base/quote`, 분류는 `subject_id`), 날짜는 도메인의 날짜 열(가격·달력은 `session_date`, 기업행동은
    `effective_date`, 재무는 `period_end`, 공시는 `filed_date`, `*_us` 열은 UTC 날짜)이다.
  - 투영은 `project_heads`와 같다. 행마다 `project_heads`가 head를 두거나(set) 지우거나(pop) 넘기는
    판정을 SQL로 계산하고, `(pin, record_id)`마다 generation 순서로 마지막 판정 하나를
    `QUALIFY row_number()`로 고른다. 자연키 열에 대한 필터와 pin 구간은 투영 전에 scan으로 내려가고,
    자연키가 아닌 날짜(기업행동 `effective_date` 등)는 revision이 그 날짜를 옮길 수 있으므로 투영한
    head에 적용한다.
  - 읽기 전에 pin이 marker와 같은지, chain의 모든 link가 기록된 hash로 다시 계산되는지, generation마다
    행 수가 marker와 같은지 확인한다. 행 값까지 다시 해시하려면 `rehash=True`(`verify_generation_bulk`
    deep)를 쓰고, 정기 확인은 `aas db verify`가 맡는다.
  - 결과를 fetch하기 전에 같은 투영의 행 수와 텍스트 길이를 SQL로 재어 호출자 할당의 비DuckDB 몫과
    비교하고, 넘으면 `ComputeResourceError`다. DuckDB 몫은 연결 한도로 제한한다.
  - 결과는 head 행(pin 번호, 도메인 값, 그 revision의 quality flag), 격자를 준 경우의 coverage, 읽기
    영수증이다. 영수증 `aas-head-read-v1`은 `binding`, `binding_hash`, `query`, `mode`,
    `time_rules`(`[pin, generation_id, available 규칙, known 규칙]` 목록), `applied_rules`,
    `withheld_rules`, `rehashed`(모든 delta를 다시 해시했는지; 거짓이면 구조 확인만 한 읽기),
    `heads`(행 수), `heads_hash`(`[pin, record_id, revision_id]` 목록의 정규 JSON SHA-256)를 가진 정규
    JSON이고, 그 SHA-256이 영수증 hash다.
  - `held=True`인 읽기는 cutoff가 아는데 head가 없는 record 가운데 grant가 시점 규칙을 막았거나
    (`ungranted_time_rule`) 공개 시점 근거가 없는(`unknown_<domain>_evidence`) record를 `HeadRead.held`
    (값 없는 coverage 칸: subject, 날짜, 이유, record ID)로 돌려주고, 영수증에 `held`(`[record_id, 이유]`
    목록)를 더한다. cutoff까지 알려지지 않은 record는 held가 아니다.
  - coverage 이유는 `ungranted_time_rule`, `flag_excluded`, `outside_cutover`, `tombstone`,
    `unknown_<domain>_evidence`, `<domain>_unavailable`, `reference_price`, `missing_<domain>`과
    head의 `value_state`다(가격은 `price`, 달력은 `session`). head가 없는 record의 이유는
    `market_inputs`의 strict reader와 같게, cutoff까지 알려진 마지막 revision이 정한다. 그 revision의
    공개 시점이 null이면 `unknown_<domain>_evidence`, cutoff 뒤면 `<domain>_unavailable`이다. head를
    돌려준 칸에도 grant가 막은 정정이나 제외한 revision이 있으면 `ungranted_time_rule`이나
    `flag_excluded`를 남기고, 칸은 present로 둔다. 한 칸에 record가 여럿이면(같은 날짜의 canonical과
    reference 가격) head를 준 record의 이유만 그 칸의 이유다. 달력 칸은 개장한 session만 present이고
    휴장 session은 `session_closed`다. 격자의 칸은 결과 행과 함께 fetch 전 할당 검사에 포함된다.
- 여러 cutoff에서 판단하는 소비자는 `read_revisions(connection, binding, query, strict, time_rules, budget)`
  (작업 공간 진입점 `market_inputs.load_pinned_revisions`)로 binding의 revision을 한 번 읽고
  `project_revisions`로 cutoff마다 투영한다. pin 확인, 시간 규칙 출처, 할당 검사는 `read_heads`와 같다.
  strict 읽기의 revision은 grant가 없는 규칙의 시점이 null로 돌아오고, 제외한 flag가 달린 revision은
  `excluded`로 표시된다. 투영은 `read_heads`의 SQL 판정을 generation 순서로 적용하므로 같은 cutoff의
  `read_heads`와 같은 head를 낸다. query에는 cutoff와 격자가 없고, 모든 필터가 자연키 열에 걸려야 한다
  (투영 뒤에만 거를 수 있는 날짜를 가진 기업행동은 `read_heads`로 읽는다). 영수증 `aas-head-revisions-v1`은
  `binding`, `binding_hash`, `query`, `mode`, `time_rules`, `applied_rules`, `withheld_rules`, `rehashed`,
  `revisions`(행 수), `revisions_hash`(`[pin, record_id, revision_id, excluded]` 목록의 정규 JSON SHA-256)다.
- 분할조정·총수익 가격은 reader가 unadjusted 가격과 cutoff 시점까지 알려진 `corporate_actions`로
  계산한다. 공급자 조정 가격은 reference로만 남는다. `storage/adjusted_prices.py`의
  `read_adjusted_prices(connection, prices, actions, query, basis, time_rules, budget)`가 가격 binding과
  기업행동 binding을 같은 query(같은 cutoff, 수집 cutoff, 지식 상한, subject, 날짜 구간)로 `read_heads`에서
  읽고 유도한다. 작업 공간 진입점 `load_adjusted_prices`는 두 읽기 모두 `market_inputs.load_pinned_heads`를
  쓴다. 그래서 cutoff까지 알려지지 않은 기업행동은 앞선 가격에 닿지 않는다. 기업행동 읽기는 `held=True`다.
  - 유도는 격자를 뺀 같은 query로 읽은 bar 전체(`series`)로 하고, query에 격자가 있으면 격자 날짜의 행만
    돌려주며 그 격자 읽기(`prices`)의 coverage를 함께 준다. 격자가 없으면 두 읽기는 같은 읽기다. 앞선
    읽기의 행은 호출자 예산의 `reserved_bytes`로 잡아 두고 다음 읽기와 유도를 그 나머지로 받는다.
  - 유도 방법 `aas-adjustment-v1`은 instrument마다 읽은 bar로 한다. 한 instrument는 cutover pin을 넘어 한
    시리즈다. ex-date(`effective_date`)가 그 instrument의 첫 bar보다 늦고 마지막 bar 이하인 행동만 쓰므로
    마지막 bar는 비조정 그대로이고 앞선 bar가 그 기준으로 표현된다.
  - 비율 행동(`split`, `stock_dividend`, `capital_adjustment`)의 비율 `r`은 앞선 가격에 `1/r`, 거래량에
    `r`을 곱한다. `total_return`에서는 배당 `D`가 앞선 가격에 `(C - D) / C`를 곱한다. `C`는 ex-date 직전
    세션의 비조정 close이며, 배당을 그 close에 재투자한다는 뜻이다. 그 세션은 ex-date 바로 앞에 읽은
    bar이고 `present`여야 한다. 격자가 그 bar와 ex-date 사이의 날짜를 가지면 그 날짜는 bar가 없는 세션이므로
    배당에 close가 없다. 격자가 없으면 세션을 빠뜨리지 않은 bar를 전제한다. `split_adjusted`는 배당을
    읽지 않는다.
  - 쓸 수 없는 행동(다른 행동 유형, `present`가 아닌 값, 재투자할 직전 세션 bar와 다른 통화의 배당, 직전
    세션 close가 없거나 그 이상인 배당)이 있으면 그보다 앞선 bar는 값 없이 `invalid`가 되고 이유 `unadjustable_action`을 단다.
    조용히 건너뛰지 않는다.
  - 기업행동 읽기가 held로 돌려준 행동(grant 없는 시점 규칙, 시점 근거 없음)도 두 basis 모두에서 쓸 수 없는
    행동이다. held record에는 값이 없어 유형도 읽지 않기 때문이다. 앞선 bar는 그 record의 이유
    (`ungranted_time_rule`이나 `unknown_corporate_actions_evidence`)와 `unadjustable_action`을 함께 단다.
  - 계수는 50자리 문맥의 정확한 십진수로 곱하고, 조정 값은 도메인의 `DECIMAL(38,12)` 정밀도인 소수
    12자리로 ROUND_HALF_EVEN한다. bar는 `basis='unadjusted'`여야 하고 한 instrument·세션에 bar가 둘이면
    거부한다(그래서 canonical과 unadjusted reference를 함께 읽으려면 query의 가격 역할로 하나를 고른다).
  - 결과 행은 bar의 record·revision ID와 도메인 값을 그대로 두고 `basis`만 유도 basis로 바꾸며, 누적
    가격 계수와 이유를 함께 싣는다. 영수증 `aas-adjusted-read-v1`은 basis, 방법, 가격 읽기(`prices`)와
    기업행동 읽기(`actions`)의 `aas-head-read-v1` 영수증과 그 hash, 유도에 쓴 읽기의 hash(`series_hash`),
    두 읽기가 막은 규칙의 합집합(`withheld_rules`), `[pin, record_id, revision_id, 계수, 이유]` 목록의 정규
    JSON SHA-256(`rows_hash`)을 담는다.

### 연구 실행의 canonical 가격 패널

선언한 미인증 연구 실행(`aas-research-run-v2`, `aas-research-composition-v1`)은 패널 원천으로 보존 관측
pin(`observations`) 대신 canonical 가격 binding(`prices`: `aas-head-binding-v1`의 pin과 cutover, flag 제외
목록)을 들 수 있다. 선언 최상위에는 둘 중 정확히 하나만 있다. `backtest_prepare._price_panels`가
`market_inputs.load_pinned_heads`로 그 binding을 한 번 읽어 open과 close 패널을 함께 만든다.

- query는 연구 모드이고 시간 규칙 grant를 쓰지 않는다. 지식 상한은 선언의 `knowledge_time`이고 subject는
  `instrument_map`의 instrument ID, 역할은 `canonical`, 날짜는 이력·기간 시작 중 이른 날부터 둘의 끝 중
  늦은 날까지다. 그래서 패널은 pin한 chain이 그 instrument에 가진 첫 세션부터 시작한다.
- 행은 `basis='unadjusted'`, `interval='1d'`의 canonical bar여야 하고 통화는 선언 통화이거나 아래 FX 변환
  grant가 허용한 통화다. `present`가 아닌
  bar와 공개 시점이 상한보다 늦은 head는 패널에 넣지 않으며 이전 값으로 대체하지 않는다. 같은
  instrument·세션의 bar가 둘이면 거부한다. 읽은 행이 없는 `instrument_map` 열쇠도 거부한다.
- 한 bar가 자기 시가와 종가를 내므로 두 패널은 basis·조정·세션이 같다. 세션 순서는 bar의 `bar_end_us`로
  정하며 이것은 지식 시점이 아니라 bar의 경제 시점이다. 일정은 관측 경로와 같은 날짜 기반 월말 판단이다.
- 읽기는 `read_heads`의 기본값대로 구조만 확인한다(`rehash=False`): pin·chain link·행 수는 확인하고 delta
  행 값은 다시 해시하지 않으며 영수증의 `rehashed`가 거짓으로 이를 기록한다. 관측 경로는 보존 관측 내용을
  다시 확인하므로 두 경로의 내용 확인 범위는 다르다. 행 값 확인은 `aas db verify`의 몫이다.
- 자산 유형은 상태 저장소의 instrument 분류에서 온다(`etf` → `ETF`). 관측 경로의 `OBSERVATION`과 다르다.
- 봉인 준비 문서는 `observations` 대신 `prices`에 `binding_hash`, 읽기 영수증 `head_read`
  (`aas-head-read-v1` 전체)와 그 SHA-256을 싣는다. `resolved_calendar.observed_calendar_ref`는 binding
  hash를 가리킨다. 실행은 `research-uncertified`이다. strict 실행 준비의 head binding은 다음 절이 다룬다.

선언은 두 grant를 선택적으로 가진다. 쓰지 않으면 열쇠 자체가 없으므로, grant가 없는 선언의 바이트와
digest는 grant가 생기기 전과 같다. 빈 배열은 거부한다.

- `macro`: 슬리브가 읽는 거시 series마다 `series_id`, `unit`, `binding`(`domain`이
  `macro_observations` 또는 `fx_rates`인 pin·cutover와 flag 제외 목록)을 `series_id` 순으로 싣는다. FX
  고시를 series로 쓰면 이름은 `BASE/QUOTE`, 단위는 호가 통화다. grant는 정확해야 한다: 슬리브들의 실행
  정의가 읽는 거시 series 집합과 선언한 집합이 같지 않으면 준비가 거부하므로, 슬리브가 series를 받지
  못한 채 replay에 가거나 쓰이지 않은 series가 기록되는 일이 없다. 각 binding은 store에서 다시 검증되고
  지식 상한까지 그 series를 그 단위로 가져야 한다(`admission`). 판단마다 그 판단의 cutoff(선언 지식
  시각을 넘지 않는다)를 지식 상한으로 다시 읽으므로, store가 vintage를 기록한 series는 판단 당시의
  vintage로 판단한다. 파생 series는 이 경로에 원천이 없어 그 슬리브는 여전히 거부한다. 표본 조합의 공격
  슬리브가 거시 신호를 가지면 그 신호가 master switch에 들어가 기록이 이름 붙인 canary가 아닌 조건으로
  방어 슬리브를 고르게 되므로 거부한다. 방어 슬리브의 거시 신호는 그 슬리브 안에서 쓰인다.
- `fx_conversions`: 선언 통화가 아닌 가격 통화마다 `currency`, `series_id`, `max_fixing_age_days`,
  `signal_basis`, `binding`(`fx_rates` pin·cutover와 flag 제외 목록), 선택 `prices`(그 통화 bar를 담은
  canonical 가격 binding)를 `currency` 순으로 싣는다. 변환은 [FX 변환 계약](#fx-변환-계약)을 따른다.
  `prices`가 있으면 선언의 `prices` binding과 같은 query로 그 binding도 읽어 패널에 더하므로, KRW
  chain과 USD chain을 한 run에서 읽는다. 그 binding의 bar는 모두 grant의 통화여야 하고 두 binding이 같은
  instrument·세션을 가지면 반복 세션으로 거부한다. 시가·종가 패널은 선언 지식 상한으로 패널 구간에 한 번
  읽은 고시로 환산하고, 환산할 고시가 없는 bar는 두 패널 모두에서 빠진다. 신호는 판단마다 그 cutoff(선언 지식
  상한을 넘지 않는다)로 고시를 다시 읽어(그 cutoff까지 고시되고 공개된 고시만) 그 판단이 읽는 종가를
  환산하므로, 판단 뒤에 고시되거나 공개되거나 정정된 고시는 그 판단의 신호에 닿지 않는다. 그 판단의 고시로
  환산되지 않는 종가는 신호 점이 아니다. `signal_basis=price_currency`이면 신호는 그 통화의 종가 그대로이고
  판단에서 고시를 읽지 않는다. 이 grant는 canonical 가격 패널에만 있고 관측 패널 선언에서는 거부한다.
- 봉인 준비 문서는 grant가 있을 때만 `macro`(`inputs`: series·단위·binding hash·binding 문서,
  `head_reads`: admission과 판단별 읽기 영수증)와 `fx_conversions`(`conversions`: 변환 문서·binding·
  패널 세션마다 적용한 고시 날짜와 환율·환산하지 못한 칸, `head_reads`: grant 가격 binding 읽기(`prices`),
  패널 고시 읽기(`panels`)와 판단별 고시 읽기(`decision`, 판단일·환산한 수·환산하지 못한 칸) 영수증)를 싣는다. 요청 저장소도 이 두 열쇠를 가진 선언을 같은 연구 스키마로 받는다.

### strict 실행 준비의 head binding

`aas-prepare-request-v1` 요청은 가격·세션·거시 입력을 기존 `generation` 참조 대신 `heads` 참조로 묶을 수
있다. `heads` 참조의 `pin`은 `aas-head-binding-v1` 문서에서 `schema`를 뺀 것(`domain`, cutover 구간을 가진
순서 있는 `pins`, `granted_rules`, `excluded_flags`)이고, `ref_id`와 `hash`는 그 문서의 binding hash,
`ref_version`은 `aas-head-binding-v1`이다. 요청 admission은 문서가 그 정규 표기(정렬·유일한 grant와 제외,
순서대로 빈틈 없는 구간, 첫 시작과 마지막 끝만 열림)인지와 hash를 다시 계산해 확인하므로, 요청의 hash와
`HeadBinding.binding_hash`는 같은 정체성이다. `backtest_prepare.prepare_backtest`가 그 binding을
`market_inputs.load_pinned_heads`·`load_pinned_revisions`와 `adjusted_prices.load_adjusted_prices`로
읽는다. `generation` 참조의 native transform 경로는 그대로다.

| 역할 | 받는 참조 | `heads`의 도메인 |
| --- | --- | --- |
| `signal_prices`, `execution_prices` | `generation`, `heads` | `prices` |
| `sessions` | `generation`, `heads` | `calendar_sessions` |
| `macro` | `generation`, `heads` | `macro_observations`, `fx_rates` |
| `actions` | `heads` | `corporate_actions` |
| `fx_conversion` | `heads` | `fx_rates` |

- 읽기는 판단마다 그 판단의 cutoff로 한다. `strict_pit` 요청은 cutoff가 있는 strict 읽기이고 binding의
  grant가 규칙 시점을 정하며, `observed_snapshot_research` 요청은 그 cutoff를 지식 상한으로 한 연구 읽기다.
  수집 cutoff는 요청의 `ingestion_cutoff_us`다. 신호 날짜 구간은 이력 시작부터 이력 끝과 판단일 중 이른 날까지다.
- `heads`로 묶은 canonical 신호 선택의 basis가 조정 basis이면 신호 가격은 공급자 조정 가격이 아니라 그
  binding의 비조정 bar와 `actions` binding의 기업행동으로 판단 cutoff에서 유도한다(`aas-adjustment-v1`).
  유도 읽기의 격자는 판단 cutoff에 알려진 달력의 개장 세션이므로, 배당 직전 개장 세션에 bar가 없으면 더 오래된
  종가로 재투자하지 않고 그 앞의 bar를 `invalid`로 둔다.
  그래서 cutoff까지 알려지지 않은 기업행동은 앞선 bar에 닿지 않고, grant가 막았거나 시점 근거가 없는
  기업행동 앞의 bar는 값 없이 `invalid`가 되어 신호에서 빠진다. 그런 유도 선택마다 `actions` binding이
  하나씩 있고, 유도 선택은 binding 순서대로 `actions` ordinal을 받는다(첫 유도 선택이 0). 그래서 시장마다
  다른 기업행동 원천을 묶고, 어느 유도 선택도 받지 않는 `actions` binding은 거부된다. `reference` 신호
  선택은 그 basis의 행을 저장된 그대로 읽으며 strict 읽기는 reference 가격을 고르지 않는다.
- 판단 cutoff의 기업행동 읽기에 bar가 있는 instrument의 기업행동이 하나도 없으면(보류된 것도 없으면) 그
  binding이 그 instrument를 덮는지 알 수 없으므로, 그 instrument는 기업행동 근거가 없는 것이다(`no_action_source`).
  그 instrument의 유도 가격은 비조정 bar와 같다. strict 준비는 그 `actions` binding의 `granted_rules`에
  `absent_actions_as_none@1`이 있을 때만 그렇게 읽고, 없으면 그 instrument를 밝혀 거부한다. 연구 준비는
  거부하지 않는다. 어느 쪽이든 그 읽기의 `head_reads` 항목이 `no_action_source`에 그 instrument를 싣는다.
- 신호 bar는 `_eligible_prices`가 받는 칸과 같은 조건으로만 쓴다: `present`이고, 판단 cutoff에 알려진
  달력의 개장 세션이며, 판단일 이전이고 cutoff까지 끝났으며, 이력 구간 안이고, identity와 universe pin이
  그 bar의 끝 시각에 그 instrument를 cutoff 기준으로 담는다. 선택 통화와 다른 통화의 bar, `1d`가 아닌 bar,
  한 instrument·세션의 두 번째 bar는 거부한다. instrument는 상태 저장소에 달력 venue로 등록돼 있어야 한다.
- 체결 가격은 기간 세션 전체를 요청의 지식 cutoff로 한 번 읽는다. 기간 밖이나 `present`가 아닌 bar는 쓰지 않는다.
- `heads`로 묶은 달력은 이력 시작부터 기간 끝까지 그 달력의 revision을 한 번 읽고(`aas-head-revisions-v1`),
  판단마다 그 cutoff로 투영한다. 행의 달력·venue·timezone version이 calendar 관례와 다르면 거부한다.
  native 가격 generation과 파생 가격 입력은 native 세션 generation과 함께 읽히므로 `heads` 달력과 함께
  묶을 수 없다. 일정은 판단 cutoff에 알려진 달력으로 정하므로, 그 cutoff에 다음 개장 session과 그 달의 남은
  날짜가 알려져 있어야 판단이 생긴다. [선언 달력](#선언-달력)의 `declared_session_end@1`은 선언 시각 이전
  날짜를 그 날짜가 끝난 시각부터 알리므로, 선언 시각보다 앞선 기간의 판단은 다음 session을 알지 못해
  일정이 만들어지지 않고 준비는 미해결 월을 보고하며 거부한다. 그런 기간의 strict 일정에 쓸 달력 사전 지식
  규칙(버전 있는 규칙과 그 grant, run의 기록)은 owner가 정하며 아직 없다(DV-336).
- 거시 선택의 subject는 `series_id`이고 행의 단위는 선택의 `unit`과 같아야 한다. FX 선택의 `series_id`는
  `BASE/QUOTE`이고, 경제 날짜는 고시 시각의 UTC 날짜, 값은 `rate`, 선택의 `unit`은 호가 통화다. 준비는
  지식 cutoff로 한 번 읽어 binding이 그 series의 head를 가지는지 확인하고(`admission`), 판단마다 다시 읽는다.
- 계좌 통화가 아닌 가격 선택은 요청의 `fx_conversions` grant로만 받는다. 항목은 `binding`(`fx_conversion`
  역할의 binding 하나), `currency`, `series_id`, `max_fixing_age_days`, `signal_basis`이고 binding 순으로
  정렬된다. grant는 정확해야 한다: 계좌 통화가 아닌 가격 통화마다 정확히 하나, `fx_conversion` binding마다
  정확히 하나이고, 어느 가격 선택도 쓰지 않는 통화의 grant는 거부한다. 환산이 없는 요청은 열쇠가 없으므로
  그 바이트와 `request_hash`는 이 계약 전과 같다. 신호는 판단마다 그 cutoff로 고시를 읽어(그 cutoff까지
  고시되고 공개된 고시만) 변환하고, 체결 가격은 기간 전체를 요청의 지식 cutoff로 한 번 읽어 변환한다.
  환산할 고시가 없는 신호 bar는 신호 점이 아니고, 체결 가격 칸은 bar가 없는 칸과 같다(그 칸에 매수가 필요하면
  봉투 내보내기가 거부한다). 고시 읽기마다 `head_reads`에 역할 `fx_conversion`, 그 영수증, 변환 문서,
  환산한 수와 환산하지 못한 칸이 남는다. 봉인 준비 문서의 `fx_conversions`는 변환마다 binding과 binding
  hash, 변환 문서, 체결 세션마다 적용한 고시(세션, 고시 날짜, 환율)와 환산하지 못한 칸을 싣고, run은 그
  문서를 그대로 봉인한다. bundle의 `fx_conversion` binding도 다른 `heads` 참조처럼 `raw/`의 문서로 검증된다.
- 봉인 준비 문서(`aas-prepared-backtest-v1`)의 `head_reads`는 준비가 한 읽기마다 역할·ordinal·목적
  (`calendar`, `admission`, `decision`, `outcomes`)·판단일과 reader가 돌려준 영수증(`aas-head-read-v1`,
  `aas-adjusted-read-v1`, `aas-head-revisions-v1`) 전체와 그 SHA-256을 싣는다. 유도 신호 읽기의 항목은 그
  읽기가 쓴 `actions` binding(`role`, `ordinal`)과 `no_action_source`(정렬한 instrument 목록)도 싣는다.
  `aas run execute`는 이 문서를 run에 그대로 봉인한다. run의 입력 bundle은 `heads` 참조를 binding hash로만
  가리키므로, 실행은 그 binding 문서를 그 hash 주소로 `raw/`에 남기고(`aas data binding-import`도 정규 표기의
  `aas-head-binding-v1` 문서를 pin 확인 뒤 같은 자리에 남긴다), bundle 검증(`aas db verify` 포함)은 그 문서를
  다시 읽어 hash·정규 표기·pin·catalog·시간 규칙 출처를 확인한다. 연구 run의 bundle은 membership만 묶으므로
  `heads` 참조가 없다.
- `heads` 입력은 envelope의 `source_pins`에 원천을 싣지 않는다. 그 출처는 읽기 영수증의 pin과 chain이다.
  proxy 규칙은 native 체결 원천에 묶이므로 `generation`으로 묶은 체결 가격에만 붙는다. proxy 값은 통화를
  싣지 않으므로 donor·target 선택은 계좌 통화여야 하고, 다른 통화로 선택한 donor·target은 이름으로 거부한다.

### FX 변환 계약

`engine.fx_conversion`이 소유하는 순수 규칙이다. 변환은 grant이고 추론하지 않는다: 호출자가 가격 통화,
계좌 통화, 둘을 잇는 고시 series, 고시의 최대 나이(일), 신호가 읽을 통화를 이름 붙이고, 이 모듈은 명시적으로
받은 고시에 그 grant를 적용한다. store를 읽거나 series를 고르지 않는다.

- 규칙 `fx_latest_fixing_on_or_before@1`: 날짜 `d`의 가격 통화 값은 날짜가 `d` 이하이고 `d`보다
  `max_fixing_age_days`일을 넘게 앞서지 않은 가장 늦은 `present` 고시로 계좌 통화가 된다. 고시 날짜는
  `fixing_at_us`의 UTC 날짜다. 그런 고시가 없으면 그 값은 계좌 통화 값이 없다. 더 늦은 고시로 채우거나
  선언한 나이를 넘겨 이월하지 않는다.
- series가 `PRICE/ACCOUNT`이면 환율(가격 통화 1단위의 계좌 통화 값)을 곱하고, `ACCOUNT/PRICE`이면 나눈다.
  다른 series는 그 변환이 아니다. 통화는 세 글자 대문자이고 가격 통화와 계좌 통화는 다르다. 한 날짜에
  고시는 하나이고 환율은 유한한 양수다.
- `signal_basis`는 `account_currency`(신호가 환산한 가격을 읽음) 또는 `price_currency`(신호가 그 통화의
  가격을 읽음)다. 둘 다 결과를 바꾸는 연구 선택이므로 기본값이 없다. 체결·평가 가격은 늘 계좌 통화다.
- 변환 문서(`aas-fx-conversion-v1`)는 규칙, 두 통화, series, 방향(`multiply`·`divide`), 최대 나이,
  `signal_basis`이고 run이 이 문서를 기록한다.

## core schema 버전

state와 market 저장소는 core schema 버전을 가지며 현재 버전은 v3이다. `aas init`은 새 저장소에 v1부터
v3까지를 한 트랜잭션으로 적용하므로 새 설치본과 migration한 설치본은 같은 객체와 같은 영수증을 가진다. `aas db migrate --to N
--backup-output DIR`이 설치본을 한 버전씩 `N`까지 올린다. `storage/migration.py`가 이 순서를 소유한다.
`v - 1`에서 `v`로 가는 단계마다 다음을 한다.

1. 설치 잠금을 잡는다. 실행 중인 run이나 다른 PREPARED 작업이 있으면 거부한다. 예외는 손대지 않은
   승격 intent 하나뿐이다(아래).
2. 백업을 만들고 검증한다. 그 경로가 이미 있으면 진행하지 않는다. 백업이 필요한 단계가 하나면 `DIR`
   자체가 백업이고, 여럿이면 새 `DIR` 아래 단계의 operation ID(`DIR/core-schema-migrate-v2` 등)마다 하나씩 만든다.
3. state에 그 단계의 intent(`core-schema-migrate-v<v>`)를 기록한다. payload hash는 그 단계 백업
   manifest(`backup.json`)의 SHA-256이다. 요청은 그 단계의 두 버전과 그때까지의 checksum만 담으므로
   나중 버전이 생겨도 완료된 intent가 계속 맞는다. v2 단계의 정체성은 출시된 그대로다.
4. market DDL을 DuckDB 한 트랜잭션으로, state DDL을 SQLite 한 트랜잭션으로 적용한다. 각 트랜잭션은
   `schema_migrations`에 새 버전 행을 더하고 `store_info.schema_version`을 바꾼다. 기존 행은 그대로 남아
   `(1, v1), (2, v2), (3, v3)`처럼 이어진다.
5. 설치 영수증(`installation.json`)의 저장소 버전을 바꾼다.
6. 올라간 market의 core catalog가 같은 버전의 빈 저장소와 같은지 확인하고, commit된 모든 generation의
   행을 기록된 digest로 다시 해시하고 전역 중복 감사(`--deep`의 감사)를 통과시킨 뒤 intent를 완료한다.
   이 확인은 백업의 `--deep` 여부와 무관하게 언제나 하고, 실패하면 intent는 PREPARED로 남는다.

어느 단계의 intent가 PREPARED인 설치본은 migration-incomplete다. 앱은 그 설치본을 읽기·쓰기 어느
쪽으로도 열지 않고, 같은 명령을 다시 실행하면 그 단계의 남은 부분부터 재개한다. 재개는 그 단계의
백업을 다시 만들지 않으며, 그 단계보다 앞선 `--to`는 거부한다. intent가 없는 단계는 언제나 자기 백업부터
만든다. 그래서 한 단계를 완료하고 다음 단계의 intent 전에 멈춘 설치본은 앞 단계 버전의 완전한
설치본이고, 새 `DIR`로 다시 실행해야 한다. 재개한 단계의 market COMMIT이 이미 끝났어도 그 단계를 마친
뒤 같은 호출의 다음 단계 백업은 같은 store·설치 정체성의 그 단계 버전 market만 받는다. 백업 목적지는
어느 단계도 쓰기 전에 백업이 받는 조건 그대로(새 디렉터리, `raw`·`runs`·`secrets` 밖, Git checkout 밖) 확인한다. 목표 이상인 설치본에는 아무것도 하지 않는다. `aas db
quarantine`은 이 intent를 끝내지 않는다. `--plan`은 저장소를 읽기 전용으로 열어 현재 버전, 인식한
checksum, 다음 단계의 남은 부분(`steps`), 목표까지의 단계별 목록(`migrations`), 백업 필요 여부,
가지고 갈 승격(`carried_operations`)과 막는 작업(`blocking_operations`)을 보고하고 아무것도 쓰지 않는다.

손대지 않은 승격 intent는 PREPARED인 채로 단계를 지난다. 그 조건은 `aas db recover`가 그 intent를
게시하는 조건이고 같은 코드로 계산한다. kind가 `promotion`이고, 그 operation이나 generation의 marker,
행, flag, catalog 기록(`dataset_versions`, 그 generation의 `promotion_report`·`partition_row_count` 품질
검사)이 없어야 한다. 보존된 증거는 intent의 것이어야 한다. operation·generation ID와 parent가 요청에서
나오고, manifest는 intent가 가리키는 hash의 정규 bytes로서 같은 요청과 generation을 적으며, 보존 요청은
보존 명세로 다시 만든 그대로다. 마지막으로 보존 명세를 다시 계획해 parent가 아직 dataset의 head이고
계획한 manifest의 SHA-256이 intent의 payload hash와 같아야 한다. 이 비교는 recover가 게시하기 전에 하는
비교와 같은 함수이므로 manifest가 적은 어떤 값도 따로 믿지 않는다. 고친 manifest를 다시 hash해 intent에
걸어도, head가 옮겨졌거나 원천 table이 바뀌었어도 가지고 가지 않는다. 재계획은 앞선 검증이나 계획이
연결에 남긴 몫이 아니라 설치본의 자원 한도에서 lease 몫을 새로 적용해 돌므로, `--plan`, migration의
확인, 백업의 재확인은 같은 lease에서 같게 판정한다. 확인이 끝나면 계획의 임시 table을 지우고 market
연결을 설치본의 자원 한도로 되돌린다. 증거를 읽거나 계획하다 난 오류는 가지고 가지
않는 이유가 되지만, 계획이 compute lease의 몫 안에서 돌 수 없으면 `ComputeResourceError`로 거부하고
조용히 가지고 가지 않는다. 그 밖의 PREPARED 작업, marker가 commit된 승격, 실행 중인 run은 계속 막는다.
이 예외는 단계의 백업에만 넘겨지고 백업이 같은 조건으로 다시 확인한다. 일반 `aas db backup`과 다른
명령의 백업은 여전히 모든 PREPARED 작업을 거부한다. 그 백업은 intent와 `raw/` 증거를 그대로 담고,
`logical.pending_operations`는 실제 수를 세며, manifest의 `carried_operations`가 그 operation을 적는다.
이것은 migration의 rollback snapshot이지 복구를 마친 설치본의 백업이 아니다. migration은 intent를 끝내거나
바꾸지 않는다. 그 뒤 `aas db recover`가 같은 재계획으로 게시한다. 재계획 뒤 게시 전에 head가 옮겨지면
게시의 parent 확인이 거부하고, recover는 그 intent를 pending으로 남긴 채 나머지 작업을 이어 간다. `--plan`의 `carried_operations`는
승격마다 operation·generation ID와 manifest hash를 적고 `proof`로 다시 계획해 증명했음을 밝힌다. 그래서
`--plan`도 compute lease 안에서 실행한다.

`schema_migrations`의 이력은 1부터 빈틈없이 이어지고 각 행의 checksum은 그 버전 DDL의 SHA-256과
같아야 한다. 알 수 없는 버전, 빈틈, 다른 checksum은 거부한다. v1 checksum은 market
`ab2383d7cb1181e7b98e7dc054042f82024a6aafc0c0fabb27ee5f94dbe0db7c`, state
`da574cef54b69961c341e3e5e92ee16334a5049911ee6b417db643f44bd881dc`로 고정돼 있다. migration하지 않은
v1 설치본도 정상으로 열리며 v1 기능을 그대로 쓴다. v2 테이블이나 close 전용 가격을 쓰는 게시는 v1
설치본에서 `aas db migrate --to 2`를 안내하며 거부된다. v1 행은 그대로 옮겨져 읽히고 기록된 delta·chain
hash로 검증된다. v2 설치본도 정상으로 열리고 v2 기능을 그대로 쓰지만, 아래 key 색인 때문에 큰 테이블의
게시는 DuckDB 몫이 그 테이블의 행 수만큼 커진다([대량 게시](#대량-게시와-reader)). 그래서 대량 승격과
유지보수 전에 `aas db migrate --to 3`으로 올린다. run 추가 스키마 버전은 이 migration과 독립이다.

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

market v3:

- 열한 도메인 테이블의 `PRIMARY KEY(generation_id, record_id, revision_id)`와 `UNIQUE(record_id,
  revision_id)`, `quality_flags`의 여섯 열 PRIMARY KEY가 없다. 그 밖의 규칙(열 순서와 타입, NOT NULL,
  CHECK, `prices.fields` 기본값, 모든 `FOREIGN KEY(generation_id)`)과 `market_generations`·결과 테이블의
  제약은 v2 그대로다. 그 key가 지키던 규칙은 아래 writer 검사와 감사가 지킨다.
- DuckDB는 기존 테이블의 제약을 지우지 못하므로 v3 단계는 열두 테이블마다 옛 테이블을 옆 이름으로 옮기고,
  v3 모양으로 새로 만들고, 명시한 열 목록으로 행을 복사한다(`CREATE TABLE AS`는 모든 제약을 잃는다). 그다음
  복사한 행을 위치마다 원래 행과 모든 열의 값으로 비교하고(text는 bytes, DOUBLE은 왕복 text로), 다른 행이나
  짝 없는 행이 하나라도 있으면 단계의 트랜잭션 전체를 거부한다. 위치 비교라서 `preserve_insertion_order`가
  꺼진 연결은 어느 테이블도 건드리기 전에 거부한다. 비교가 끝나야 옛 테이블을 지운다. 열두 재구축과
  `schema_migrations`·`store_info` 갱신은 DuckDB 한 트랜잭션이고 compute lease의 DuckDB 몫에서 돈다. 행 수
  대조만으로는 값 보존이 증명되지 않으므로, 단계는 위 6번의 전체 rehash와 중복 감사를 통과해야 끝난다.
- 두 writer(`market.publish_generation`, `bulk_generation.publish_generation_bulk`)는 게시 트랜잭션의 첫
  쓰기로 store를 claim한다. 같은 store의 두 게시 중 하나만 commit되고 다른 하나는 claim이나 COMMIT에서
  거부된다. 트랜잭션 안에서 삽입 전에 그 generation의 행이 없어야 하고, 삽입 뒤 계획한 행 수, record마다
  revision 하나, 같은 도메인의 다른 어떤 generation도 같은 `(record_id, revision_id)`를 갖지 않음을 확인한다.
  마지막 검사는 chain이나 dataset이 아니라 도메인 전체를 읽는다. 게시의 quality flag는 writer가 직접 넣고,
  반복된 flag key, 저장되지 않은 revision의 flag, 검토한 수·digest와 다른 flag를 COMMIT 전에 거부한다.
- 저장된 상태는 `aas db verify`와 백업·복원·compact의 검증이 감사한다. 기본 감사는 core catalog가 같은
  버전의 빈 저장소와 같은지(열 순서·타입·nullability·기본값·제약, add-on과 임시 table 제외), 도메인마다
  generation별 행 수와 marker의 domain·행 수가 양쪽으로 맞는지, flag가 자기 generation에 저장된 revision을
  가리키는지 본다. `--deep`은 도메인 전역 `(record_id, revision_id)` 중복과 flag key 중복을 더 본다. 그 검사는
  실제로 잰 행 수와 key bytes로 pass를 나누고, 한 pass가 할당에 들어가지 않으면 길이를 붙인 key 전체의
  SHA-256 접두어로 더 나누며, 비교는 원래 key 전체로 한다. 한 행도 할당에 들어가지 않으면
  `ComputeResourceError`다. 감사는 통과하거나 거부하므로 검증 보고 형식은 그대로다.
- 해시 형식과 요청·generation·delta·chain·manifest hash는 core 버전과 무관하다. DDL, 색인, 버전은 hash
  입력이 아니므로 v2에서 게시한 generation은 v3에서 같은 hash로 검증되고, 같은 명세를 v2와 v3에서 승격하면
  같은 generation이 나온다.

state v3는 테이블을 바꾸지 않는다. 두 저장소가 언제나 같은 core 버전을 갖도록 버전과 checksum만 기록한다.

## 원천 은퇴와 동치 증명

`aas db source-retire --spec FILE --sha256 H [--backup DIR] --plan|--apply`는 다른 원천으로 대체된 원천
자료실 테이블을 지운다. `storage/source_retirement.py`가 이 경로를 소유한다. 요청은
`aas-source-retirement-v1` 문서 하나이고 8 MiB 이하의 정확한 bytes를 SHA-256으로 pin한다.

```json
{"schema_version": "aas-source-retirement-v1",
 "groups": [{"reason": "Norgate normalized copy of the raw export",
             "uncompared": ["path"],
             "retire": {"sources": ["..."], "table": "observations", "columns": ["assetid", "date", "close"]},
             "equivalent": {"sources": ["..."], "table": "observations", "columns": ["assetid", "date", "close"]}}]}
```

- group 하나는 은퇴할 원천 목록과 동치 원천 목록, 양쪽의 테이블 이름 하나와 위치별로 대응하는 비교 열을
  가진다. 원천을 나눈 경계가 양쪽에서 달라도 되므로 한 group은 여러 원천을 여러 원천과 비교한다.
- `uncompared`는 은퇴할 테이블의 열 가운데 비교하지 않는 열을 모두 나열한다(없으면 빈 목록). 은퇴할
  테이블마다 commit manifest의 열이 정확히 비교 열과 `uncompared`의 합이어야 하고, 아니면 group은
  `uncompared_columns_differ`로 거부된다. 비교 열을 `uncompared`에 넣은 문서는 읽기 전에 거부한다. 그래서
  비교에서 빠지는 열은 언제나 hash된 문서에 이름으로 적힌 소유자의 허가이고, 조용히 빠지지 않는다.
- 같은 원천 테이블은 한 group에서만 은퇴하고, 은퇴할 원천은 어느 group의 동치 원천도 될 수 없다. 은퇴할
  원천의 모든 테이블이 어떤 group의 비교 대상이어야 한다. 알 수 없는 필드, 빈 목록, 중복, 열 수 불일치는
  데이터를 읽기 전에 거부한다.
- group은 통째로 은퇴하거나 통째로 거부된다. `--apply`는 다음이 모두 성립하는 group만 처리하고, 나머지
  group은 이유와 함께 보고한다.

1. **참조 없음**: committed generation을 증명하는 보존 문서(승격 명세, 연구 변환, import 문서)의 JSON,
   market 도메인 테이블의 `source_snapshot_id`, state의 `dataset_sources`·`identity_assertions`·
   `universe_members`, 입력 binding과 feature 입력의 `ref_id`, 전략 레지스트리의 `strategy_registrations` 중
   어느 것도 group의 원천을 가리키지 않는다. 원천 자신의
   source-link 행(`sl:` snapshot과 `source_files`)은 참조로 세지 않는다. 그 행은 은퇴 뒤에도 계보로 남는다.
2. **동치 증명**: 양쪽 비교 열의 정규 행 multiset에 대한 `aas-rowset-v1` digest가 같고, 은퇴할 테이블의
   나머지 열이 정확히 `uncompared`다. digest는 key 없는 multiset이므로 비교 열만 증명한다. 값 열만 비교하면
   같은 값 묶음이 다른 종목이나 날짜에 붙어 있어도 같은 digest가 나온다. 행의 동치를 증명하려면 자연 key
   열(종목·날짜에 대응하는 열)도 비교 열에 넣는다. 열 이름은 은퇴
   쪽의 이름을 쓰고, 각 셀은 정확한 형태로 부호화한다. 정수는 `int`, binary64(그리고 넓힌 binary32)는
   `float`, 날짜는 `date`, timestamp는 UTC microsecond `utc_us`, 소수 12자리 이하의 decimal은 scale 12의
   `decimal`, 문자열은 `text`, 불리언은 `bool`이다. 정확한 형태가 없는 값(int64를 넘는 정수, NaN, 무한대)이나
   다른 타입(binary, list 등)은 group을 거부한다. 양쪽 행 수가 다르거나 열 타입이 다르면 계산 없이 거부한다.
   DuckDB가 행을 부호화해 정렬하고 Python은 할당 안의 배치로 digest만 계산한다.
3. **다른 장치 백업**: `--backup`의 백업은 모든 파일을 다시 해시해 검증되고, 같은 설치본의 것이며,
   설치본의 루트·state·market·`raw/`와 다른 장치에 있다. 백업의 market 저장소에는 원천마다 같은 commit
   manifest와 그 manifest가 나열한 모든 테이블이 같은 행 수로 있고, 그 테이블의 행은 백업 안에서 commit이
   기록한 digest로 다시 해시된다. 백업이 `--deep` 없이 만들어졌어도 은퇴할 원천의 행은 이렇게 확인되며,
   다르면 `backup_content_mismatch`로 거부한다. 보고의 `backup_deep`은 백업 `backup.json`의 `deep`이다.
   `backup_id`는 백업 `backup.json`의 SHA-256이다.

이미 은퇴한 원천의 동치 원천이었던 원천은 은퇴하지 않는다. 은퇴의 증명이 가리키는 원천이 사라지지
않게 하기 위해서다. 같은 group을 다시 요청하면 `already_retired`로 보고하고 아무것도 쓰지 않는다.

`--plan`은 설치본을 읽기 전용으로 열고 group별 상태(`retire`·`refused`·`already_retired`), 이유, 증명 범위
(`compared`: 모든 열이면 `all_columns`, `uncompared`가 있으면 `partial_columns`)와 `uncompared_columns`, 양쪽 행
수와 digest, 원천별 실제 비교 밖 열(`uncompared_columns`)·참조 위치와 이유, 부분 열 group 수
(`partial_column_groups`), 후보·은퇴 가능 원천과 행의 합계를 보고한다. v1 state에서는 계획만
하고 `apply_needs_v2`를 보고한다. `--apply`는 다음 순서로 진행한다.

1. 실행 중인 run이나 다른 종류의 PREPARED 작업이 있으면 거부한다. 남은 `source-retire` intent가 있으면
   먼저 끝낸다.
2. 계획을 다시 세우고 은퇴할 원천마다 기록을 만든다. 기록은 `source_id`, `digest`(commit manifest 텍스트의
   SHA-256. manifest는 테이블별 행 수와 내용 digest를 담고 marker에 남는다), `rows`, group의 `reason`,
   `equivalent_to_source_id`(동치 쪽 첫 원천), `equivalence_spec`(양쪽 원천·테이블·비교 열과 `uncompared`를
   담은 group의 정규 JSON), `equivalence_digest`, `backup_id`다.
3. 기록 문서(`aas-source-retirement-records-v1`, 은퇴 시각 포함)를 `raw/`에 보존하고 그 해시를 payload로
   `source-retire` intent를 기록한다. intent ID는 `source-retire:` + request hash이고, request hash는
   `aas-source-retirement-request-v1`(`spec_sha256`, `backup_id`, 시각을 뺀 기록 목록)의 정규 JSON SHA-256이다.
4. 남은 테이블을 DuckDB 한 트랜잭션으로 지운다. 그 전에 테이블이 남은 원천의 참조를 다시 확인한다.
5. `source_retirements`에 원천마다 불변 행 하나를 쓰고 intent를 완료한다.

중간에 멈춘 intent는 같은 명령을 다시 실행하거나 `aas db recover`로 끝낸다. 끝내기는 보존한 기록에서
시작하고, commit manifest가 기록의 digest와 같은지와 남은 테이블의 참조만 다시 확인한다. 테이블을 하나라도
지운 intent는 `aas db quarantine`이 끝내지 않는다. 아무 테이블도 지우지 않은 intent는 quarantine할 수 있다.

은퇴는 원천 자료실 테이블만 지운다. commit marker, `sl:` 연결, `raw/`의 원본 bytes와 보관 archive는 남는다.
은퇴한 원천은 원천 목록과 reader에서 빠지고, 읽으려 하면 동치 원천과 백업 ID를 알려 주며 거부한다. 은퇴한
원천의 비교 열은 동치 원천과 백업에서 다시 얻는다. `uncompared` 열은 동치 원천에 없을 수 있으므로 백업이나
`raw/`의 원본 bytes에서만 다시 얻는다.

은퇴한 원천은 commit된 원천이다. 같은 원천 ID와 같은 요청의 재적재는 앞선 적재의 재사용
(`reused`, `retired`)이고 테이블을 다시 만들지 않는다. 내용이 다른 재적재는 거부한다. 원천이 이미 commit됐는지
판단하는 importer(`aas collect qveris import`, `aas identity kr-import --plan`, `aas import sec-companies --plan`)는
`source_library.committed_source_ids`(목록의 원천과 은퇴한 원천)를 쓴다. `aas import legacy --verify`는 은퇴한
unit을 다시 해시하지 않고, 은퇴 기록의 digest가 marker의 commit manifest와 같고 링크가 그대로면 `retired`로
일치시킨다. 수집기가 계획에 읽는 receipt·batch 원천은 목록의 원천이므로, 그런 원천을 은퇴시키면 그 수집기는
그 질의를 모르는 것으로 계획한다. `aas db verify`는 은퇴한
commit마다 기록의 digest·행 수가 manifest와 같은지, 테이블이 없는지, 은퇴 intent가 완료됐는지, 동치 원천의
commit이 있는지 확인하고, 은퇴가 하나라도 있으면 보고에 `source_library.retired`(원천 수와 행 수)를 더한다.

물리 공간 회수는 `aas db compact --to NEW_ROOT`(`storage/compaction.py`)가 한다. 설치본을 유지보수 잠금 아래
검증한 뒤 새 루트에 저장소를 새로 쓴다. SQLite는 backup API와 `VACUUM`으로 옮긴다. DuckDB는 새 파일에
`COPY FROM DATABASE … (SCHEMA)`로 catalog를 만든 뒤 테이블마다 행을 외래 key의 부모 테이블부터 옮기고,
자기 자신을 가리키는 테이블(`market_generations.parent_id`)은 chain 단계마다 한 번씩 옮긴다. `COPY FROM
DATABASE`의 자료 복사는 외래 key 순서를 지키지 않기 때문이다. 그래서 지운 테이블이 남긴 빈 블록을 버린다. `raw/`, `runs/`, `secrets/`는 파일 단위로
복사하고, `runtime.json`은 경로만 새 루트 기준 기본값으로 바꾼다. 원래 설치본은 기본 검증(`--deep`이면 deep)으로,
새로 쓴 루트는 항상 deep으로 검증한다. 새 루트를 열어 같은 논리 검증 결과가 나와야
설치 영수증이 `ready`가 된다. 실패하면 새 루트는 `restore-incomplete`로 남고 원래 설치본은 그대로다. 원래
설치본은 바뀌거나 지워지지 않으며, `AAS_HOME`(또는 `--home`)을 새 루트로 바꾸는 일은 운영자가 한다. 새
루트는 존재하지 않는 경로여야 하고 원래 설치본의 경로와 겹치지 않는다.

## 전략 레지스트리

전략 원본 레코드는 원천 자료실에 `source_only`로 보존된 작성자의 요청이다. `aas strategy promote
--source ID --sha256 H --plan|--apply`가 그 레코드를 비공개 전략 저장소의 불변 정의 문서로 등록한다.
`storage/strategy_registry.py`가 이 경로를 소유한다. 정의는 엔진 bundle이 아니다. `strategy_versions`에
들어가지 않고, 실행·연구·백테스트 자격을 주지 않으며, 원본을 bundle로 추정 변환하지 않는다. 실행에
쓰려면 검증된 bundle을 `aas strategy import`로 같은 전략 ID에 따로 등록한다.

**원천 형식 `snowball-request@1`.** 원천은 `strategies` 저장소의 원천 자료실 commit이고 정확히 이 열을
가진 세 테이블을 담는다. `strategy`(`id`, `title`, `source_type`, `country`, `is_personal`, `report_path`,
`request_json`, `normalized_json`, `exact_hash`, `rule_hash`, `family_hash`, `start_date`, `finish_date`,
`source_data_basis`, `quality_status`), `asset_dependency`(`strategy_id`, `role`, `ordinal`,
`raw_token_json`, `token_kind`), `macro_dependency`(`strategy_id`, `ordinal`, `raw_json`). 세 테이블은
원천 pin(원천 ID, 원천 SHA-256, 테이블 digest)으로 검증한 뒤 읽는다. 세 테이블은 함께 메모리에 올라가므로
계산 예산(설정이 없으면 512 MiB 직렬 기본값)에서 차례로 누적해 승인한다. 다른 모양의 원천, 다른 SHA-256,
같은 전략 ID가 두 번 나오는 원천은 거부한다.

**정의 문서 `aas-strategy-definition-v1`.** 레코드 하나가 문서 하나다. 문서는 `strategy_id`,
`source_format`, `requirement_map`, `title`, `country`, `source_type`, `is_personal`, `report_path`,
`start_date`, `finish_date`, `data_basis`, `quality_status`, 원천이 계산한 `source_hashes`(`exact`, `rule`,
`family`), 해독한 `request`를 원래 값 그대로 담는다. 원천이 스스로 유도한 `normalized_json`은 담지
않는다. 저장 bytes는 정규 JSON이고 버전은 `def-` + 문서 SHA-256의 앞 16자다. 그래서 내용이 같으면 같은
버전이고, 내용이 바뀐 레코드는 새 버전이 되며 이전 버전은 그대로 남는다. 정의·요구·출처·등록 행은
UPDATE와 DELETE를 거부한다. 같은 정의를 다른 원천에서 다시 만나면 버전은 재사용되고 출처 행만 더해진다.
`strategies` 행은 전략 ID와 앞뒤 공백을 뺀 제목으로 처음 한 번 만들어진다.

**요구 사상표 `aas-strategy-requirement-map-v1`.** 정의의 요구 행은 저장된 문서에서만 유도되고 검증할
때마다 다시 유도해 대조한다.

- 가격 역할은 요청 경로 `/offensive`, `/defensive_rule/defensive`, `/defensive_rule/unallocated`,
  `/canary/etf_list`, `/defensive_rule/abs_compare`, `/asset_selection_rule/abs_compare`의 원소마다
  하나, `/benchmark` 하나다. 배열 경로의 ordinal은 원소 위치이고 값이 없는 경로는 행을 만들지 않는다.
- 거시 역할은 `/crash_protection/crash_protector`의 원소마다 하나이고 토큰은 그 `func`다.
- `CASH`는 `cash` 도메인의 `not_applicable`이다. 여섯 자리 숫자 코드는 `prices.kr.eodhd`, 미국 상장
  ticker 모양은 `prices.us.norgate`에 `mapped`된다. 벤치마크의 `6040`·`SP500`·`NASDAQ`·`KOSPI`는 상장
  종목이 아닌 합성 지표라서 `unmapped`(`composite_benchmark`)다. 어느 모양도 아닌 토큰은
  `unmapped`(`unrecognized_token`)다.
- 거시 `T10Y2Y`·`T10Y3M`은 같은 이름의 ALFRED series로 `macro.us.alfred`에 `mapped`된다. 다른 함수는
  등록된 파생 정의가 없으므로 `unmapped`(`derived_series`)다.
- `weight_calculation_rule.constant`가 위 경로 어디에도 없는 자산을 가리키면 정의를 거부한다. 가격
  입력이 요구 행 없이 남지 않게 하기 위해서다.
- 요청의 통화는 `/exchange`(`USD` 또는 `KRW`)다. `mapped` 가격 행 중 하나라도 다른 통화의 시장(`us`는
  `USD`, `kr`는 `KRW`)이면 `fx` 도메인 행 하나(역할 `/exchange`, ordinal 0, 토큰은 요청 통화)가
  `fx.usdkrw.norgate`의 `USD/KRW` series로 `mapped`된다. 같은 쌍의 `fx.usdkrw.fred`는 소비자가 대신 pin할
  수 있는 다른 공급자다. `mapped` 가격 행이 있는데 `/exchange`가 없으면 `unmapped`(`missing_currency`,
  빈 토큰), 다른 통화면 `unmapped`(`unrecognized_currency`)다. 모든 가격이 요청 통화의 시장이거나
  `mapped` 가격 행이 없으면 `fx` 행은 없다.

사상표를 바꾸면 새 map ID가 되고, 따라서 그 사상표를 쓰는 정의는 새 버전이 된다. 저장된 정의의 의미는
바뀌지 않는다. 요구 행의 dataset ID는 소비자가 pin할 dataset의 후보이며 pin이나 binding이 아니다.

**대조.** 유도한 가격·거시 요구는 원천의 `asset_dependency`·`macro_dependency`와 (역할, ordinal, 토큰)
집합으로 같아야 한다. 어긋난 전략은 `--plan`이 `dependency_mismatches`로 보고하고 `--apply`는 등록
전체를 거부한다. `--plan`은 쓰지 않고 dataset ID마다 요구 수·전략 수, state 카탈로그(`datasets`)에
있는지, committed 버전 수를 보고한다. 카탈로그에 없는 dataset을 가리키는 요구도 그대로 등록된다. 등록은
요구를 기록할 뿐 그 dataset이 있다고 주장하지 않는다.

**쓰기와 복구.** 등록은 `aas-strategy-registry-request-v1` 요청 hash(원천 pin, 세 테이블 digest, 원천
형식, 정의 schema, 사상표)로 state intent(`strategy_registry`)를 PREPARED로 남기고, 비공개 저장소의 한
트랜잭션에서 `strategy_registrations` marker, 전략, 정의, 요구, 출처 행을 쓴 뒤 intent를 완료한다.
intent의 payload hash는 등록 대상 `(strategy_id, version, document_sha256)` 집합의 hash다. 같은 요청을 다시
실행하면 marker를 재사용한다. marker가 commit된 채 PREPARED로 남은 intent는 `aas db recover`가 marker가
덮는 정의를 다시 유도해 완료하며 원천을 다시 읽지 않는다. marker 없이 PREPARED로 남은 intent는 같은
`--apply`를 다시 실행하면 같은 operation ID로 쓰고 완료한다. operation ID가 요청 hash라서 격리된 intent는
그 원천의 모든 `--apply`를 거부하게 되므로 `aas db quarantine`은 `strategy_registry` intent를 끝내지 않는다.
`aas db verify`는 marker와 intent를 양방향으로 대조하고(marker에 PREPARED·COMPLETED intent가 있고,
COMPLETED intent에 marker가 있다) 모든 정의의 hash와 요구 행을 다시 확인해 `strategy_registry`(marker가 있는
등록 수, 전략 수, 정의 수)로 보고한다. 정의 문서 hash, 요청 hash와 payload hash 형식, 그리고 확장 DDL의
checksum은 테스트에 기록된 값으로 고정된다.

**저장 확장.** 레지스트리 테이블은 원천 자료실처럼 `strategies.sqlite3` 안의 별도 확장이다.
`strategy_registry_schema`가 버전과 DDL checksum을 한 행으로 기록하고, 다른 checksum은 거부한다. 확장은 첫
`--apply`가 만들며 `--plan`은 만들지 않는다. strategies core schema는 v1 그대로다.

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
| DV-10 | 승격 명세는 알 수 없는 필드·`latest`·해시 불일치를 거부한다 | `tests/storage/test_promotion_spec.py::test_spec_rejects_unknown_fields_and_moving_refs` | 구현 |
| DV-11 | 같은 승격 요청은 같은 generation을 재사용한다 | `tests/storage/test_promotion_engine.py::test_same_request_reuses_generation` | 구현 |
| DV-12 | 같은 명세의 재승격은 빈 delta다 | `tests/storage/test_promotion_engine.py::test_repromotion_yields_empty_delta` | 구현 |
| DV-13 | op는 head 비교로 ASSERT·SUPERSEDE·TOMBSTONE·skip을 정한다 | `tests/storage/test_promotion_engine.py::test_head_diff_decides_operation` | 구현 |
| DV-14 | TOMBSTONE은 전체 snapshot과 명세 허용이 있을 때만 생긴다 | `tests/storage/test_promotion_engine.py::test_tombstone_requires_full_snapshot_policy` | 구현 |
| DV-15 | `revision_id`는 직전 revision과 dataset을 포함해 A→B→A에서도, 같은 원천을 다른 dataset에 승격해도 유일하다 | `tests/storage/test_promotion_engine.py::test_revision_id_is_unique_across_value_return` | 구현 |
| DV-16 | 승격 시각은 어떤 열에도 들어가지 않는다 | `tests/storage/test_promotion_engine.py::test_promotion_is_independent_of_wall_clock` | 구현 |
| DV-17 | 승격 도중 중단은 게시 단계만 재개하고 공급자를 호출하지 않는다 | `tests/storage/test_promotion_engine.py::test_interrupted_promotion_resumes_publication_only` | 구현 |
| DV-18 | 시간 규칙은 수집 시각으로 null을 채우지 않고 근거가 없으면 null이다 | `tests/storage/test_time_rules.py::test_rules_never_fill_null_from_ingestion` | 구현 |
| DV-19 | `session_close_plus_lag@1`은 pin한 세션 종료 + lag이며 세션이 없으면 null이다 | `tests/storage/test_time_rules.py::test_session_close_plus_lag` | 구현 |
| DV-20 | `local_day_end@1`은 현지 날짜 끝이며 `time_precision_day` flag를 단다 | `tests/storage/test_time_rules.py::test_local_day_end_flags_day_precision` | 구현 |
| DV-21 | grant에 없는 규칙의 시점은 strict에서 제외되고 영수증에 grant가 남는다 | `tests/storage/test_read_heads.py::test_ungranted_rule_rows_excluded_from_strict` | 구현 |
| DV-22 | `krw_tick@1`은 정확한 십진 전개를 원 단위로 HALF_EVEN 반올림하고 flag를 단다 | `tests/storage/test_decimal_rules.py::test_krw_tick_rounds_half_even_and_flags` | 구현 |
| DV-23 | 숫자 규칙의 SQL 결과와 Python 결과가 같다 | `tests/storage/test_decimal_rules.py::test_sql_and_python_rounding_parity` | 구현 |
| DV-24 | flag는 값을 바꾸지 않고 generation manifest에 해시로 고정된다 | `tests/storage/test_promotion_engine.py::test_quality_flags_are_hashed_with_generation` | 구현 |
| DV-25 | 대량 게시의 delta·chain hash는 Python `aas-rowset-v1` 경로와 같다 | `tests/storage/test_bulk_generation.py::test_streaming_hash_matches_python_rowset` | 구현 |
| DV-26 | `read_heads`는 `project_heads`와 같은 head를 돌려준다 | `tests/storage/test_read_heads.py::test_read_heads_matches_project_heads` | 구현 |
| DV-27 | cutover 구간 밖 날짜는 다른 pin으로 채우지 않고 누락으로 보고한다 | `tests/storage/test_read_heads.py::test_cutover_gap_is_reported_not_filled` | 구현 |
| DV-28 | 유도 조정 가격은 cutoff 이후 기업행동을 쓰지 않는다 | `tests/storage/test_read_heads.py::test_adjustment_ignores_actions_after_cutoff` | 구현 |
| DV-29 | 티커로 instrument를 만들 수 없다 | `tests/storage/test_identity_mint.py::test_ticker_anchor_is_refused` | 구현 |
| DV-30 | v1→v2 migration은 백업 없이 거부하고 중단 후 재개한다 | `tests/storage/test_migration.py::test_migration_requires_backup_and_resumes` | 구현 |
| DV-31 | migration 후 v1 checksum 행이 남고 알 수 없는 버전은 거부한다 | `tests/storage/test_migration.py::test_migration_keeps_v1_receipt_and_rejects_unknown` | 구현 |
| DV-32 | `fields='close'` 가격은 reference만 될 수 있다 | `tests/storage/test_migration.py::test_close_only_prices_are_reference` | 구현 |
| DV-33 | 참조 중이거나 동치가 아니거나 백업이 없는 원천은 은퇴하지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_requires_proof` | 구현 |
| DV-34 | 은퇴는 `raw/` 원본을 지우지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_keeps_raw_bytes` | 구현 |
| DV-35 | 대응표의 구현 행은 존재하는 테스트를, 예정 행은 아직 없는 테스트를 가리킨다 | `tests/tools/test_data_vertical_contract.py::test_contract_rows_match_tests` | 구현 |
| DV-36 | `record` 근거 규칙의 SUPERSEDE 시점은 정정을 담은 원천의 증거 시각이다 | `tests/storage/test_promotion_engine.py::test_superseding_revision_is_not_known_before_its_source` | 구현 |
| DV-37 | 명세 `scope` 밖의 record는 원천에서 빠져도 TOMBSTONE되지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_stays_within_declared_scope` | 구현 |
| DV-38 | TOMBSTONE 시점은 부재를 증명한 snapshot의 증거 시각이며 record 날짜 규칙을 쓰지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_time_comes_from_absence_snapshot` | 구현 |
| DV-39 | 같은 값을 다른 수집 시각에 다시 수집해도 revision이 생기지 않는다 | `tests/storage/test_promotion_engine.py::test_recollection_at_new_ingestion_time_is_not_a_revision` | 구현 |
| DV-40 | head보다 이른 시점의 원천 행은 head를 대체하지 않고 stale로 보고된다 | `tests/storage/test_promotion_engine.py::test_older_source_row_does_not_supersede_newer_head` | 구현 |
| DV-41 | 참조가 없고 동치이며 백업된 원천은 자기 source-link 행이 있어도 은퇴하고 그 행은 남는다 | `tests/storage/test_source_retirement.py::test_unreferenced_equivalent_backed_up_source_is_retired` | 구현 |
| DV-42 | `source_id` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_source_identity.py::test_source_id_format_is_frozen` | 구현 |
| DV-43 | `revision_id` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_revision_id_format_is_frozen` | 구현 |
| DV-44 | `source_row_hash` 형식은 float·bytes·null을 포함한 고정 입력과 기대 값으로 고정되고 `_aas_ordinal`을 제외한다 | `tests/storage/test_promotion_formats.py::test_source_row_hash_format_is_frozen` | 구현 |
| DV-45 | TOMBSTONE 해시 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_tombstone_hash_format_is_frozen` | 구현 |
| DV-46 | `request_hash` 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_promotion_formats.py::test_request_hash_format_is_frozen` | 구현 |
| DV-47 | 매퍼의 SQL `source_row_hash`는 Python 계산과 같다 | `tests/storage/test_promotion_formats.py::test_source_row_hash_sql_matches_python` | 구현 |
| DV-48 | `float_shortest@1`은 소수 12자리를 넘으면 HALF_EVEN으로 반올림하고 범위를 넘는 값은 거부한다 | `tests/storage/test_decimal_rules.py::test_float_shortest_rounds_half_even_and_rejects_overflow` | 구현 |
| DV-49 | 매퍼는 자연키가 겹치는 원천 행을 거부한다 | `tests/storage/test_promotion_engine.py::test_mapper_rejects_overlapping_natural_keys` | 구현 |
| DV-50 | identity로 해석하지 못한 행은 승격하지 않고 미해결 보고에 남는다 | `tests/storage/test_promotion_engine.py::test_unresolved_identity_rows_are_reported_not_promoted` | 구현 |
| DV-51 | flag 제외 목록은 binding hash와 읽기 영수증에 들어가고, 제외한 revision은 알려진 시점부터 이전 head를 지운다 | `tests/storage/test_read_heads.py::test_flag_exclusions_enter_bundle_hash_and_receipt` | 구현 |
| DV-52 | `cross_provider_mismatch`는 명세의 허용오차를 넘을 때만 달린다 | `tests/storage/test_promotion_engine.py::test_cross_provider_mismatch_uses_spec_tolerance` | 구현 |
| DV-53 | 같은 parent에 다른 요청이 먼저 게시되면 부모 CAS가 실패한다 | `tests/storage/test_promotion_engine.py::test_competing_request_fails_parent_cas` | 구현 |
| DV-54 | migration-incomplete 설치본은 정상으로 열리지 않는다 | `tests/storage/test_migration.py::test_incomplete_migration_refuses_normal_open` | 구현 |
| DV-55 | 수집 시각보다 늦은 규칙 시점은 물리 기준 이후에 받은 행에서만 수집 시각으로 내려가 flag를 달고, 물리 기준 전에 받은 행은 보류로 보고된다 | `tests/storage/test_time_rules.py::test_rule_after_ingestion_is_clamped_above_physical_base` | 구현 |
| DV-56 | parent 명세와 시간 규칙(달력 pin 제외)이 다른 승격은 거부되고 규칙 변경은 `.r<N>` 새 dataset으로만 한다 | `tests/storage/test_promotion_engine.py::test_time_rule_change_requires_new_chain` | 구현 |
| DV-57 | identity 정정은 새 assertion이며 기존 행을 UPDATE하지 않는다 | `tests/storage/test_identity_registration.py::test_correction_is_a_new_assertion_without_update` | 구현 |
| DV-58 | 같은 provider key의 두 assertion이 정정 관계 없이 유효·지식 구간에서 겹치면 등록을 거부한다 | `tests/storage/test_identity_registration.py::test_overlapping_unrelated_assertions_conflict` | 구현 |
| DV-59 | instrument·issuer·assertion ID 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_identity_mint.py::test_identity_id_formats_are_frozen` | 구현 |
| DV-60 | 35,603건 identity 문서의 part와 manifest hash는 다른 설치에서도 같게 재현된다 | `tests/storage/test_identity_snapshot.py::test_chunked_snapshot_hashes_reproduce` | 구현 |
| DV-61 | part 이름은 manifest 전용이고 v1 문서와 manifest는 root를 공유하지 않는다 | `tests/storage/test_identity_snapshot.py::test_part_names_are_reserved_for_manifests` | 구현 |
| DV-62 | 각자 유효한 part 사이의 identity 구간 겹침도 거부한다 | `tests/storage/test_identity_snapshot.py::test_cross_part_overlap_is_rejected` | 구현 |
| DV-63 | 정정 assertion은 그것을 실은 원천의 수집 시각보다 먼저 알려질 수 없다 | `tests/storage/test_identity_registration.py::test_correction_is_never_known_before_its_source` | 구현 |
| DV-64 | issuer 없이 등록한 instrument도 나중에 `issuer` assertion으로 issuer에 연결된다 | `tests/storage/test_identity_registration.py::test_issuer_link_after_a_null_issuer` | 구현 |
| DV-65 | manifest root 아래에 저장된 member 행은 읽기와 검증에서 거부된다 | `tests/storage/test_identity_snapshot.py::test_manifest_root_members_are_rejected` | 구현 |
| DV-66 | part는 참조하는 원천의 파일 목록 전체를 싣고, 한 part에 들어가지 않는 원천은 거부한다 | `tests/storage/test_identity_snapshot.py::test_source_inventory_bounds_a_part` | 구현 |
| DV-67 | 대량 게시의 SQL 셀 부호화는 Python `aas-rowset-v1` codec과 같다 | `tests/storage/test_bulk_generation.py::test_sql_cell_encoding_matches_python_codec` | 구현 |
| DV-68 | 대량 게시의 SQL `record_id`는 `aas-record-v1` Python 계산과 같다 | `tests/storage/test_bulk_generation.py::test_sql_record_identity_matches_python` | 구현 |
| DV-69 | 대량 게시는 `normalize_rows`가 거부하는 행을 같은 이유로 거부한다 | `tests/storage/test_bulk_generation.py::test_row_rules_match_normalize_rows` | 구현 |
| DV-70 | 계획 뒤 dataset head가 바뀌면 대량 게시는 부모 CAS로 거부되고 새 head에서 다시 계획한다 | `tests/storage/test_bulk_generation.py::test_parent_cas_mismatch_requires_replan` | 구현 |
| DV-71 | 같은 ID의 기존 generation은 내용이 같은 요청에만 재사용되고 다른 요청이 채택하지 않는다 | `tests/storage/test_bulk_generation.py::test_leftover_generation_is_not_adopted` | 구현 |
| DV-72 | 증분 검증은 모든 chain link와 대상 generation의 행을, `deep`은 모든 delta를 다시 해시한다 | `tests/storage/test_bulk_generation.py::test_incremental_verify_checks_links_and_leaf_rows` | 구현 |
| DV-73 | 대량 게시의 Python 배치 과금은 가장 넓은 행으로 정해지고 행 수와 무관하다. DuckDB 몫은 포함하지 않는다 | `tests/storage/test_bulk_generation.py::test_python_batch_charge_is_independent_of_row_count` | 구현 |
| DV-74 | DuckDB가 할당 안에서 끝내지 못한 대량 게시는 marker와 행을 남기지 않고 `ComputeResourceError`가 된다 | `tests/storage/test_bulk_generation.py::test_duckdb_exhaustion_rolls_back_as_a_budget_error` | 구현 |
| DV-75 | v2 key 색인이 작은 DuckDB 몫을 넘는 큰 테이블에서도 v3로 올린 같은 설치본은 같은 lease 안에서 유지보수 generation을 계획대로 게시하고 검증한다 | `tests/storage/test_core_v3_memory.py::test_the_rehearsal_failure_shape_publishes_once_the_store_is_v3` | 구현 |
| DV-76 | `aas-head-binding-v1` binding hash와 `aas-head-read-v1` 영수증 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_read_heads.py::test_head_binding_and_receipt_formats_are_frozen` | 구현 |
| DV-77 | `read_heads`는 pin이 marker와 다르거나 chain link·행 수가 맞지 않으면 행을 읽지 않고, `rehash`는 모든 delta를 다시 해시한다 | `tests/storage/test_read_heads.py::test_pins_are_verified_before_rows_are_read` | 구현 |
| DV-78 | generation의 시간 규칙 출처는 보존 증거(승격 명세, 연구 변환, 봉인 import 문서)에서 오고, 출처가 없거나 catalog에 없는 pin은 읽지 않는다 | `tests/storage/test_read_heads.py::test_time_rule_provenance_comes_from_retained_evidence` | 구현 |
| DV-79 | `read_heads`는 fetch 전에 결과 크기를 SQL로 재어 할당을 넘으면 `ComputeResourceError`로 거부한다 | `tests/storage/test_read_heads.py::test_head_read_is_admitted_before_rows_are_fetched` | 구현 |
| DV-80 | inspection·연구 읽기는 grant와 무관하고 grant 하나는 그 규칙의 시점만 strict에 허용한다 | `tests/storage/test_read_heads.py::test_rule_grant_changes_strict_reads_only` | 구현 |
| DV-81 | strict 실행 준비는 사용한 `read_heads` 읽기 영수증을 run에 그대로 기록한다 | `tests/application/test_strict_head_inputs.py::test_strict_preparation_records_head_read_receipt` | 구현 |
| DV-82 | pin 하나의 strict 읽기는 `market_inputs` strict reader와 같은 coverage 이유를 보고한다 | `tests/storage/test_read_heads.py::test_coverage_reasons_match_market_inputs` | 구현 |
| DV-83 | 여러 pin의 읽기는 각 pin chain의 `project_heads`를 그 pin 구간으로 거른 것과 같다 | `tests/storage/test_read_heads.py::test_multi_pin_reads_match_each_pin_projection` | 구현 |
| DV-84 | 승격 `--plan`은 같은 계산을 보고하고 저장소에 아무것도 쓰지 않는다 | `tests/storage/test_promotion_cli.py::test_promote_plan_writes_nothing_and_apply_publishes` | 구현 |
| DV-85 | `eodhd.bars@1`은 합성 원천 fixture를 독립 기대값과 같은 도메인 열로 옮긴다 | `tests/storage/test_promotion_mappers.py::test_eodhd_bars_maps_synthetic_fixture` | 구현 |
| DV-86 | 같은 chain의 승격은 달력 pin을 parent 달력 generation의 후손으로 옮길 수 있고 다른 달력 dataset의 pin은 거부된다 | `tests/storage/test_promotion_engine.py::test_calendar_descendant_extends_chain` | 구현 |
| DV-87 | `cross_provider_mismatch`는 role을 뺀 가격 키가 같은 기준 행과만 비교하고 revision마다 flag를 하나만 단다 | `tests/storage/test_promotion_engine.py::test_cross_provider_matches_one_reference_per_key` | 구현 |
| DV-88 | `krw_tick@1`은 KRW가 아닌 행의 값을 반올림하지 않고 숫자 거부로 보고한다 | `tests/storage/test_promotion_engine.py::test_krw_tick_refuses_non_krw_rows` | 구현 |
| DV-89 | 부재는 전체 snapshot pin의 행으로만 판단하고 범위 안의 다른 pin 행은 계획 거부로 보고된다 | `tests/storage/test_promotion_engine.py::test_absence_is_proven_by_the_full_snapshot_only` | 구현 |
| DV-90 | watermark의 version은 시각을 앞으로 옮긴 generation만 바꾼다 | `tests/storage/test_promotion_engine.py::test_watermark_version_follows_its_time` | 구현 |
| DV-91 | `calendar.declared@1`은 합성 원천 fixture를 독립 기대값과 같은 도메인 열과 `public_by`로 옮기고 잘못된 행의 `status`를 비운다 | `tests/storage/test_promotion_mappers.py::test_calendar_declared_maps_synthetic_fixture` | 구현 |
| DV-92 | 임시 휴장은 새 generation의 SUPERSEDE이고 이전 pin은 그대로 검증되고 읽힌다 | `tests/storage/test_calendar_refresh.py::test_temporary_closure_is_a_new_generation` | 구현 |
| DV-93 | head 선언보다 이른 선언, 같은 시각의 다른 선언, 미래 시각의 선언, head 선언의 날짜를 모두 덮지 않는 선언은 갱신하지 못한다 | `tests/storage/test_calendar_refresh.py::test_older_declaration_cannot_undo_a_newer_one` | 구현 |
| DV-94 | 선언 달력의 처음 선언된 날짜 시점은 `declared_session_end@1`로 선언 시각과 그 session의 끝 중 이른 값이다 | `tests/storage/test_calendar_refresh.py::test_session_times_are_bounded_by_the_declaration` | 구현 |
| DV-95 | 선언 문서는 범위 밖·중복·구간을 반복하는 예외와 잘못된 형식을 거부한다 | `tests/storage/test_calendar_declaration.py::test_declaration_has_one_spelling` | 구현 |
| DV-96 | 패키지 XNYS·XKRX 선언은 2027년 일정을 포함해 2027-12-31까지 덮는다 | `tests/storage/test_calendar_declaration.py::test_packaged_declarations_cover_the_next_year` | 구현 |
| DV-97 | `calendar refresh --plan`은 아무것도 쓰지 않고 head 선언과의 날짜 변경을 보고한다 | `tests/storage/test_calendar_refresh.py::test_refresh_plan_writes_nothing` | 구현 |
| DV-98 | 과거 날짜의 정정(휴장, 이른 마감, 재개장)은 SUPERSEDE로 게시되고 정정 선언을 받은 시각부터 알려지며, 두 선언 사이 cutoff의 strict 읽기는 grant 아래 이전 선언을, grant 없이는 아무 행도 돌려주지 않는다 | `tests/storage/test_calendar_refresh.py::test_past_corrections_are_known_from_their_declaration` | 구현 |
| DV-99 | stale 행이 남는 선언 갱신은 게시하지 않고 거부한다 | `tests/storage/test_calendar_refresh.py::test_refresh_refuses_a_stale_plan` | 구현 |
| DV-100 | `declared_session_end@1`은 입력 상한을 값과 물리 기준으로 쓰고 근거 `record`만 받는다 | `tests/storage/test_time_rules.py::test_declared_session_end_is_a_record_rule_on_its_bound` | 구현 |
| DV-101 | KR instrument는 검사 숫자가 맞는 KR ISIN에서만 발급되고 EODHD 심볼·단축코드 assertion은 수집 시각부터 알려진다 | `tests/storage/test_kr_identity.py::test_kr_instruments_are_minted_from_isin_never_from_a_code` | 구현 |
| DV-102 | 없거나 틀리거나 KR이 아닌 ISIN과 심볼·ISIN·단축코드의 모호한 매칭은 이유와 함께 미해결로 남는다 | `tests/storage/test_kr_identity.py::test_missing_invalid_and_ambiguous_isins_stay_unresolved` | 구현 |
| DV-103 | KIND 상장일과 DART issuer 연결은 하나의 ISIN에 묶인 단축코드로만 instrument에 닿는다 | `tests/storage/test_kr_identity.py::test_kind_and_dart_reach_an_instrument_only_through_one_isin` | 구현 |
| DV-104 | KR 원천 단위는 receipt가 기록한 크기·SHA-256과 완료 문서의 파일 목록으로 확인된다 | `tests/storage/test_kr_identity.py::test_kr_receipts_are_checked_against_their_recorded_bytes` | 구현 |
| DV-105 | DART 매퍼는 완료된 `corp_codes` receipt 하나의 해시가 맞는 압축 문서만 읽고 다듬어지지 않은 회사명을 거부한다 | `tests/storage/test_kr_identity.py::test_dart_receipt_must_be_one_completed_corp_code_archive` | 구현 |
| DV-106 | KR 원천은 내용 원천으로 재사용되고 commit된 원천에서 만든 문서는 누락 참조 없이 한 번에 등록된다 | `tests/storage/test_kr_identity.py::test_kr_sources_import_as_content_and_register_as_one_document` | 구현 |
| DV-107 | KR 등록의 snapshot은 `eodhd.bars@1` 승격에서 심볼을 instrument로 해석하고 ISIN이 없는 심볼은 미해결로 둔다 | `tests/storage/test_kr_identity.py::test_kr_registry_resolves_eodhd_bars_in_promotion` | 구현 |
| DV-108 | `identity kr-import --plan`은 쓰지 않고 `kr-build`는 새 파일에만 쓰며 그 문서는 `identity register`로 등록된다 | `tests/storage/test_kr_identity.py::test_kr_cli_imports_builds_and_registers` | 구현 |
| DV-109 | ISIN을 실은 목록끼리 한 심볼의 ISIN·유형·통화가 다르면 어느 쪽도 고르지 않고 미해결로 둔다 | `tests/storage/test_kr_identity.py::test_lists_that_disagree_on_a_symbol_leave_it_unresolved` | 구현 |
| DV-110 | KR 빌드는 등록된 KR assertion의 원천을 모두 읽어야 하며, 더는 나오지 않는 등록 주장을 `withdrawn`으로 보고한다 | `tests/storage/test_kr_identity.py::test_a_build_reads_every_registered_kr_source_and_reports_withdrawn_claims` | 구현 |
| DV-111 | KRX에 상장한 외국 기업은 자기 ISIN으로 해석되고 KR 주식의 asset_type은 `unclassified`다 | `tests/storage/test_kr_identity.py::test_a_foreign_issuer_listed_on_krx_resolves_by_its_own_isin` | 구현 |
| DV-112 | KIND 상장일과 DART receipt 여러 개를 함께 읽어 충돌하는 단축코드·회사는 미해결로 둔다 | `tests/storage/test_kr_identity.py::test_dart_receipts_and_kind_lists_read_together_leave_conflicts_unresolved` | 구현 |
| DV-113 | legacy 편입 `--plan`은 원본만 읽어 행 수·digest·원천 ID와 대조를 보고하고 설치본과 원본에 아무것도 쓰지 않는다 | `tests/storage/test_legacy_import.py::test_plan_reads_originals_and_writes_nothing` | 구현 |
| DV-114 | legacy 편입은 단위의 원본을 모두 `raw/`에 보존하고 테이블마다 내용 원천을 commit·연결하며 다시 실행하면 재사용한다 | `tests/storage/test_legacy_import.py::test_apply_commits_content_sources_and_reruns_reuse` | 구현 |
| DV-115 | legacy 원천 ID는 단위의 bytes에서만 나오며 경로·manifest 이름과 무관하다 | `tests/storage/test_legacy_import.py::test_source_ids_follow_bytes_not_paths` | 구현 |
| DV-116 | legacy CSV 값은 원문 문자열로 보존되고 원본에 없는 열만 null이다 | `tests/storage/test_legacy_import.py::test_history_rows_keep_original_text` | 구현 |
| DV-117 | 자기 색인과 다른 bytes, 알 수 없는 열, 비공개가 아닌 원본의 단위는 거부되고 실행은 그 단위에서 멈춘다 | `tests/storage/test_legacy_import.py::test_unit_contradicting_its_index_is_refused` | 구현 |
| DV-118 | `--verify`는 계획한 원천이 완료·동일·연결되고 저장 테이블이 다시 해시해도 같으며 `raw/` 원본이 온전할 때만 `complete`이다 | `tests/storage/test_legacy_import.py::test_verify_requires_committed_identical_linked_sources` | 구현 |
| DV-119 | 지수 구성 편입은 계획한 쌍과 수집한 쌍의 누락·계획 밖·반복을 보고하고 journal이 manifest와 다르면 거부한다 | `tests/storage/test_legacy_import.py::test_membership_reconciles_planned_pairs` | 구현 |
| DV-120 | SEC archive 편입은 member마다 크기·CRC·SHA-256을 색인하고 영수증이 archive와 다르면 거부한다 | `tests/storage/test_legacy_import.py::test_sec_archive_indexes_members_and_checks_receipts` | 구현 |
| DV-121 | FRED·KR 공개·FMP·identity authority loader는 합성 원본을 독립 기대값과 같은 행으로 옮긴다 | `tests/storage/test_legacy_import.py::test_small_loaders_map_their_originals` | 구현 |
| DV-122 | Norgate 내보내기의 재사용 시리즈는 checkpoint 단위로 편입되고(배치 없는 내보내기 포함), 편입되기 전에는 `missing_series`로 남아 기록되지 않은 불일치로 대조를 실패시키며, 계획과 다른 checkpoint나 acquisition은 거부된다 | `tests/storage/test_legacy_import.py::test_reused_series_are_units_until_imported` | 구현 |
| DV-123 | 항목 경로 아래 어떤 단위도 덮지 않는 파일은 `uncovered`로 보고되고 `retain`·`exclude`로 기록되기 전까지 `complete`를 막으며, 보존 파일의 `raw/` 사본이 없으면 `unmatched`다 | `tests/storage/test_legacy_import.py::test_uncovered_files_keep_verify_incomplete` | 구현 |
| DV-124 | 등록된 모든 loader에서 계획·실행·재실행·검증의 원천 ID·행 수·digest가 같고 재실행은 재사용, 검증은 `complete`다 | `tests/storage/test_legacy_import.py::test_every_loader_plans_applies_and_verifies_alike` | 구현 |
| DV-125 | 보존한 `raw/` 사본이 주소와 다르거나 크기가 다르면 legacy 단위 읽기를 거부한다 | `tests/storage/test_legacy_import.py::test_retained_bytes_refuse_a_changed_raw_object` | 구현 |
| DV-126 | US instrument는 Norgate asset ID에서만 발급되고 master 행이 하나씩 instrument가 되며 Norgate 주장은 원천 연결 시각부터 알려진다 | `tests/storage/test_us_identity.py::test_us_instruments_are_minted_from_norgate_asset_ids` | 구현 |
| DV-127 | EODHD·FMP 심볼은 상장 행 하나의 티커로만 instrument에 닿고 두 상장 행이 같은 티커면 미해결이다 | `tests/storage/test_us_identity.py::test_provider_symbols_reach_only_a_unique_active_ticker` | 구현 |
| DV-128 | issuer 연결은 SEC가 티커를 하나의 CIK에 싣고 FMP가 같은 CIK를 줄 때만 생기며 SEC·FMP 중 늦은 수집 시각부터 유효하고 세 근거 중 가장 늦은 시각부터 알려진다. FMP profile이 없는 티커와 CIK가 없는 profile은 이유가 다르다 | `tests/storage/test_us_identity.py::test_issuer_needs_sec_and_fmp_to_agree` | 구현 |
| DV-129 | SEC 매퍼는 member 색인으로 archive를 읽고 기록과 다른 archive·member bytes를 거부한다 | `tests/storage/test_us_identity.py::test_sec_members_are_read_through_their_index` | 구현 |
| DV-130 | 서로 다른 FMP 행, Norgate와 다른 유형·통화, 두 티커가 공유한 CUSIP·ISIN은 미해결로 남고 CUSIP·ISIN은 수집 시각부터 유효하다 | `tests/storage/test_us_identity.py::test_fmp_disagreement_and_shared_identifiers_stay_unresolved` | 구현 |
| DV-131 | commit된 US 원천에서 만든 문서는 누락 참조·충돌 없이 한 번에 등록되고 legacy bindings와의 asset ID 집합 비교를 보고한다 | `tests/storage/test_us_identity.py::test_us_sources_register_as_one_document` | 구현 |
| DV-132 | US 등록의 snapshot은 `eodhd.bars@1` 승격에서 `.US` 심볼을 instrument로 해석하고 상장 티커가 아닌 심볼은 미해결로 둔다 | `tests/storage/test_us_identity.py::test_us_registry_resolves_eodhd_bars_in_promotion` | 구현 |
| DV-133 | US 빌드는 등록된 US assertion의 원천을 모두 읽어야 하며 더는 나오지 않는 등록 주장을 `withdrawn`으로 보고한다 | `tests/storage/test_us_identity.py::test_a_us_build_reads_every_registered_us_source` | 구현 |
| DV-134 | `identity us-build`는 새 파일에만 쓰고 그 문서는 `identity register`로 등록된다 | `tests/storage/test_us_identity.py::test_us_cli_builds_and_registers` | 구현 |
| DV-135 | 상장 티커의 주장은 상장 `first_date`와 이전 상폐 보유자의 `last_date` 다음 날 중 늦은 날부터 master의 마지막 관측 세션 다음 날까지만 유효하고, 구간을 정할 수 없는 티커는 미해결이다 | `tests/storage/test_us_identity.py::test_ticker_claims_are_bounded_by_the_master` | 구현 |
| DV-136 | `eodhd.bars@1` 승격은 master 마지막 관측 세션 뒤의 bar와 이전 보유자 시기의 bar를 미해결로 둔다 | `tests/storage/test_us_identity.py::test_us_registry_resolves_eodhd_bars_in_promotion` | 구현 |
| DV-137 | `aas-norgate-assetids-v1` 해시는 고정값을 재현하고 중복·순서에 무관하다 | `tests/storage/test_us_identity.py::test_the_asset_id_set_hash_is_pinned` | 구현 |
| DV-138 | 수집 시각이 없거나 시간대가 없는 FMP 행은 그 심볼만 `fmp_retrieved_invalid`로 둔다 | `tests/storage/test_us_identity.py::test_an_fmp_row_without_a_retrieval_instant_refuses_its_symbol` | 구현 |
| DV-139 | 상한을 넘거나 archive에 없는 색인 member와 zip이 아닌 archive는 SEC 원천 전체를 거부한다 | `tests/storage/test_us_identity.py::test_sec_archives_that_do_not_match_their_index_are_refused` | 구현 |
| DV-140 | SEC 매퍼는 zip 하나를 보존한 `sec-submissions-zip-*` 내용 원천만 읽는다 | `tests/storage/test_us_identity.py::test_only_an_sec_submissions_zip_source_is_read` | 구현 |
| DV-141 | 이미 정정된 등록 US 주장은 이번 원천이 내지 않아도 `withdrawn`에 들지 않는다 | `tests/storage/test_us_identity.py::test_a_corrected_registered_claim_is_not_reported_withdrawn` | 구현 |
| DV-142 | `scripts/us_identity_report.py`는 market 파일만 읽기 전용으로 열어 bulk·격리 행의 US 해석을 이유별로 보고한다 | `tests/storage/test_us_identity.py::test_the_report_script_resolves_bulk_and_quarantined_us_rows` | 구현 |
| DV-143 | legacy 항목의 보존 파일은 경로·SHA-256·크기·이유를 담은 보존 목록 원천으로 commit되고, 목록 원천이 없거나 연결된 보존 bytes가 없으면 `--verify`가 `unmatched`로 센다 | `tests/storage/test_legacy_import.py::test_uncovered_files_keep_verify_incomplete` | 구현 |
| DV-144 | 압축 해제가 깨진 SEC member나 지수 구성 gzip은 그 단위를 이유와 함께 거부하고 나머지 계획은 이어진다 | `tests/storage/test_legacy_import.py::test_sec_archive_refuses_a_corrupt_deflate_stream` | 구현 |
| DV-145 | 상장의 티커 구간 밖에서 수집한 FMP profile과 SEC member는 그 상장에 FMP·issuer 주장을 만들지 않고 구간 안의 행 합의를 흐리지 않으며, 구간 안의 행이 없으면 이유와 함께 미해결로 남는다 | `tests/storage/test_us_identity.py::test_ticker_claims_are_bounded_by_the_master` | 구현 |
| DV-146 | `us_identity_report`가 읽은 master는 `us-build`와 같은 심볼·구간·`through`·미해결 이유를 낸다 | `tests/storage/test_us_identity.py::test_the_report_reads_the_master_as_us_build_does` | 구현 |
| DV-147 | 부분 응답 행은 막히지 않고 flag `provider_reported_partial`과 함께 승격되며, generation은 parent chain 대비 `partition_row_count@1` 검사를 남긴다 | `tests/storage/test_kr_prices.py::test_warned_bulk_rows_are_promoted_with_flags_and_counted` | 구현 |
| DV-148 | 부분 OHLCV와 음수 가격은 값 없이 `invalid`로, identity가 없는 심볼은 미해결로 남고 승격되지 않는다 | `tests/storage/test_kr_prices.py::test_kr_bars_refuse_partial_negative_and_unresolved` | 구현 |
| DV-149 | 공급자가 다시 계산한 분할 이전 가격은 `krw_tick@1`로 원 단위로 돌아오고 분할 비율과 맞는다 | `tests/storage/test_kr_prices.py::test_known_split_rounds_back_to_whole_won` | 구현 |
| DV-150 | `partition`이 있는 명세는 파티션 날짜가 없는 원천 행을 거부하고, 필수 열이 빈 행은 identity와 무관하게 거부된다 | `tests/storage/test_kr_prices.py::test_partitioned_plan_refuses_rows_without_partition_date` | 구현 |
| DV-151 | `aas data kr-prices`는 이력 연도와 부분 응답 날짜를 순서대로 승격하고 반복 내려받기를 가장 이른 연결로 한 번만 pin하며 다시 실행하면 쓰지 않는다 | `tests/storage/test_kr_prices.py::test_kr_prices_backfills_years_then_partial_days` | 구현 |
| DV-152 | `kr-prices --plan`은 설치본에 아무것도 쓰지 않는다 | `tests/storage/test_promotion_cli.py::test_kr_prices_plan_writes_nothing_and_apply_publishes` | 구현 |
| DV-153 | `eodhd.bulk_quarantine@1`은 합성 원천 fixture를 독립 기대값과 같은 도메인 열과 행 flag로 옮기고 다른 보류 이유·잘못된 날짜·JSON의 세션 날짜를 비운다 | `tests/storage/test_promotion_mappers.py::test_eodhd_bulk_quarantine_maps_synthetic_fixture` | 구현 |
| DV-154 | adjusted close 매퍼는 close 전용 `total_return` reference 가격을 낸다 | `tests/storage/test_promotion_mappers.py::test_eodhd_adjusted_close_maps_close_only_reference` | 구현 |
| DV-155 | `eodhd.bars_quarantine@1`은 manifest `jobs`로 보류 행의 심볼·완료 시각을 찾아 값 없는 `invalid` bar로 옮기고, 두 번 나오거나 없는 fingerprint·다른 보류 이유는 해석하지 않는다 | `tests/storage/test_promotion_mappers.py::test_eodhd_bars_quarantine_maps_held_history_rows` | 구현 |
| DV-156 | `kr-prices`는 보류된 이력 행을 이력 연도 뒤 한 단계로 `invalid` bar로 승격하고 반복 내려받기는 한 번만 pin하며 reference에는 그 단계가 없다 | `tests/storage/test_kr_prices.py::test_held_history_rows_become_invalid_bars` | 구현 |
| DV-157 | `manifest_items` 목록이 없는 원천을 pin한 계획은 거부되고 심볼 없는 보류 행은 필수 열 누락으로 거부된다 | `tests/storage/test_kr_prices.py::test_held_rows_without_manifest_jobs_are_refused` | 구현 |
| DV-158 | manifest 목록을 모양은 맞게 고쳐 request hash와 어긋난 원천을 pin한 계획은 거부된다 | `tests/storage/test_kr_prices.py::test_held_rows_with_an_edited_manifest_are_refused` | 구현 |
| DV-159 | `partition_row_count@1`의 기준은 부분 응답 flag가 없는 완결 날짜에서만 나와 앞선 부분 응답 날과 같은 수도 `below_reference`가 된다 | `tests/storage/test_kr_prices.py::test_partial_days_are_counted_against_complete_dates_only` | 구현 |
| DV-160 | 같은 날짜의 내용이 다른 내려받기는 연결 순서대로 이어지는 generation이 되어 나중 것이 자기 수집 시각으로 SUPERSEDE한다 | `tests/storage/test_kr_prices.py::test_a_later_different_download_of_a_day_supersedes_the_earlier` | 구현 |
| DV-161 | 매핑된 이유와 날짜가 없는 보류 행만 있으면 보류 단계 없이 `held_unmapped_rows`를 보고하고 이력 단계는 계획된다 | `tests/storage/test_kr_prices.py::test_held_rows_without_a_mapped_date_plan_no_held_step` | 구현 |
| DV-162 | 행이 있지만 날짜가 없는 이력 lineage는 거부된다 | `tests/storage/test_kr_prices.py::test_undated_history_is_refused` | 구현 |
| DV-163 | 거래소를 말하지 않는 부분 응답 테이블은 pin되지 않고 `bulk_unclassified_tables`에 남는다 | `tests/storage/test_kr_prices.py::test_bulk_tables_naming_no_exchange_are_listed` | 구현 |
| DV-164 | `fred.alfred@1`은 합성 원천을 독립 기대값과 같은 도메인 열로 옮기고 vintage 끝을 싣지 않으며 1970년 이전 vintage의 시간 입력은 1970-01-01이다 | `tests/storage/test_macro_fx.py::test_fred_alfred_maps_synthetic_fixture` | 구현 |
| DV-165 | ALFRED vintage는 vintage 구간 순서로 승격하면 SUPERSEDE가 되고, grant 아래 strict 읽기는 cutoff 당시 vintage를, grant 없이는 아무 행도 돌려주지 않는다 | `tests/storage/test_macro_fx.py::test_alfred_vintages_are_superseding_revisions` | 구현 |
| DV-166 | vintage 구간은 관측마다 vintage를 하나만 담고 모든 vintage를 덮는 가장 적은 목록이며, vintage 시작이 없는 행은 거부한다 | `tests/storage/test_macro_fx.py::test_vintage_partitions_hold_each_observation_once` | 구현 |
| DV-167 | FX 매퍼는 합성 원천을 독립 기대값과 같은 도메인 열로 옮기고 원문과 다른 값·날짜를 `invalid`로 둔다 | `tests/storage/test_macro_fx.py::test_fx_mappers_map_synthetic_fixtures` | 구현 |
| DV-168 | KR 공개 관측 매퍼는 기간 형식과 단위를 정해진 규칙으로만 옮기고 나머지는 필수 열을 비운다 | `tests/storage/test_macro_fx.py::test_korea_observations_map_synthetic_fixture` | 구현 |
| DV-169 | FX 승격은 한 통화쌍만 고르고 나머지를 `unselected_rows`로 보고하며, 전체 snapshot에서 빠진 고시는 `fixing_at_us`의 UTC 날짜로 범위를 정해 tombstone한다 | `tests/storage/test_macro_fx.py::test_fx_series_promote_one_pair_each` | 구현 |
| DV-170 | legacy FRED CSV와 KR 공개 응답은 편입 뒤 승격되고, 텍스트 날짜 원천의 파티션은 `YYYY-MM-DD` 날짜만 고르며 FRED FX와 시점이 없는 거시 행의 두 시점은 null이고 BOK 정책금리의 두 정의는 다른 단위다 | `tests/storage/test_macro_fx.py::test_legacy_fred_and_kr_public_sources_promote` | 구현 |
| DV-171 | legacy Norgate 내보내기는 편입 뒤 `norgate.fx_history@1`로 한 통화쌍이 승격된다 | `tests/storage/test_macro_fx.py::test_norgate_history_export_promotes_one_pair` | 구현 |
| DV-172 | 원천 ID 접두사를 선언한 매퍼는 다른 공급자의 원천 pin을 명세 단계에서 거부한다 | `tests/storage/test_macro_fx.py::test_legacy_fred_and_kr_public_sources_promote` | 구현 |
| DV-173 | 지수 구성 쌍의 연속한 `1` 행 묶음은 member 구간 하나이고, 구간을 쌍의 날짜에 펼치면 원래 일간 값이 나온다 | `tests/storage/test_universe.py::test_interval_compression_round_trips_daily_values` | 구현 |
| DV-174 | 지수 구성의 SQL 압축은 참조 구현과 같은 구간을 내고, 두 원천이 실은 쌍은 이어 붙이지 않고 통째로 거부한다 | `tests/storage/test_universe.py::test_index_universe_sql_matches_the_reference_and_round_trips` | 구현 |
| DV-175 | 값·날짜·순서·지수 이름이 정규가 아니거나 `9999-12-31`을 담은 쌍은 이유와 함께 통째로 거부하고, 등록되지 않은 asset ID는 미해결로 보고한다 | `tests/storage/test_universe.py::test_index_pairs_with_unreadable_values_are_refused` | 구현 |
| DV-176 | 지수 universe는 chunked 문서로 등록되고 재등록은 같은 pin이며, 한 build의 universe는 모두 등록되거나 하나도 등록되지 않고, 읽기와 `aas db verify`를 통과한다 | `tests/storage/test_universe.py::test_index_universes_register_and_read_back` | 구현 |
| DV-177 | 상장 universe의 member는 master 행의 `first_date`부터 `last_date` 다음 날까지이고 `last_date` 없는 상장 행은 `through`까지다 | `tests/storage/test_universe.py::test_listing_universe_spans_each_master_listing` | 구현 |
| DV-178 | universe part는 원천별로 채우고 읽기는 member를 정규 순서로 돌려준다 | `tests/storage/test_universe.py::test_universe_parts_are_filled_source_by_source` | 구현 |
| DV-179 | `aas universe --plan`은 쓰지 않고, 만들 수 없는 `--report` 경로는 등록 전에 거부하며, `--report`는 pin을 담고, 등록한 universe를 `aas universe show`가 읽는다 | `tests/storage/test_universe.py::test_universe_cli_plans_registers_and_shows` | 구현 |
| DV-180 | 두 원천이 실은 쌍은 한 사본이 다른 이유로 거부돼도 나머지 사본을 받지 않고 `pair_repeated`로 거부한다 | `tests/storage/test_universe.py::test_a_pair_two_sources_carry_is_refused_when_one_copy_is_refused` | 구현 |
| DV-181 | 원천 순서가 맞아도 두 part에 걸친 같은 member key는 읽기와 verify에서 거부한다 | `tests/storage/test_universe.py::test_a_member_key_repeated_across_parts_is_refused` | 구현 |
| DV-182 | 끝 날짜가 `9999-12-31`인 상장은 `listing_end_invalid`로 거부되고 `through`를 옮기지 않는다 | `tests/storage/test_universe.py::test_a_listing_ending_on_the_last_representable_date_is_refused` | 구현 |
| DV-183 | `norgate.classification@1`은 asset ID로 instrument를 발급하고 유형 경로·거래소를 옮기며, 비정규 asset ID·`as_of` 뒤 날짜·빈 단계는 주체나 코드를 비운다 | `tests/storage/test_classifications.py::test_norgate_classification_maps_synthetic_fixture` | 구현 |
| DV-184 | `sec.sic@1`은 member와 문서가 같은 CIK를 말할 때만 issuer를 발급하고 SIC나 그 설명이 없는 회사는 행을 내지 않는다 | `tests/storage/test_classifications.py::test_sec_sic_maps_synthetic_fixture` | 구현 |
| DV-185 | `kind.industry@1`은 단축코드를 수집 시각에 해석할 token으로 내고 `effective_from`은 그 시각의 Asia/Seoul 날짜다 | `tests/storage/test_classifications.py::test_kind_industry_maps_synthetic_fixture` | 구현 |
| DV-186 | 영구 anchor로 발급하는 분류 매퍼는 identity snapshot을 pin하지 않고 identity key가 있는 매퍼는 pin한다 | `tests/storage/test_classifications.py::test_classification_subjects_resolve_or_mint` | 구현 |
| DV-187 | 분류 snapshot은 snapshot 전 cutoff에서 반환되지 않고 날짜 규칙 시점은 grant한 strict 읽기에만 보인다 | `tests/storage/test_classifications.py::test_classifications_are_not_returned_before_the_snapshot` | 구현 |
| DV-188 | 이후 snapshot은 자기 날짜의 행을 더하고 앞의 행을 바꾸지 않으며 같은 snapshot의 재승격은 빈 delta다 | `tests/storage/test_classifications.py::test_a_later_snapshot_adds_rows_of_its_own_date` | 구현 |
| DV-189 | 선언한 `as_of`보다 늦은 날짜를 싣는 snapshot 행은 승격을 거부한다 | `tests/storage/test_classifications.py::test_snapshot_rows_dated_after_as_of_refuse_the_promotion` | 구현 |
| DV-190 | KIND 업종은 단축코드를 pin한 snapshot으로 해석하고 수집 시각 전에는 보이지 않는다 | `tests/storage/test_classifications.py::test_kind_industries_resolve_short_codes_through_the_snapshot` | 구현 |
| DV-191 | `import sec-companies`는 CIK member마다 문서 값을 텍스트로 commit하고 다시 실행하면 재사용하며, SIC나 그 설명이 없는 회사는 `unselected_rows`로 보고된다 | `tests/storage/test_classifications.py::test_sec_companies_import_and_promote_sic` | 구현 |
| DV-192 | `import sec-companies --plan`은 쓰지 않고 실행은 같은 원천 ID를 commit한다 | `tests/storage/test_classifications.py::test_sec_companies_cli_plans_and_imports` | 구현 |
| DV-193 | `partition`이 있는 분류 명세는 파티션 날짜가 없는 Norgate 행을 거부한다 | `tests/storage/test_classifications.py::test_a_partitioned_plan_refuses_undated_norgate_rows` | 구현 |
| DV-194 | UTF-8 JSON 객체가 아닌 CIK member는 `import sec-companies` 전체를 거부하고 아무것도 commit하지 않는다 | `tests/storage/test_classifications.py::test_sec_companies_refuse_a_member_that_is_not_a_json_object` | 구현 |
| DV-195 | 연구 모드의 지식 상한은 그보다 늦게 알려진 revision만 빼고 시점 없는 revision은 남기며, 설정했을 때만 query 문서에 들어간다 | `tests/storage/test_read_heads.py::test_research_known_ceiling_matches_snapshot_candidates` | 구현 |
| DV-196 | canonical 가격 pin에서 유도한 연구 실행 패널은 같은 값의 관측 패널과 같은 날짜·시가·종가·판단을 낸다 | `tests/application/test_research_prices.py::test_the_price_route_matches_the_observation_route` | 구현 |
| DV-197 | 가격 pin 연구 실행의 봉인 준비 문서는 그 읽기의 `aas-head-read-v1` 영수증과 해시를 싣는다 | `tests/application/test_research_prices.py::test_the_sealed_preparation_carries_the_head_read_receipt` | 구현 |
| DV-198 | `krw_tick@1`로 승격한 `prices.kr.eodhd` chain의 KRW 실행은 원 단위 가격으로 같은 판단을 낸다 | `tests/application/test_research_prices.py::test_a_krw_run_over_a_promoted_chain_decides_like_the_usd_run` | 구현 |
| DV-199 | 선언 지식 시점 뒤에 알려진 정정은 연구 패널에 들어가지 않는다 | `tests/application/test_research_prices.py::test_a_revision_received_after_the_knowledge_time_is_not_read` | 구현 |
| DV-200 | 선언 통화와 다른 가격과 pin이 싣지 않은 instrument는 거부된다 | `tests/application/test_research_prices.py::test_a_price_run_refuses_what_the_pins_do_not_carry` | 구현 |
| DV-201 | 선언은 패널 원천을 정확히 하나만 든다: 둘 다 든 선언, 관측 열쇠의 `instrument_map`, 이어지지 않는 pin은 거부된다 | `tests/application/test_research_prices.py::test_a_declaration_names_exactly_one_panel_source` | 구현 |
| DV-202 | 패널 원천이 없는 선언은 거부된다 | `tests/application/test_research_prices.py::test_a_declaration_with_neither_panel_source_is_refused` | 구현 |
| DV-203 | `present`가 아닌 bar와 공개 시점이 상한보다 늦은 head는 패널에서 빠지고 그 세션은 다른 값으로 채워지지 않는다 | `tests/application/test_research_prices.py::test_a_skipped_bar_leaves_its_session_empty_and_is_not_filled` | 구현 |
| DV-204 | canonical unadjusted가 아닌 행, `1d`가 아닌 bar, 같은 instrument·세션의 두 번째 bar는 거부된다 | `tests/application/test_research_prices.py::test_a_price_panel_refuses_rows_it_cannot_read_as_one_daily_bar_per_session` | 구현 |
| DV-205 | 가격 pin 선언은 sleeve와 composition 모두 `aas run research`로 기록되고 `aas run rerun --declaration`이 준비와 결과를 재현한다 | `tests/application/test_research_prices.py::test_a_priced_run_is_recorded_and_reproduces_from_its_declaration` | 구현 |
| DV-206 | US 가격 매퍼는 등록돼 있고 Norgate 매퍼는 asset ID로, FMP 매퍼는 FMP 심볼로 해석하며 정의하지 않은 인자를 거부한다 | `tests/storage/test_us_prices.py::test_us_price_mappers_are_registered` | 구현 |
| DV-207 | `norgate.prices_none@1`은 미국 주식 내보내기 원문을 canonical bar로 옮기고 다른 database·잘못된 날짜 행에 세션 날짜를 주지 않으며 일부 값만 있는 bar는 값 없이 `invalid`다 | `tests/storage/test_us_prices.py::test_norgate_prices_none_maps_export_text` | 구현 |
| DV-208 | `norgate.prices_adjusted@1`은 `CAPITAL`·`TOTALRETURN` binary32 part를 `split_adjusted`·`total_return` reference bar로 옮기고 다른 조정 유형과 0시가 아닌 날짜를 거부한다 | `tests/storage/test_us_prices.py::test_norgate_prices_adjusted_maps_binary32_parts` | 구현 |
| DV-209 | Norgate 기준 시리즈는 close 전용 reference이고 원문 close가 저장 값·날짜와 다르면 `invalid`이며 주식 내보내기 행은 기준 시리즈가 되지 않는다 | `tests/storage/test_us_prices.py::test_norgate_reference_series_are_close_only` | 구현 |
| DV-210 | `fmp.eod_non_split@1`의 revision N은 bar마다 수집 시각순 N번째 값 구간의 첫 응답이고 같은 시각의 다른 응답은 revision 1에만 함께 들어간다 | `tests/storage/test_us_prices.py::test_fmp_revisions_select_the_first_response_of_each_run` | 구현 |
| DV-211 | nanosecond timestamp 원천 열의 `source_row_hash`는 고정값을 재현하고 SQL과 Python에서 같다 | `tests/storage/test_us_prices.py::test_nanosecond_timestamps_hash_alike_in_sql_and_python` | 구현 |
| DV-212 | 내보내기로 확장한 US 등록에서 Norgate canonical, EODHD(교차 대조 flag), 기준 지수가 승격되고 잘린 거래량은 flag를 단다 | `tests/storage/test_us_prices.py::test_us_prices_promote_through_the_export_registry` | 구현 |
| DV-213 | Norgate 조정 part는 `float_shortest@1`로 승격되고 binary32 저장값에 `provider_float_storage` flag를 단다 | `tests/storage/test_us_prices.py::test_norgate_adjusted_parts_promote_with_float_storage_flags` | 구현 |
| DV-214 | FMP 정정은 다음 revision generation의 SUPERSEDE이며 정정을 수집한 시각부터 알려진다 | `tests/storage/test_us_prices.py::test_fmp_corrections_promote_as_later_generations` | 구현 |
| DV-215 | history 내보내기는 master에 없는 시리즈를 asset ID로 발급하고 `through` 뒤 창의 티커 주장을 만들며, 그 창에서 수집한 FMP profile은 창의 시리즈로 판단되고 모호하거나 옮겨 간 티커는 미해결이다 | `tests/storage/test_us_prices.py::test_exports_extend_ticker_claims_past_the_master` | 구현 |
| DV-216 | 부호 있는 Norgate 기준 시리즈는 `signed_series`로 찾고, 명세의 `signed`에 들면 행이 하나도 선택되지 않으며 `signed`는 증가하는 asset ID 목록이어야 한다 | `tests/storage/test_us_prices.py::test_norgate_reference_series_are_close_only` | 구현 |
| DV-217 | 같은 Norgate history 행은 `norgate.fx_history@1`과 `norgate.reference_history@1`에서 같은 값 판정을 받고 0만 다르다 | `tests/storage/test_us_prices.py::test_history_mappers_share_one_value_predicate` | 구현 |
| DV-218 | history 내보내기 가격 매퍼는 `norgate-history-csv-`가 아닌 원천 pin을 명세 단계에서 거부한다 | `tests/storage/test_us_prices.py::test_history_mappers_refuse_another_providers_source` | 구현 |
| DV-219 | 같은 시각에 수집한 서로 다른 FMP 응답은 승격에서 자연키 반복으로 거부되고, 그 bar의 뒤 정정은 revision 2에 들지 않는다 | `tests/storage/test_us_prices.py::test_fmp_tied_responses_are_refused_and_never_revised` | 구현 |
| DV-220 | 한 시리즈의 issuer 연결은 모든 티커 주장 중 가장 이른 유효 시작의 CIK이고, 다른 CIK를 대는 주장은 처리 순서와 무관하게 미해결이다 | `tests/storage/test_us_identity.py::test_a_series_links_to_the_earliest_valid_cik_of_all_its_claims` | 구현 |
| DV-221 | `calendar_compare`는 텍스트 날짜·거래량을 읽고 읽히지 않는 날짜의 행을 session이 아닌 `undated_rows`로 센다 | `tests/tools/test_calendar_compare.py::test_text_dates_and_volumes_are_read_and_undated_rows_counted` | 구현 |
| DV-222 | 여러 내보내기에 걸친 시리즈는 마지막 날짜에 닿는 원천을 근거로 삼고, master asset ID를 기준 시리즈로 내보낸 시리즈는 `export_database_differs_from_master`로 미해결이다 | `tests/storage/test_us_prices.py::test_export_series_cite_their_extent_and_respect_the_master` | 구현 |
| DV-223 | `dart.fnltt@1`은 합성 원천 fixture를 독립 기대값과 같은 발행인 재무 행으로 옮기고, 자료 없음·실패·다른 endpoint는 행을 만들지 않으며 거부되는 응답은 issuer 없는 행 하나로 남기고 `accept` grant가 있으면 뺀다 | `tests/storage/test_promotion_mappers.py::test_dart_fnltt_maps_synthetic_fixture` | 구현 |
| DV-224 | `dart.fnltt_filings@1`은 공시마다 행 하나를 내고 같은 공시의 다른 응답은 한 번만 읽으며 거부되는 응답은 issuer 없이 남긴다 | `tests/storage/test_promotion_mappers.py::test_dart_fnltt_filings_maps_synthetic_fixture` | 구현 |
| DV-225 | `aas-dimensions-v1` 형식은 고정 입력과 기대 값으로 고정돼 있고 SQL 계산이 Python과 같다 | `tests/storage/test_promotion_formats.py::test_dimensions_hash_format_is_frozen` | 구현 |
| DV-226 | SQL의 JSON 문자열 표기는 모든 code point에서 `json.dumps`와 같다 | `tests/storage/test_promotion_formats.py::test_json_string_sql_matches_json_dumps` | 구현 |
| DV-227 | instrument가 선택인 도메인은 identity snapshot 없이 승격되고, instrument가 필수인 도메인은 instrument를 해석하는 매퍼만 받는다 | `tests/storage/test_promotion_spec.py::test_an_optional_instrument_needs_no_identity_snapshot` | 구현 |
| DV-228 | 펼치는 매퍼의 재무 행은 발행인 단위로 게시되고, 자료 없음 응답은 결과 분포로 보고·manifest·품질 검사에 남으며, 같은 응답의 재승격은 빈 delta다 | `tests/storage/test_dart_promotion.py::test_statements_promote_as_issuer_fundamentals_with_coverage` | 구현 |
| DV-229 | 읽을 수 없는 DART 응답은 승격 전체를 거부한다 | `tests/storage/test_dart_promotion.py::test_an_unreadable_response_refuses_the_promotion` | 구현 |
| DV-230 | 정정 공시는 같은 줄의 record를 SUPERSEDE하고 정정 접수일부터 알려지며, 시점 읽기는 그때의 공시 하나를 head로 얻는다 | `tests/storage/test_dart_promotion.py::test_an_amendment_supersedes_the_values_it_restates` | 구현 |
| DV-231 | DART 명세 파티션은 요청의 사업연도로 원천 행을 고른다 | `tests/storage/test_dart_promotion.py::test_a_partition_selects_requests_by_business_year` | 구현 |
| DV-232 | DART 공시는 접수번호마다 행 하나이고, 한 generation에서 같은 공시의 연결·별도 응답은 한 번 읽히며 따로 승격하면 변하지 않는다 | `tests/storage/test_dart_promotion.py::test_filings_promote_one_row_per_filing` | 구현 |
| DV-233 | DART receipt의 결과는 요청·응답·줄 검사와 요청 일치 검사로 정해지고, 사업연도가 없는 행의 파티션 날짜는 null이다 | `tests/storage/test_promotion_mappers.py::test_dart_receipt_outcomes_and_partition_dates` | 구현 |
| DV-234 | `dart.fnltt@1`은 손익계산서 당기 금액을 분기로, 반기·3분기 누적 금액을 누적 기간으로, 현금흐름·자본변동을 누적으로, 재무상태표를 시점으로 옮긴다 | `tests/storage/test_promotion_mappers.py::test_dart_fnltt_reads_each_report_period` | 구현 |
| DV-235 | 재무 응답은 요청마다 가장 늦은 공시의 가장 이른 수집 하나로 읽히고, 같은 공시의 다른 bytes도 그 하나로 읽힌다 | `tests/storage/test_promotion_mappers.py::test_dart_fnltt_maps_one_response_per_request` | 구현 |
| DV-236 | 결과를 선언한 매퍼의 빈 delta는 parent version에 요청마다 하나의 `promotion_coverage@1` 검사로 결과 분포를 남기고, parent가 없으면 남기지 않는다 | `tests/storage/test_dart_promotion.py::test_coverage_without_new_rows_is_recorded_on_the_head` | 구현 |
| DV-237 | 사업연도를 읽을 수 없는 DART 요청은 모든 파티션에 세어져 승격을 거부하고, `accept` grant가 있으면 빠진 채 결과로 기록된다 | `tests/storage/test_dart_promotion.py::test_a_request_without_a_business_year_refuses_every_partition` | 구현 |
| DV-238 | 명세가 12월 결산으로 선언하지 않은 회사의 재무 응답은 `year_end_unknown`으로 거부되고 `accept` grant가 있으면 빠진 채 세어지며, 공시는 결산월 없이 승격된다 | `tests/storage/test_dart_promotion.py::test_a_year_end_the_spec_does_not_declare_is_refused` | 구현 |
| DV-239 | 한 재무제표 안에서 되풀이되는 줄은 `ord` 순서의 `occurrence`로 구별된다 | `tests/storage/test_promotion_mappers.py::test_dart_fnltt_numbers_repeated_lines_by_order` | 구현 |
| DV-240 | `sec.submissions_filings@1`은 제출자 문서와 나열된 쪽의 공시를 member·배열 순서대로 원문 값의 행으로 편입한다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_reads_every_listed_filing` | 구현 |
| DV-241 | 없는 쪽, 나열되지 않은 쪽, 공시 수가 다른 쪽은 불일치 지표이고 `expect`에 적기 전까지 대조를 실패시킨다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_count_page_discrepancies` | 구현 |
| DV-242 | 알 수 없는 배열, 길이가 다른 배열, 다른 JSON 타입의 값은 submissions 단위를 거부한다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_refuse_unknown_shapes` | 구현 |
| DV-243 | `sec.submissions@1`은 합성 원천을 독립 기대값과 같은 발행인 공시 행으로 옮기고, `filingDate`의 UTC 0시·New York 현지 0시인 접수 시각과 다른 표기는 null로 둔다 | `tests/storage/test_promotion_mappers.py::test_sec_submissions_maps_synthetic_fixture` | 구현 |
| DV-244 | `sec.companyfacts@1`은 사실마다 accession을 dimensions로 한 발행인 재무 행을 내고, 접수 시각을 pin한 공시 참조에서 accession으로 조인하며 없거나 서로 다른 접수 시각은 null이다 | `tests/storage/test_promotion_mappers.py::test_sec_companyfacts_maps_synthetic_fixture` | 구현 |
| DV-245 | SEC 공시는 기록된 접수 시각을 시점으로 승격되고 공동 제출자는 따로 record이며, 발행인 매퍼의 명세는 identity snapshot을 pin하지 않는다 | `tests/storage/test_sec_promotion.py::test_filings_take_the_recorded_acceptance_instant` | 구현 |
| DV-246 | SEC 재무는 pin한 공시 generation의 접수 시각부터 알려지고, 조인되지 않은 사실은 시점이 null이며, 같은 원천의 재승격은 빈 delta다 | `tests/storage/test_sec_promotion.py::test_facts_are_known_from_their_filing_acceptance` | 구현 |
| DV-247 | 매퍼 참조 pin은 parent의 generation이나 그 후손으로만 옮겨지고, 후손 공시 generation이 접수 시각을 준 사실은 SUPERSEDE다 | `tests/storage/test_sec_promotion.py::test_a_later_filings_generation_completes_unmatched_facts` | 구현 |
| DV-248 | 같은 accession의 바뀐 값은 SUPERSEDE이고 그 시점은 공시 접수 시각이며, 명세 파티션은 공시일로 사실을 고른다 | `tests/storage/test_sec_promotion.py::test_a_changed_value_of_one_accession_supersedes_and_partitions_select_by_filing` | 구현 |
| DV-249 | 매퍼 참조는 참조 도메인의 dataset generation만 pin할 수 있다 | `tests/storage/test_sec_promotion.py::test_a_filings_reference_must_pin_a_filings_generation` | 구현 |
| DV-250 | SEC가 두 번 나열한 같은 공시는 한 번 읽히고, 값이 다른 같은 accession의 행은 둘 다 매핑되어 승격을 거부한다 | `tests/storage/test_sec_promotion.py::test_a_repeated_listing_is_read_once` | 구현 |
| DV-251 | 제출자 문서가 자기 CIK의 `CIK##########-submissions-###.json`이 아닌 쪽을 나열하면 submissions 단위를 거부한다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_refuse_a_page_of_another_filer` | 구현 |
| DV-252 | `norgate.dividends@1`은 자본 기준 배당을 그 행의 비조정/자본 close 비율로 지급액으로 되돌리고 시리즈의 다음 세션을 ex-date로 삼으며, 마지막 행의 배당은 고르지 않고 값이 유한한 양수가 아닌(음수 포함) 배당은 `invalid`이며, 0시가 아닌 다음 행은 ex-date 없는 행으로 거부된다 | `tests/storage/test_us_actions.py::test_norgate_dividends_are_paid_cash_on_the_next_session` | 구현 |
| DV-253 | `norgate.capital_adjustments@1`은 자본 계수가 백만분의 1보다 크게 바뀐 세션을 직전/현재 계수 비율로 내고, binary32 저장만으로 생긴 움직임은 건너뛰며, 계수 없는 행을 사이에 둔 변화는 비율 없는 `invalid` 사건이고 0시가 아닌 행의 사건은 ex-date 없이 거부된다 | `tests/storage/test_us_actions.py::test_norgate_capital_adjustments_step_the_price_factor` | 구현 |
| DV-254 | `norgate.status@1`은 master 행마다 `first_date`의 상장과 `last_date` 다음 날의 상장폐지를 내고, 상장폐지는 `last_date`에서 읽으며, 거꾸로 된 날짜 쌍과 `9999-12-31` 상장폐지는 시작 없이 거부된다 | `tests/storage/test_us_actions.py::test_norgate_status_reads_listing_and_delisting_from_the_master` | 구현 |
| DV-255 | FMP 배당·분할 매퍼는 응답 revision 구간마다 첫 응답을 고르고, 한 시각에 서로 다른 값을 가진 적 있는 키는 어느 revision에서도 고르지 않으며, 분할은 유형과 관계없이 `split:<ex-date>`이고 빈 유형은 `unspecified_split`이다 | `tests/storage/test_us_actions.py::test_fmp_actions_select_each_run_and_never_a_tied_key` | 구현 |
| DV-256 | Norgate 기업행동은 파티션 없이 `exdate_open@1`로 승격되고(파티션을 둔 명세는 거부), 유도 분할조정·총수익 가격은 그 generation과 canonical 가격에서 나온다 | `tests/storage/test_us_actions.py::test_norgate_actions_promote_and_adjust_canonical_prices` | 구현 |
| DV-257 | 쓸 수 없는 기업행동(직전 세션 bar가 `present`가 아닌 배당 포함) 앞의 유도 bar는 값 없이 `invalid`이고 `unadjustable_action`을 달며, 분할조정은 배당을 읽지 않고 읽은 bar 밖의 행동은 쓰지 않는다 | `tests/storage/test_us_actions.py::test_adjustment_marks_bars_before_an_unadjustable_action` | 구현 |
| DV-258 | grant 없는 규칙이나 시점 근거 없음으로 held된, cutoff가 아는 기업행동 앞의 유도 bar는 `invalid`이고 그 이유를 달며, 영수증은 막은 규칙을 싣는다 | `tests/storage/test_read_heads.py::test_adjustment_marks_bars_before_a_withheld_action` | 구현 |
| DV-259 | 격자 읽기도 배당을 격자의 앞선 날짜가 아니라 ex-date 직전 세션 close에 재투자하고, 영수증은 격자 읽기와 유도에 쓴 읽기의 hash를 따로 싣는다 | `tests/storage/test_read_heads.py::test_adjustment_reinvests_at_the_session_before_the_exdate_under_a_grid` | 구현 |
| DV-260 | 격자가 말하는 ex-date 직전 세션에 bar가 없으면 배당을 쓸 수 없다 | `tests/storage/test_read_heads.py::test_adjustment_needs_a_close_on_the_session_before_the_exdate` | 구현 |
| DV-261 | `aas-adjusted-read-v1` 영수증은 같은 읽기에 같은 hash이고, 행동 집합이 바뀌면 `rows_hash`가 바뀌며, 하위 읽기의 hash를 싣는다 | `tests/storage/test_read_heads.py::test_adjusted_receipt_pins_the_reads_and_the_rows` | 구현 |
| DV-262 | `norgate.status@1`과 `fmp.dividends@1`은 승격을 거쳐 `local_day_end@1`과 XNYS `exdate_open@1` 시각으로 저장된다 | `tests/storage/test_us_actions.py::test_norgate_status_and_fmp_actions_promote` | 구현 |
| DV-263 | OpenDART 요청 지문은 endpoint와 parameter만 해시하고 legacy 요청의 관측일을 무시한다 | `tests/data/test_opendart.py::test_request_fingerprint_names_the_question_without_its_observation_date` | 구현 |
| DV-264 | 응답 결과는 수집 경로만 정하고 공급자 상태를 남기며, 키·한도 거부는 실행을 멈춘다. 상장회사로 읽히지 않는 corp code 답, 요청한 page·날이 아닌 목록 page, 1,000 page를 넘게 세는 목록, key를 되풀이하는 JSON은 `FAILED`다 | `tests/data/test_opendart.py::test_outcomes_route_the_collector_and_keep_the_provider_status` | 구현 |
| DV-265 | 보고서는 12월 결산 기간이 끝나면 cohort에 들어오고 최신 기간부터 묻는다 | `tests/data/test_opendart_cohort.py::test_a_quarter_enters_the_cohort_when_its_period_ends` | 구현 |
| DV-266 | `NO_DATA`는 종결이 아니며 시즌 안에서는 7일, 그 뒤에는 직전 사업연도까지 90일마다 다시 묻는다 | `tests/data/test_opendart_cohort.py::test_no_data_is_asked_again_in_season_weekly_and_after_it_quarterly` | 구현 |
| DV-267 | 마지막 수집일 이후의 공시(늦은 제출·정정)는 그 요청을 다시 묻게 한다 | `tests/data/test_opendart_cohort.py::test_a_filing_on_or_after_the_last_ask_asks_again` | 구현 |
| DV-268 | 별도 재무제표 요청은 연결 요청의 마지막 답이 `NO_DATA`인 동안만 묻는다 | `tests/data/test_opendart_cohort.py::test_the_separate_statement_follows_a_consolidated_no_data` | 구현 |
| DV-269 | 실패했거나 답을 보존하지 못한 요청은 하루 뒤 다시 묻는다 | `tests/data/test_opendart_cohort.py::test_failed_and_unanswered_asks_wait_a_day` | 구현 |
| DV-270 | 회사 목록은 KIND 목록으로 좁혀지고 유가증권·코스닥 정기공시 회사로 넓혀진다 | `tests/data/test_opendart_cohort.py::test_the_universe_is_narrowed_by_kind_and_widened_by_listed_filers` | 구현 |
| DV-271 | 공시 목록의 날은 그 날이 끝난 뒤 받은 첫 page와 그 page가 센 모든 page로만 덮이고, 실패한 page는 하루 뒤 다시 묻는다 | `tests/data/test_opendart_cohort.py::test_list_days_are_covered_only_by_answers_after_the_day_ended` | 구현 |
| DV-272 | legacy 원장 재생은 고정 cohort가 묻지 않은 분기와 `NO_DATA` 뒤의 별도 재무제표를 계획한다 | `tests/data/test_opendart_cohort.py::test_the_legacy_ledger_replay_plans_the_quarter_its_fixed_cohort_never_asks` | 구현 |
| DV-273 | 호출은 `reserved` attempt와 usage event로 먼저 기록되고 답을 보존한 뒤 receipt 해시로 정산된다 | `tests/storage/test_collection_ledger.py::test_a_call_is_reserved_before_it_starts_and_settled_after` | 구현 |
| DV-274 | 중단된 attempt는 호출 전이면 `released`, 호출 뒤면 `uncertain`이 되고 성공이나 미호출로 바뀌지 않는다 | `tests/storage/test_collection_ledger.py::test_recovery_never_turns_an_interrupted_call_into_a_success_or_a_non_call` | 구현 |
| DV-275 | quota는 창 안에서 해제되지 않은 모든 예약을 센다 | `tests/storage/test_collection_ledger.py::test_the_quota_counts_every_unreleased_reservation_in_the_window` | 구현 |
| DV-276 | 수집 실행은 corp code·공시 목록·재무 순으로 묻고, batch 단위 `opendart-receipts` 원천을 DART 매퍼가 읽으며, 키는 보존되지 않는다 | `tests/storage/test_kr_collection.py::test_a_run_asks_by_phase_and_commits_receipts_the_dart_mappers_read` | 구현 |
| DV-277 | 다음 실행은 commit된 답으로 할 일을 정하고 끝난 요청을 다시 묻지 않는다 | `tests/storage/test_kr_collection.py::test_the_next_day_asks_what_the_answers_made_due_and_nothing_else` | 구현 |
| DV-278 | 일일 quota는 실행을 넘어 원장으로 세어진다 | `tests/storage/test_kr_collection.py::test_the_daily_quota_counts_the_ledger_across_runs` | 구현 |
| DV-279 | 중단된 실행의 attempt는 정산되고, 보존된 receipt는 다음 실행이 계획 전에 알고 먼저 commit하며 그 요청을 다시 묻지 않는다 | `tests/storage/test_kr_collection.py::test_an_interrupted_run_is_settled_and_its_receipts_committed_next` | 구현 |
| DV-280 | 키·한도 거부는 실행을 멈추고 그 답을 commit한다 | `tests/storage/test_kr_collection.py::test_a_refused_key_stops_the_run_and_keeps_the_answer` | 구현 |
| DV-281 | 전송 실패는 `uncertain`이며 세 번 이어지면 실행을 멈추고 quota에 세어진다 | `tests/storage/test_kr_collection.py::test_transport_failures_are_uncertain_and_stop_after_three` | 구현 |
| DV-282 | KIND 목록 수집은 `kind-listings` 원천으로 commit되고 cohort의 회사를 좁힌다 | `tests/storage/test_kr_collection.py::test_kind_lists_commit_as_listing_sources_and_narrow_the_cohort` | 구현 |
| DV-283 | 상장법인목록 표가 아닌 KIND 답은 거부를 보고하고 원천이 되지 않는다 | `tests/storage/test_kr_collection.py::test_a_kind_answer_that_is_not_the_listing_table_is_refused` | 구현 |
| DV-284 | 한 `opendart-receipts` 원천에는 완료된 corp code 답이 많아야 하나다 | `tests/storage/test_kr_collection.py::test_a_batch_holds_at_most_one_completed_corp_code_list` | 구현 |
| DV-285 | marker는 commit됐지만 완료되지 않은 batch는 다음 실행이 완료하고 그 receipt를 다시 commit하지 않는다 | `tests/storage/test_kr_collection.py::test_a_commit_left_without_its_completion_is_finished_not_committed_again` | 구현 |
| DV-286 | legacy `opendart-native` receipts 테이블의 세 형태(`raw_json`, `raw_base64`, 검증 결과)는 모두 계획에 읽힌다 | `tests/storage/test_kr_collection.py::test_legacy_receipts_tables_of_every_shape_are_read` | 구현 |
| DV-287 | `aas-opendart-receipt-v1`, `aas-opendart-batch-v1`, `aas-kind-receipt-v1` 형식은 고정 입력과 기대 digest로 고정돼 있다 | `tests/storage/test_kr_collection.py::test_receipt_batch_and_kind_receipt_formats_are_frozen` | 구현 |
| DV-288 | KIND 목록의 코드 하나라도 KRX 단축코드가 아니면 cohort를 좁히지 않는다 | `tests/storage/test_kr_collection.py::test_a_kind_list_with_a_malformed_code_never_narrows_the_cohort` | 구현 |
| DV-289 | 응답 없는 완료 공시 목록 행은 읽히지 않은 행이고 그 날을 덮지 않는다 | `tests/storage/test_kr_collection.py::test_a_completed_list_row_without_its_page_is_unreadable` | 구현 |
| DV-290 | batch는 응답 bytes 상한에서도 끝난다 | `tests/storage/test_kr_collection.py::test_a_batch_ends_at_its_byte_budget` | 구현 |
| DV-291 | 완료된 Qveris job 하나는 행 원천과 보류 원천으로 commit되고 둘은 원본 bytes의 `hex`를 공유하며 매퍼가 lineage 접두어와 테이블 이름으로 찾는다 | `tests/storage/test_qveris_import.py::test_one_job_commits_its_rows_and_held_rows_under_one_content_hex` | 구현 |
| DV-292 | 같은 job을 다시 적재하면 재사용하고 적재 코드만 바뀌어도 같은 원천 ID다 | `tests/storage/test_qveris_import.py::test_reimport_reuses_and_a_code_change_keeps_the_id` | 구현 |
| DV-293 | identity 문서가 커지거나 바뀌어도 적재된 job은 같은 원천으로 재사용되고 identity 해시는 lineage에 남는다 | `tests/storage/test_qveris_import.py::test_an_identity_document_that_grows_keeps_every_imported_source` | 구현 |
| DV-294 | 경고가 붙은 내려받기는 빈 행 테이블과 `provider_reported_partial` 보류 행으로 적재되고 적재를 막지 않는다 | `tests/storage/test_qveris_import.py::test_a_warned_download_commits_an_empty_rows_table_and_its_held_rows` | 구현 |
| DV-295 | 읽을 수 없는 job은 기록되고 나머지 job은 적재된다 | `tests/storage/test_qveris_import.py::test_unreadable_jobs_are_recorded_and_the_run_continues` | 구현 |
| DV-296 | 일간 요청은 선언 달력의 열린 세션에서 나오고 관측일과 무관하게 완료된 요청은 빠진다 | `tests/application/test_qveris_cli.py::test_daily_jobs_follow_declared_sessions_and_skip_completed_requests` | 구현 |
| DV-297 | 완료 없이 시도만 있는 요청은 `held`로 보고되고 계획되지 않는다 | `tests/application/test_qveris_cli.py::test_an_attempt_without_a_completion_is_held_not_planned` | 구현 |
| DV-298 | 유료 호출 한도에 닿은 실행은 시도 없이 `budget_exhausted`로 끝나고 다음 실행은 완료된 job을 다시 호출하지 않는다 | `tests/application/test_qveris_cli.py::test_run_stops_at_the_paid_call_limit_and_resumes_without_repeating` | 구현 |
| DV-299 | 결과가 불확실한 유료 호출은 수집을 멈추고 종료 코드 2를 낸다 | `tests/application/test_qveris_cli.py::test_run_stops_with_exit_two_when_a_paid_call_is_uncertain` | 구현 |
| DV-300 | 병렬 group은 예산에 전부 예약되거나 하나도 예약되지 않고, 거부된 group은 실행되지 않는다 | `tests/data/test_qveris_parallel.py::test_group_is_reserved_whole_or_not_at_all` | 구현 |
| DV-301 | 공급자 경고는 완료로 세어지고 cohort를 멈추지 않는다 | `tests/data/test_qveris_batch.py::test_a_provider_warning_completes_and_the_cohort_continues` | 구현 |
| DV-302 | 분할 비율은 공급자 텍스트로 남고 identity가 없거나 양수가 아닌 비율은 보류된다 | `tests/data/test_qveris_actions_fx.py::test_splits_keep_the_ratio_text_and_hold_unknown_identities` | 구현 |
| DV-303 | 통화쌍 이력은 identity 없이 쌍과 두 통화를 남기고 맞지 않는 OHLC는 보류된다 | `tests/data/test_qveris_actions_fx.py::test_forex_history_keeps_the_pair_and_holds_bad_rows` | 구현 |
| DV-304 | 요청 시작 간격과 HTTP 시도·시간 한도는 coordinator와 작업자에 공유되고 한도는 새 page의 첫 요청만 거부한다 | `tests/data/test_qveris_pacing.py::test_admission_bounds_requests_and_time_across_shared_clients` | 구현 |
| DV-305 | HTTP 시도 수 한도가 어느 요청 위치에 걸려도 종료 코드 0인 실행은 미정산 page나 group을 남기지 않고 다음 실행이 모두 완료한다 | `tests/application/test_qveris_cli.py::test_a_request_limit_at_any_position_never_strands_a_started_page` | 구현 |
| DV-306 | 시간 한도가 어느 요청 위치에 걸려도 종료 코드 0인 실행은 미정산 page나 group을 남기지 않는다 | `tests/application/test_qveris_cli.py::test_a_time_limit_at_any_position_never_strands_a_started_page` | 구현 |
| DV-307 | 미정산 증거가 있는 예산 거부는 깨끗한 예산 소진이 아니다 | `tests/data/test_qveris_batch.py::test_a_budget_code_with_unresolved_evidence_is_not_a_clean_stop` | 구현 |
| DV-308 | `aas collect qveris quarantine`은 page나 group을 예약을 남긴 채 격리하고 계정의 새 실행을 다시 허용한다 | `tests/application/test_qveris_cli.py::test_quarantine_releases_the_account_and_keeps_the_reservation` | 구현 |
| DV-309 | 고정 합성 job의 정규화 행과 보류 행 digest는 고정값과 같다 | `tests/storage/test_qveris_import.py::test_normalized_rows_match_the_pinned_legacy_shape` | 구현 |
| DV-310 | 정산된 뒤 응답 형태가 맞지 않는 job은 알려진 실패로 세고 cohort는 계속된다 | `tests/data/test_qveris_batch.py::test_a_settled_malformed_payload_is_a_known_failure_and_the_cohort_continues` | 구현 |
| DV-311 | 시간 한도는 공유 간격을 기다린 뒤 확인된다 | `tests/data/test_qveris_pacing.py::test_the_deadline_is_checked_after_waiting_for_the_shared_pacer` | 구현 |
| DV-312 | 병렬 응답이 job의 bytes 상한을 넘으면 보존하지 않고 실패로 정산된다 | `tests/data/test_qveris_parallel.py::test_a_parallel_response_over_the_job_byte_bound_is_never_retained` | 구현 |
| DV-313 | 인증·quota 응답으로 멈춘 병렬 group은 정산된 완료와 호출을 센 뒤 멈춘다 | `tests/data/test_qveris_parallel_batch.py::test_a_provider_stop_counts_the_settled_group_before_stopping` | 구현 |
| DV-314 | 손상된 완료 문서는 `unreadable_completion`으로 보고되고 다른 job은 적재된다 | `tests/storage/test_qveris_import.py::test_a_damaged_completion_marker_is_reported_and_other_jobs_import` | 구현 |
| DV-315 | `plan`은 `run`이 재사용하는 같은 fingerprint만 완료로 세고 관측일이 다른 같은 요청은 `equivalent`로 센다 | `tests/application/test_qveris_cli.py::test_plan_counts_only_jobs_run_reuses_as_completed` | 구현 |
| DV-316 | revision 읽기를 cutoff로 투영하면 grant·flag 제외·cutover·수집 cutoff·연구 지식 상한 아래에서 같은 cutoff의 `read_heads`와 같은 head가 나온다 | `tests/storage/test_read_heads.py::test_revision_projection_matches_read_heads` | 구현 |
| DV-317 | `aas-head-revisions-v1` 영수증 형식은 고정돼 있고, cutoff·격자·자연키가 아닌 필터를 가진 revision 읽기와 할당을 넘는 읽기는 거부된다 | `tests/storage/test_read_heads.py::test_revision_read_receipt_format_and_refusals` | 구현 |
| DV-318 | 비조정 bar와 분할에서 판단 cutoff로 유도한 canonical 신호는 공급자 조정 reference와 같은 판단을 내고, 체결은 비조정 bar를 읽는다 | `tests/application/test_strict_head_inputs.py::test_derived_canonical_signal_decides_like_the_native_reference_series` | 구현 |
| DV-319 | grant가 없는 가격 규칙은 strict 준비에서 bar를 막고 연구 준비에서는 막지 않는다 | `tests/application/test_strict_head_inputs.py::test_an_ungranted_price_rule_withholds_strict_bars_but_not_research` | 구현 |
| DV-320 | grant가 막은, cutoff가 아는 기업행동은 조용히 빠지지 않고 그 앞의 bar를 신호에서 뺀다 | `tests/application/test_strict_head_inputs.py::test_a_known_action_withheld_by_its_grant_is_never_dropped` | 구현 |
| DV-321 | 판단 뒤에 알려진 기업행동은 그 판단의 신호를 조정하지 않는다 | `tests/application/test_strict_head_inputs.py::test_an_action_known_after_a_decision_does_not_adjust_it` | 구현 |
| DV-322 | grant가 없는 달력 규칙의 세션은 strict 준비가 알지 못한다 | `tests/application/test_strict_head_inputs.py::test_an_ungranted_calendar_knows_no_session` | 구현 |
| DV-323 | native 가격 generation은 `heads` 달력과 함께 묶이지 않는다 | `tests/application/test_strict_head_inputs.py::test_a_native_price_generation_needs_a_sessions_generation` | 구현 |
| DV-324 | `heads`로 묶은 거시 series는 같은 값의 native generation과 같은 신호를 내고, 단위가 다르거나 cutoff까지 head가 없으면 거부된다 | `tests/application/test_strict_head_inputs.py::test_a_head_bound_macro_series_decides_like_its_native_generation` | 구현 |
| DV-325 | FX 고시는 `BASE/QUOTE` series로 UTC 날짜에 읽히고 그 단위는 호가 통화다 | `tests/application/test_strict_head_inputs.py::test_a_head_bound_fx_fixing_is_a_macro_series` | 구현 |
| DV-326 | 요청의 `heads` 참조 hash와 reader의 binding hash는 같은 정체성이고 정규 표기가 아닌 문서는 거부된다 | `tests/application/test_strict_head_inputs.py::test_head_binding_hash_is_one_identity_across_request_and_reader` | 구현 |
| DV-327 | 요청은 `heads` 참조를 역할의 도메인으로만 받고, hash 불일치·비정렬 grant·빈틈 있는 구간을 거부하며, `actions` binding은 유도 canonical 신호 선택마다 정확히 하나다 | `tests/application/test_strict_head_inputs.py::test_the_request_admits_head_references_only_as_their_roles_allow` | 구현 |
| DV-328 | cutover가 있는 체결 binding은 구간마다 자기 pin의 bar를 읽는다 | `tests/application/test_strict_head_inputs.py::test_a_cutover_reads_each_interval_from_its_own_pin` | 구현 |
| DV-329 | 규칙 시점 binding의 읽기 영수증은 grant가 허용한 규칙을 싣고 막은 규칙이 없다 | `tests/application/test_strict_head_inputs.py::test_head_reads_record_the_rules_their_grants_apply` | 구현 |
| DV-330 | 기업행동 근거가 없는 instrument는 `head_reads`의 `no_action_source`에 실리고, strict 준비는 `absent_actions_as_none@1` grant 없이 그것을 읽지 않는다 | `tests/application/test_strict_head_inputs.py::test_an_absent_action_source_is_recorded_and_strict_needs_its_grant` | 구현 |
| DV-331 | 유도 신호 선택은 binding 순서대로 자기 `actions` binding을 ordinal로 받는다 | `tests/application/test_strict_head_inputs.py::test_each_derived_selection_takes_its_own_actions_binding` | 구현 |
| DV-332 | 세션이 그 끝 시각부터 알려진 `heads` 달력에서는 판단 cutoff가 다음 session을 몰라 strict 준비가 미해결 월로 거부한다 | `tests/application/test_strict_head_inputs.py::test_a_strict_schedule_needs_the_next_session_known_at_its_cutoff` | 구현 |
| DV-333 | `heads` 달력은 판단마다 그 cutoff에 알려진 revision으로 투영되어, 두 판단 사이에 알려진 개정은 뒤 판단에만 닿는다 | `tests/application/test_strict_head_inputs.py::test_each_decision_projects_the_head_calendar_its_cutoff_knows` | 구현 |
| DV-334 | `heads` 신호 bar와 목표는 identity·universe pin이 cutoff 기준으로 담는 instrument만 쓴다 | `tests/application/test_strict_head_inputs.py::test_head_signals_and_targets_are_held_by_the_membership_pins` | 구현 |
| DV-335 | bundle의 `heads` 참조는 `raw/`에 남은 정규 binding 문서로만 다시 검증되고, 없거나 바뀌었거나 marker와 맞지 않는 binding은 거부된다 | `tests/application/test_strict_head_inputs.py::test_a_head_binding_is_verified_again_only_from_its_retained_document` | 구현 |
| DV-336 | 선언 시각 이전 기간의 strict 일정은 owner가 정한 달력 사전 지식 규칙의 grant로 만들어지고 run이 그 grant를 기록한다 | `tests/application/test_strict_head_inputs.py::test_a_granted_calendar_knowledge_rule_schedules_history` | 예정 |
| DV-337 | 요청 지문은 공급자·endpoint·parameter만 해시하고 FRED 키는 공급자 URL에만 실리며 키를 되돌리는 답은 보존되지 않는다 | `tests/data/test_fred_collect.py::test_a_request_names_its_window_and_never_its_key` | 구현 |
| DV-338 | FRED 답은 읽히면 `COMPLETED`, 없는 시계열은 `NO_DATA`, 창 밖 행이나 다른 모양은 `FAILED`이고 키 거부와 한도는 실행을 멈춘다 | `tests/data/test_fred_collect.py::test_answers_are_classified_and_a_refused_key_stops_the_run` | 구현 |
| DV-339 | observations 창은 vintage 날짜를 1990개까지 담고 다음 창은 앞 창의 마지막 vintage 날짜에서 시작한다 | `tests/data/test_fred_collect.py::test_windows_span_at_most_1990_vintages_and_chain_on_their_last_vintage` | 구현 |
| DV-340 | 원점이 아닌 창의 시작일에 시작하는 행은 이미 가진 구간의 재진술로 세어지고 옮겨지지 않는다 | `tests/data/test_fred_collect.py::test_rows_starting_on_a_window_start_restate_what_is_held` | 구현 |
| DV-341 | 계획은 알려진 vintage 날 다음 날부터 끝난 FRED 날까지 vintage를 확인하고 알려진 날이 없으면 원점부터 묻으며 CSV는 FRED 날마다 한 번이다 | `tests/data/test_fred_collect.py::test_the_plan_checks_vintages_after_the_known_day_and_csv_once_a_day` | 구현 |
| DV-342 | 원점 수집은 잘린 재진술 없이 FRED가 매긴 구간을 한 번씩만 1990개 이하의 창으로 옮긴다 | `tests/storage/test_us_collection.py::test_an_origin_run_collects_every_vintage_once_in_windows_of_at_most_1990` | 구현 |
| DV-343 | 날마다의 수집과 승격에서 승격 watermark와 수집기의 시계열별 vintage 날은 앞으로만 가고, 새 vintage가 없으면 그대로다 | `tests/storage/test_us_collection.py::test_the_watermark_advances_monotonically_across_daily_collections` | 구현 |
| DV-344 | 완결되지 않은 창은 옮겨지지 않고 다음 실행이 같은 알려진 날에서 다시 묻는다 | `tests/storage/test_us_collection.py::test_an_incomplete_window_is_asked_again_from_the_same_known_day` | 구현 |
| DV-345 | 중단된 FRED 실행의 attempt는 정산되고 보존된 receipt는 다음 실행이 먼저 commit하며 완결된 창은 알려진 것이 된다 | `tests/storage/test_us_collection.py::test_a_crashed_run_is_settled_and_its_receipts_committed_by_the_next` | 구현 |
| DV-346 | FRED 키 거부는 실행을 멈추고 그 답을 commit한다 | `tests/storage/test_us_collection.py::test_a_refused_key_stops_the_run_and_keeps_the_answer` | 구현 |
| DV-347 | `fred.alfred@1`이 읽는 legacy 테이블도 알려진 vintage 날을 정하고, 받은 FRED 날에 시작한 vintage는 세지 않는다 | `tests/storage/test_us_collection.py::test_a_legacy_alfred_table_sets_the_known_vintage_day` | 구현 |
| DV-348 | CSV 내려받기는 `fred.series_csv@1` 편입과 같은 원천이고 `fred.fx_series@1`로 승격된다 | `tests/storage/test_us_collection.py::test_the_csv_download_is_the_source_the_legacy_import_makes` | 구현 |
| DV-349 | SEC 요청은 문서를 이름 짓고 연락처 `User-Agent`는 header에만 실리며 그것이나 연락처 주소를 되돌리는 답은 보존되지 않는다 | `tests/data/test_sec_collect.py::test_requests_name_the_document_and_never_the_contact` | 구현 |
| DV-350 | 일일 색인은 모든 줄을 남기고 다섯 칸으로 읽힌 줄만 공시가 되며 header는 열 이름(대소문자·공백 무시, `File Name`·`Filename`)으로 찾고 경로의 CIK가 다른 줄은 공시가 아니며 404는 `NO_DATA`, 403은 실행을 멈춘다 | `tests/data/test_sec_collect.py::test_a_daily_index_keeps_every_line_and_reads_the_filings` | 구현 |
| DV-351 | companyfacts 사실은 숫자의 JSON 원문을 그대로 남기고 다른 CIK·되풀이된 key·알 수 없는 필드·십진수가 아닌 값은 답을 거부한다 | `tests/data/test_sec_collect.py::test_company_facts_keep_each_number_as_written` | 구현 |
| DV-352 | 색인 날은 답이나 다음 날이 끝난 뒤 받은 404로만 덮이고 빈 날은 알려진 가장 이른 날부터 채운다 | `tests/data/test_sec_collect.py::test_index_days_are_covered_by_an_answer_or_a_404_after_the_next_day` | 구현 |
| DV-353 | 문서는 제출자마다 원한 공시로 한 번 묻고 나열되지 않은 공시는 창 안에서 하루 뒤 다시 묻으며 발행인 범위 밖은 세어진다 | `tests/data/test_sec_collect.py::test_documents_are_asked_per_filer_for_the_wanted_filings_and_retried_in_window` | 구현 |
| DV-354 | SEC 실행은 색인 뒤 문서를 묻고 원한 공시의 행만 commit하며 `sec.submissions@1`·`sec.companyfacts@1`이 그 원천을 승격한다 | `tests/storage/test_us_collection.py::test_a_run_reads_indexes_then_documents_and_the_sec_mappers_promote_them` | 구현 |
| DV-355 | 다음 SEC 실행은 덮이지 않은 날과 아직 없는 공시만 묻는다 | `tests/storage/test_us_collection.py::test_the_next_run_asks_only_what_is_missing` | 구현 |
| DV-356 | `registered` 범위는 identity에 SEC 발행인이 등록된 제출자의 문서만 묻는다 | `tests/storage/test_us_collection.py::test_registered_issuers_limit_the_documents` | 구현 |
| DV-357 | SEC의 한도 거부는 실행을 멈추고 연락처는 `raw/`의 어떤 bytes에도 남지 않는다 | `tests/storage/test_us_collection.py::test_a_refused_rate_stops_the_run_and_the_contact_is_never_retained` | 구현 |
| DV-358 | `aas collect fred plan`과 `aas collect sec plan`은 공급자를 호출하지 않고 `run`은 호출 상한에서 정상 종료한다 | `tests/storage/test_us_collection.py::test_the_commands_plan_without_calls_and_run_through_the_cli` | 구현 |
| DV-359 | 행으로 읽히지 않는 submissions 답은 `FAILED`이고 실행이나 commit을 막지 않는다 | `tests/storage/test_us_collection.py::test_a_submissions_answer_whose_rows_do_not_read_is_failed` | 구현 |
| DV-360 | receipts 원천을 commit하기 전에 중단된 FRED batch는 다음 실행이 통째로 다시 commit하고 이미 commit된 파생 원천을 재사용해 CSV 날과 vintage를 잃지 않는다 | `tests/storage/test_us_collection.py::test_a_batch_whose_receipts_did_not_commit_is_derived_again_whole` | 구현 |
| DV-361 | receipts 원천을 commit하기 전에 중단된 SEC batch의 색인은 다음 실행에서 알려지고 그 색인이 말하는 모든 문서를 묻는다 | `tests/storage/test_us_collection.py::test_an_sec_batch_whose_receipts_did_not_commit_loses_no_filing` | 구현 |
| DV-362 | 고아 receipt는 실행의 batch 한도로 나뉘어 commit된다 | `tests/storage/test_us_collection.py::test_orphans_are_committed_in_batches_of_the_run_bounds` | 구현 |
| DV-363 | 복구 batch는 질의 사이에서만 닫혀 한 observations 질의의 page를 한 batch에 둔다 | `tests/storage/test_us_collection.py::test_a_recovered_batch_closes_only_between_queries` | 구현 |
| DV-364 | 전송 실패가 세 번 이어지면 `transport_failures`로 멈추고 명령은 종료 코드 1이다 | `tests/storage/test_us_collection.py::test_three_transport_failures_in_a_row_stop_the_run` | 구현 |
| DV-365 | 시계열은 처음 완결되지 않은 창에서 멈추고 앞 창까지만 commit하며 다음 실행은 그 창의 마지막 vintage에서 다시 묻는다 | `tests/storage/test_us_collection.py::test_a_series_stops_at_its_first_incomplete_window_and_resumes_there` | 구현 |
| DV-366 | receipt·batch 문서와 receipts 원천 ID는 문서화된 정규 bytes로 고정된다 | `tests/storage/test_us_collection.py::test_receipts_and_batches_have_fixed_canonical_bytes` | 구현 |
| DV-367 | 답을 보존하지 못한 SEC 문서 요청은 물은 것으로 보아 같은 날 다시 묻지 않고 하루 뒤 다시 묻는다 | `tests/storage/test_us_collection.py::test_an_uncertain_document_ask_is_asked_again_the_next_day` | 구현 |
| DV-368 | 행이 읽히지 않는 CSV 답은 `FAILED`이고 그 FRED 날을 덮지 않아 다음 실행이 다시 받는다 | `tests/storage/test_us_collection.py::test_a_csv_download_whose_rows_do_not_read_is_failed_and_asked_again` | 구현 |
| DV-369 | pyarrow가 없으면 US 수집은 원장이나 공급자 호출 전에 멈춘다 | `tests/storage/test_us_collection.py::test_a_run_without_pyarrow_stops_before_any_call` | 구현 |
| DV-370 | `strategy promote --plan`은 원천의 모든 레코드를 세고 요구를 dataset ID별로 state 카탈로그와 대조하며 아무것도 쓰지 않는다 | `tests/storage/test_strategy_registry.py::test_plan_reports_every_record_and_reconciles_requirements_with_the_catalog` | 구현 |
| DV-371 | 요구 사상표 v1은 가격·현금·벤치마크·거시·환율 입력을 모두 행으로 만들고 목록에 없는 자산의 고정 비중을 거부한다 | `tests/storage/test_strategy_registry.py::test_requirement_map_derives_every_named_input` | 구현 |
| DV-372 | `--apply`는 계획과 같은 전략 집합을 등록하고 같은 원천의 재실행은 재사용이다 | `tests/storage/test_strategy_registry.py::test_apply_registers_the_planned_set_and_a_repeat_is_reused` | 구현 |
| DV-373 | 정의 버전은 내용 hash이고, 바뀐 레코드는 새 버전이며 저장된 정의·요구·등록 행은 바뀌거나 지워지지 않는다 | `tests/storage/test_strategy_registry.py::test_definition_versions_are_immutable` | 구현 |
| DV-374 | 원천 의존 테이블과 요청이 어긋나면 계획이 보고하고 등록은 아무것도 쓰지 않고 거부한다 | `tests/storage/test_strategy_registry.py::test_a_dependency_table_that_disagrees_is_reported_and_refused` | 구현 |
| DV-375 | 원천 형식이 아니거나 SHA-256이 다른 원천은 거부된다 | `tests/storage/test_strategy_registry.py::test_a_source_of_another_shape_or_hash_is_refused` | 구현 |
| DV-376 | marker가 commit된 등록 intent는 원천을 다시 읽지 않고 복구된다 | `tests/storage/test_strategy_registry.py::test_an_interrupted_registration_is_recovered_from_its_marker` | 구현 |
| DV-377 | 저장된 요구 행이 정의에서 다시 유도한 행과 다르면 검증이 실패한다 | `tests/storage/test_strategy_registry.py::test_a_changed_definition_fails_verification` | 구현 |
| DV-378 | `aas strategy promote`와 `aas strategy definitions`는 계획·등록·조회·검증을 CLI로 끝낸다 | `tests/storage/test_strategy_registry.py::test_strategy_promote_cli_plans_applies_and_lists` | 구현 |
| DV-379 | 전략 레지스트리 등록이 가리키는 원천은 은퇴 대상에서 참조로 세어 거부된다 | `tests/storage/test_source_retirement.py::test_a_source_a_strategy_registration_names_is_referenced` | 구현 |
| DV-380 | 요청 통화와 다른 시장의 `mapped` 가격이 있으면 `/exchange` `fx` 요구 행이 생기고, 통화가 없거나 모르는 통화면 이유와 함께 `unmapped`다 | `tests/storage/test_strategy_registry.py::test_requirement_map_records_the_conversion_a_request_currency_needs` | 구현 |
| DV-381 | 정의 문서 hash·버전, 등록 요청 hash·operation ID, payload hash는 고정된 기대 digest를 가진다 | `tests/storage/test_strategy_registry.py::test_definition_and_registry_request_formats_are_frozen` | 구현 |
| DV-382 | `strategy_registry_schema` v1 checksum은 기록된 값과 같다 | `tests/storage/test_strategy_registry.py::test_the_registry_schema_checksum_is_recorded` | 구현 |
| DV-383 | 같은 전략 ID가 두 번 나오는 원천, 원천에 없는 전략의 의존 행, 문자열이 아닌 `token_kind`는 아무것도 쓰지 않고 거부된다 | `tests/storage/test_strategy_registry.py::test_a_source_whose_records_do_not_hold_together_writes_nothing` | 구현 |
| DV-384 | 저장된 정의 문서의 bytes·hash·제목이 바뀌면 검증이 실패한다 | `tests/storage/test_strategy_registry.py::test_a_tampered_definition_document_fails_verification` | 구현 |
| DV-385 | intent 없는 marker와 marker 없는 COMPLETED intent는 검증이 거부한다 | `tests/storage/test_strategy_registry.py::test_markers_and_intents_are_verified_in_both_directions` | 구현 |
| DV-386 | 등록 intent는 격리되지 않고, marker 없이 남은 intent는 같은 `--apply`가 끝내며 그 전까지 등록 수에 들지 않는다 | `tests/storage/test_strategy_registry.py::test_an_intent_without_a_marker_is_finished_by_applying_again` | 구현 |
| DV-387 | FX 변환은 날짜 이하의 가장 늦은 고시를 쓴다 | `tests/engine/test_fx_conversion.py::test_a_value_takes_the_latest_fixing_on_or_before_its_date` | 구현 |
| DV-388 | 더 늦은 고시나 선언한 나이를 넘긴 고시는 값을 환산하지 않는다 | `tests/engine/test_fx_conversion.py::test_no_later_fixing_and_no_fixing_past_its_age_converts_a_value` | 구현 |
| DV-389 | 계좌 통화가 아닌 가격 선택은 `fx_conversions` grant로만 받고, grant가 없는 요청은 열쇠가 없어 바이트가 그대로다 | `tests/engine/test_backtest_request.py::test_a_foreign_price_currency_is_admitted_by_its_fx_conversion_grant` | 구현 |
| DV-390 | FX grant는 통화·binding·series·신호 통화가 정확해야 하고 빈 목록, 쓰이지 않는 통화, 짝 없는 binding을 거부한다 | `tests/engine/test_backtest_request.py::test_an_fx_conversion_grant_is_exact` | 구현 |
| DV-391 | 혼합 통화 strict 전략은 grant한 FX pin으로 실행되고, run이 변환·binding·세션별 고시와 읽기 영수증을 기록한다 | `tests/application/test_strict_head_inputs.py::test_a_mixed_currency_strategy_runs_on_its_granted_fx_pin` | 구현 |
| DV-392 | `signal_basis`는 신호가 환산한 종가와 그 통화의 종가 중 무엇을 읽는지 정하고 체결은 늘 환산한다 | `tests/application/test_strict_head_inputs.py::test_a_conversion_states_which_currency_its_signals_read` | 구현 |
| DV-393 | 고시가 없는 체결 세션은 grant한 나이 안의 앞선 고시만 쓰고, 그것도 없으면 그 칸은 값이 없다 | `tests/application/test_strict_head_inputs.py::test_a_fixing_converts_only_within_its_granted_age` | 구현 |
| DV-394 | 연구 선언의 `macro` grant는 슬리브가 읽는 series를 판단마다 그 cutoff로 읽어 공급하고 봉인 문서가 binding과 읽기를 기록한다 | `tests/application/test_research_grants.py::test_a_granted_macro_series_feeds_its_sleeve_at_each_decision` | 구현 |
| DV-395 | `macro` grant는 슬리브들이 읽는 series 집합과 정확히 같아야 하고 단위·이름·도메인이 맞아야 한다 | `tests/application/test_research_grants.py::test_a_macro_grant_names_exactly_the_series_the_sleeves_read` | 구현 |
| DV-396 | 연구 선언의 FX grant는 자기 통화 chain을 함께 읽어 계좌 통화로 환산하고, 같은 값의 단일 통화 run과 같은 패널과 판단을 낸다 | `tests/application/test_research_grants.py::test_a_mixed_currency_run_converts_the_granted_chain_into_its_account` | 구현 |
| DV-397 | 연구 패널의 각 세션은 그 세션의 고시로 환산된다 | `tests/application/test_research_grants.py::test_each_session_takes_its_own_fixing` | 구현 |
| DV-398 | 연구 패널은 grant 없는 통화의 bar와 grant chain의 다른 통화 bar를 거부한다 | `tests/application/test_research_grants.py::test_a_price_currency_is_read_only_under_its_grant` | 구현 |
| DV-399 | grant를 가진 연구 선언은 run으로 기록되고 선언으로 다시 준비해 재현된다 | `tests/application/test_research_grants.py::test_a_granted_run_is_recorded_and_reproduces_from_its_declaration` | 구현 |
| DV-400 | 표본 조합의 공격 슬리브는 거시 신호로 전환하지 않고, 방어 슬리브는 거시 신호를 가질 수 있다 | `tests/application/test_research_grants.py::test_a_composition_offense_switches_on_its_canary_not_a_macro_signal` | 구현 |
| DV-401 | 연구 판단은 신호 종가를 그 cutoff까지 공개된 고시로 환산하고, 그 뒤에 공개된 고시는 시가·종가 패널만 환산한다 | `tests/application/test_research_grants.py::test_a_decision_converts_its_signals_with_the_fixings_its_cutoff_knows` | 구현 |
| DV-402 | 연구 `signal_basis=price_currency`는 신호가 그 통화의 종가를 읽게 하고 판단에서 고시를 읽지 않으며 패널은 늘 환산한다 | `tests/application/test_research_grants.py::test_a_research_conversion_states_which_currency_its_signals_read` | 구현 |
| DV-403 | strict 판단은 그 cutoff 뒤에 공개된 고시로 신호를 환산하지 않고, 체결 가격은 그 고시로 환산한다 | `tests/application/test_strict_head_inputs.py::test_a_fixing_published_after_a_decision_converts_its_fills_not_its_signals` | 구현 |
| DV-404 | proxy의 donor·target 선택은 계좌 통화여야 한다 | `tests/application/test_strict_head_inputs.py::test_a_proxy_reads_its_donor_and_target_in_the_account_currency` | 구현 |
| DV-405 | grant 없는 실행 요청의 `request_hash`는 FX grant 계약 전과 같다 | `tests/engine/test_backtest_request.py::test_a_request_without_a_grant_keeps_its_hash` | 구현 |
| DV-406 | grant 없는 연구 선언의 선언 hash와 봉인 문서는 grant 계약 전과 같다 | `tests/application/test_research_run.py::test_a_declaration_without_grants_keeps_its_hash_and_sealed_record` | 구현 |
| DV-407 | [dataset 카탈로그](#dataset-카탈로그) 표는 `storage/dataset_catalog.py`의 `CATALOG`와 열마다 같다 | `tests/tools/test_dataset_catalog.py::test_the_documented_table_is_the_code_catalog` | 구현 |
| DV-408 | `aas data datasets`는 문서의 카탈로그 항목을 그대로 돌려준다 | `tests/tools/test_dataset_catalog.py::test_datasets_command_reports_the_documented_catalog` | 구현 |
| DV-409 | 등록된 매퍼는 모두 카탈로그의 자기 도메인 dataset을 만든다 | `tests/tools/test_dataset_catalog.py::test_every_registered_mapper_builds_a_dataset_of_its_domain` | 구현 |
| DV-410 | 카탈로그의 시간·숫자 규칙은 등록된 규칙이고 매퍼의 시간 입력과 숫자 열 타입에 맞는다 | `tests/tools/test_dataset_catalog.py::test_rules_are_registered_and_fit_the_mappers` | 구현 |
| DV-411 | 코드가 쓰는 dataset 이름은 모두 카탈로그 항목에 속한다 | `tests/tools/test_dataset_catalog.py::test_dataset_names_the_code_writes_are_cataloged` | 구현 |
| DV-412 | committed dataset은 자기 항목(`.r<N>` 세대 포함) 아래 나열되고, 카탈로그 밖 이름은 게시된 채 `uncataloged`로 남는다 | `tests/storage/test_dataset_listing.py::test_published_datasets_are_listed_under_their_entry` | 구현 |
| DV-413 | Qveris US 일간 `splits`·`dividends` 원천은 `actions.us.eodhd` generation으로 승격된다 | `tests/storage/test_qveris_actions.py::test_us_splits_and_dividends_promote_to_actions_us_eodhd` | 예정 |
| DV-414 | Qveris KR(KO, KQ) 일간 `splits`·`dividends` 원천은 `actions.kr.eodhd` generation으로 승격된다 | `tests/storage/test_qveris_actions.py::test_kr_splits_and_dividends_promote_to_actions_kr_eodhd` | 예정 |
| DV-415 | KIND 상장법인목록 원천은 `status.kr.kind` generation으로 승격된다 | `tests/storage/test_kind_status.py::test_kind_listings_promote_to_status_kr_kind` | 예정 |
| DV-416 | DART 공시 목록(`list.json`) 원천은 `dart.list` 매퍼로 `filings.kr.dart` generation에 승격된다 | `tests/storage/test_dart_promotion.py::test_list_json_filings_promote_to_filings_kr_dart` | 예정 |
| DV-417 | 기본 verify는 원천 행을 읽지 않고 `--deep`과 같은 보고를 내며, 원천 테이블의 행 수·열·존재가 기록과 다르면 기본에서도 실패한다 | `tests/storage/test_verification.py::test_default_verify_compares_digests_and_deep_rehashes` | 구현 |
| DV-418 | KR 가격과 선언 달력의 승격 명세는 카탈로그 항목의 매퍼·시간·숫자·품질 규칙을 쓴다 | `tests/tools/test_dataset_catalog.py::test_kr_price_specs_use_the_catalog_rules` | 구현 |
| DV-419 | 동치 digest는 비교 열의 정확한 형태 셀에 대한 `aas-rowset-v1`이며 독립 Python 계산과 같다 | `tests/storage/test_source_retirement.py::test_equivalence_digest_is_rowset_v1_of_typed_cells` | 구현 |
| DV-420 | 정확한 rowset 형태가 없는 값이나 타입은 group을 거부한다 | `tests/storage/test_source_retirement.py::test_values_without_an_exact_rowset_form_refuse_the_group` | 구현 |
| DV-421 | 승격 명세·`dataset_sources`·도메인 행은 원천 참조로 센다 | `tests/storage/test_source_retirement.py::test_promotion_pins_and_rows_are_references` | 구현 |
| DV-422 | 은퇴 request hash 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_source_retirement.py::test_retirement_request_hash_format_is_frozen` | 구현 |
| DV-423 | 테이블을 지운 뒤 멈춘 은퇴는 quarantine되지 않고 recover가 기록까지 끝낸다 | `tests/storage/test_source_retirement.py::test_interrupted_retirement_is_finished_not_quarantined` | 구현 |
| DV-424 | compact는 새 루트에서 같은 논리 검증을 통과하고 은퇴한 테이블의 공간을 회수한다 | `tests/storage/test_compaction.py::test_compaction_reclaims_retired_space_and_verifies_the_same` | 구현 |
| DV-425 | 중단된 compact는 원래 설치본을 바꾸지 않고 새 루트를 미완료로 남긴다 | `tests/storage/test_compaction.py::test_interrupted_compaction_preserves_original` | 구현 |
| DV-426 | 비교에서 빠진 은퇴 테이블 열은 `uncompared`로 정확히 선언해야 하고 계획은 `partial_columns`로, 기록의 `equivalence_spec`은 그 열 이름으로 남긴다 | `tests/storage/test_source_retirement.py::test_a_column_left_out_is_declared_and_recorded` | 구현 |
| DV-427 | feature 입력과 입력 binding의 `ref_id`는 원천 참조로 센다 | `tests/storage/test_source_retirement.py::test_feature_inputs_and_bindings_are_references` | 구현 |
| DV-428 | 은퇴한 legacy unit의 재적재는 재사용이고 `--verify`는 그 unit을 `retired`로 일치시킨다 | `tests/storage/test_source_retirement.py::test_a_retired_legacy_unit_is_reused_and_verified` | 구현 |
| DV-429 | compact는 외래 key를 가진 generation chain과 run 결과 행을 부모부터 옮기고 같은 검증을 통과한다 | `tests/storage/test_compaction.py::test_compaction_copies_rows_that_reference_other_rows` | 구현 |
| DV-430 | 원천 ID이면서 다른 원천의 `sl:` 링크인 이름은 두 원천 모두의 참조로 센다 | `tests/storage/test_source_retirement.py::test_a_name_that_denotes_two_sources_references_both` | 구현 |
| DV-431 | 복구가 읽을 수 없는 64 MiB 초과 기록 문서는 intent를 만들기 전에 거부한다 | `tests/storage/test_source_retirement.py::test_records_recovery_cannot_read_are_never_prepared` | 구현 |
| DV-432 | Arrow 원천·SQLite 원천·승격 조상 delta의 값 변경은 각각 기본 verify·backup·restore를 통과하고 `--deep` verify·backup·restore에서 실패하며, 백업 `backup.json`은 `deep`을 기록한다 | `tests/storage/test_verification.py::test_a_changed_value_fails_only_deep_verify_backup_and_restore` | 구현 |
| DV-433 | 원천 은퇴는 기본 백업 안의 은퇴 원천 행을 기록된 digest로 다시 해시하고, 다르면 `backup_content_mismatch`로 거부한다 | `tests/storage/test_source_retirement.py::test_retirement_rehashes_a_default_backup` | 구현 |
| DV-434 | 두 날의 유지보수 실행은 모든 chain을 head의 자식으로 이어 붙이고, 같은 날 다시 실행하면 답한 요청을 다시 묻지 않고 아무 generation도 게시하지 않는다 | `tests/application/test_maintain.py::test_two_daily_runs_continue_every_chain_and_repeat_nothing` | 구현 |
| DV-435 | 유료 호출 뒤나 승격 게시 뒤에 중단된 실행은 다음 실행이 공급자를 다시 부르지 않고 끝내며 원장에 증거와 어긋나는 attempt가 남지 않는다 | `tests/application/test_maintain.py::test_a_killed_run_is_recovered_without_asking_any_answered_request_again` | 구현 |
| DV-436 | 새 KR 상장은 identity 증분과 새 유지보수 snapshot으로 등록되고 같은 실행의 KR 가격·분류 generation에서 해석된다 | `tests/application/test_maintain.py::test_new_kr_listings_advance_identity_and_every_kr_chain` | 구현 |
| DV-437 | Qveris 요청은 묻기의 raw 증거로 정해진다: 경고 완료는 덮이고, 정산된 실패는 다음 날 새 job으로, 미정산은 보류되며 자기 job으로 먼저 끝낸다 | `tests/application/test_maintain_qveris.py::test_each_ask_state_decides_its_request` | 구현 |
| DV-438 | 격리된 Qveris 묻기는 다음 관측일에 새 job으로 다시 묻고 같은 날에는 기다린다 | `tests/application/test_maintain_qveris.py::test_a_quarantined_ask_is_asked_again_the_next_day_and_not_the_same_day` | 구현 |
| DV-439 | 유지보수 명세는 template에서 parent·원천·partition만 바꾸고 tombstone은 `never`이며, 끝난 원천은 `maintain_source@1`로 다시 계획되지 않는다 | `tests/storage/test_maintain_promotion.py::test_new_sources_continue_the_chain_from_the_template` | 구현 |
| DV-440 | head나 route 매퍼의 template이 없는 dataset은 유지보수가 시작하지 않고 보고한다 | `tests/storage/test_maintain_promotion.py::test_a_route_without_a_head_or_template_reports_and_starts_nothing` | 구현 |
| DV-441 | 유지보수 명세의 generation pin은 지금 head로 바뀌고 identity는 snapshot을 pin한 template에서만 바뀐다 | `tests/storage/test_maintain_promotion.py::test_generation_pins_advance_and_the_identity_replaces_only_a_pinned_snapshot` | 구현 |
| DV-442 | 유지보수 설정의 알 수 없는 필드·빠진 상한·잘못된 타입은 설정 전체를 거부한다 | `tests/application/test_maintain_cli.py::test_a_typo_or_missing_cap_refuses_the_whole_configuration` | 구현 |
| DV-443 | 설치 receipt와 다른 실행 환경은 실행 보고의 차이로 남고 실행을 막지 않는다 | `tests/application/test_maintain_cli.py::test_the_install_receipt_records_the_runtime_and_reports_its_differences` | 구현 |
| DV-444 | 유지보수 계획은 자격 증명을 읽지 않고, `jobs.enabled` grant가 없는 실행은 공급자를 부르지 않고 나머지 단계를 끝낸다 | `tests/application/test_maintain_cli.py::test_plan_reads_no_key_and_run_without_the_jobs_grant_calls_nothing` | 구현 |
| DV-445 | 재개 뒤에도 정산되지 않은 유료 page는 Qveris를 `stopped`(종료 코드 2)로 남기고 attempt를 열린 채 두며, 정산이 돌아오면 다시 호출 없이 `succeeded`로 끝난다 | `tests/application/test_maintain.py::test_an_unsettled_paid_page_stops_every_run_until_its_settlement` | 구현 |
| DV-446 | 한 단계의 데이터베이스 오류는 그 단계의 실패로 기록되고 뒤 단계와 실행 보고는 그대로 남는다 | `tests/application/test_maintain.py::test_a_database_error_in_one_stage_leaves_the_later_stages_and_the_report` | 구현 |
| DV-447 | `identity` 단계가 실패한 실행은 snapshot을 pin하는 chain을 `identity_unavailable`로 두고, 회복한 실행이 새 상장의 행을 해석해 승격한다 | `tests/application/test_maintain.py::test_a_failed_identity_stage_holds_identity_chains_until_it_recovers` | 구현 |
| DV-448 | 늦은 identity가 남긴 `unresolved` 원천은 같은 identity로는 다시 계획되지 않고, 그 key의 해석 행이 바뀐 snapshot에서 다시 승격된다 | `tests/application/test_maintain.py::test_rows_an_older_snapshot_left_unresolved_are_promoted_once_identity_catches_up` | 구현 |
| DV-449 | 연속한 정산된 실패는 1, 2, 4일 간격으로 늦춰 다시 묻고 재시도는 첫 묻기 뒤에 온다 | `tests/application/test_maintain_qveris.py::test_settled_failures_back_off_and_retry_after_the_first_asks` | 구현 |
| DV-450 | 전환을 마친 설치본은 모든 확인을 통과하고 통과한 보고만 정확한 bytes로 `raw/`와 `runtime/`에 기록된다 | `tests/application/test_cutover.py::test_a_cut_over_installation_passes_and_records_it` | 구현 |
| DV-451 | legacy `aas-*` unit이 하나라도 파일이나 적재된 unit(실패 상태 포함)으로 남으면 unit 확인이 실패한다 | `tests/application/test_cutover.py::test_legacy_units_left_behind_fail_the_unit_check` | 구현 |
| DV-452 | 유지보수 service가 없거나 timer가 enabled·active가 아니면 unit 확인이 실패한다 | `tests/application/test_cutover.py::test_the_maintenance_timer_must_be_installed_enabled_and_active` | 구현 |
| DV-453 | `systemctl`을 실행할 수 없으면 unit 확인은 실패로 닫힌다 | `tests/application/test_cutover.py::test_an_unavailable_systemctl_fails_closed` | 구현 |
| DV-454 | deep이 아니거나, 뒤에 끝난 operation을 담지 않거나, 설치본과 같은 장치의 백업은 확인을 통과하지 않는다 | `tests/application/test_cutover.py::test_a_backup_must_be_deep_current_and_on_another_device` | 구현 |
| DV-455 | 파일이 기록과 다르거나 없는 백업과 다른 설치본의 백업은 확인을 통과하지 않는다 | `tests/application/test_cutover.py::test_a_broken_or_foreign_backup_fails` | 구현 |
| DV-456 | 꺼진 예상 공급자, `jobs.enabled` 거짓, 없는 설치 receipt, 성공하지 않은 유지보수 실행은 각자의 항목으로 실패한다 | `tests/application/test_cutover.py::test_collectors_receipt_and_maintenance_run_are_required` | 구현 |
| DV-457 | `raw/`에 없고 verify 보고도 없는 manifest, 남은 항목 경로, 남은 `--removed` 경로는 legacy 확인을 실패시킨다 | `tests/application/test_cutover.py::test_legacy_paths_must_be_imported_and_gone` | 구현 |
| DV-458 | 은퇴 기록은 백업 ID·operation·이유별 원천과 행 수로 보고된다 | `tests/application/test_cutover.py::test_retirements_are_reported_by_backup_operation_and_reason` | 구현 |
| DV-459 | `cutover-check --record`는 실패한 확인을 기록하지 않고 종료 코드 1, 통과한 확인을 기록하고 종료 코드 0이다 | `tests/application/test_cutover.py::test_the_command_records_only_a_passing_check` | 구현 |
| DV-460 | 운영 runbook의 fence와 inline `aas` 명령과 그 옵션은 CLI에 있고, runbook은 legacy unit 넷을 은퇴하고 다섯 수집기를 기대한다 | `tests/application/test_cutover.py::test_the_runbook_names_only_commands_and_options_the_cli_has` | 구현 |
| DV-461 | `raw/`에 있는 manifest라도 verify 보고가 없거나, `complete`가 아니거나, 그 원천이 설치본에 commit으로 없거나, 읽을 수 없는 보고가 있으면 legacy 확인이 실패한다 | `tests/application/test_cutover.py::test_a_retained_manifest_without_a_complete_verify_fails` | 구현 |
| DV-462 | `PREPARED` storage operation이 남으면 operations 확인이 실패한다 | `tests/application/test_cutover.py::test_a_prepared_operation_fails_the_operations_check` | 구현 |
| DV-463 | core schema가 현재 버전이 아닌 설치본은 schema 확인이 `outdated`로 실패한다 | `tests/application/test_cutover.py::test_an_outdated_core_schema_fails_the_schema_check` | 구현 |
| DV-464 | runbook 리허설은 설치본 밖 삭제·systemd·receipt를 실행하지 않고, 별도 블록인 리허설 정리는 끝까지 마친 리허설의 모든 출력 경로만 지운다 | `tests/application/test_cutover.py::test_the_rehearsal_writes_only_paths_its_cleanup_removes` | 구현 |
| DV-465 | 삽입 뒤 COMMIT에서 메모리가 고갈된 대량 게시도 marker와 행을 남기지 않고 `ComputeResourceError`가 된다 | `tests/storage/test_bulk_generation.py::test_commit_exhaustion_rolls_back_as_a_budget_error` | 구현 |
| DV-466 | 용량과 무관한 COMMIT 실패는 인용한 key가 용량 문구를 담아도 `ComputeResourceError`로 바뀌지 않고, 뒤따르는 ROLLBACK이 그 오류를 덮지 않는다 | `tests/storage/test_bulk_generation.py::test_a_failed_commit_is_not_replaced_by_its_rollback` | 구현 |
| DV-467 | core migration의 market 단계는 compute lease의 DuckDB 몫에서 실행하고, 그 COMMIT의 고갈은 intent를 남긴 채 `ComputeResourceError`가 된다 | `tests/storage/test_migration.py::test_an_exhausted_market_step_is_a_budget_error_under_the_lease` | 구현 |
| DV-468 | compact의 market 복사는 compute lease의 DuckDB 몫에서 실행하고, 고갈은 원본을 바꾸지 않은 채 `ComputeResourceError`가 된다 | `tests/storage/test_compaction.py::test_compaction_copies_under_the_lease_and_names_exhaustion` | 구현 |
| DV-469 | core migration 단계마다 자기 intent와 백업을 가지며, v2 단계의 요청과 부모 정체성은 나중 버전이 생겨도 출시된 그대로다 | `tests/storage/test_migration_steps.py::test_step_identities_are_frozen_and_outlive_later_steps` | 구현 |
| DV-470 | 한 호출이 여러 단계를 지나면 단계마다 새 백업을 만들고 각 intent가 자기 백업을 가리킨다 | `tests/storage/test_migration_steps.py::test_one_invocation_takes_every_step_with_its_own_backup` | 구현 |
| DV-471 | 단계 완료와 다음 단계 intent 사이에서 멈춘 설치본은 다음 단계의 새 백업을 요구한다 | `tests/storage/test_migration_steps.py::test_a_kill_between_steps_needs_the_next_steps_own_backup` | 구현 |
| DV-472 | 손대지 않은 승격 intent는 증거와 함께 migration 백업에 PREPARED로 실리고, `--plan`은 다시 계획해 증명했다고 적으며, migration 뒤 `aas db recover`가 실패 전 계획과 같은 generation·delta·chain·manifest hash로 게시한다 | `tests/storage/test_migration_steps.py::test_an_untouched_promotion_is_carried_through_a_step_and_then_recovered` | 구현 |
| DV-473 | marker·catalog 흔적이 있거나 증거가 intent의 것이 아닌 승격은 migration을 막고, 이름을 넘겨도 백업이 다시 거부한다 | `tests/storage/test_migration_steps.py::test_only_a_promotion_recovery_would_publish_is_carried` | 구현 |
| DV-474 | migration snapshot은 v2만 아는 코드로 복원·검증되고 다시 migration한 뒤 같은 승격을 게시한다 | `tests/storage/test_migration_steps.py::test_the_snapshot_restores_for_v2_code_and_migrates_again` | 구현 |
| DV-475 | `--to 2`는 나중 버전을 아는 코드에서도 v2에서 멈추고, 다음 단계는 거기서 시작하며, 목표 이상인 설치본은 그대로 둔다 | `tests/storage/test_migration_steps.py::test_to_2_stops_there_and_a_later_step_starts_from_it` | 구현 |
| DV-476 | 나중 단계 어디서 멈춰도 같은 목표로 다시 실행하면 백업 없이 끝나고, 앞선 목표는 거부된다 | `tests/storage/test_migration_steps.py::test_a_kill_in_a_later_step_is_finished_by_repeating_it` | 구현 |
| DV-477 | market COMMIT 뒤 멈춘 단계를 재개하는 호출은 다음 단계 백업까지 이어 가고, 그 전에 모든 백업 목적지를 백업의 조건대로 확인한다 | `tests/storage/test_migration_steps.py::test_a_step_whose_market_landed_resumes_into_the_next_step` | 구현 |
| DV-478 | bytes가 intent와 맞아도 보존 명세를 다시 계획해 같은 manifest가 나오지 않는 승격(고쳐 다시 hash한 manifest, 옮겨진 head, 바뀐 원천)은 migration을 막고 recover도 게시하지 않는다 | `tests/storage/test_migration_steps.py::test_only_a_promotion_recovery_would_publish_is_carried` | 구현 |
| DV-479 | 승격 증거를 읽다 난 I/O 오류는 가지고 가지 않는 이유가 되며 migration 밖으로 새지 않는다 | `tests/storage/test_migration_steps.py::test_an_evidence_read_error_refuses_the_carry` | 구현 |
| DV-480 | promotion이 아닌 PREPARED 작업은 carry할 승격과 같은 요청·generation·manifest를 적어도 migration을 막는다 | `tests/storage/test_migration_steps.py::test_a_prepared_operation_of_another_kind_blocks` | 구현 |
| DV-481 | migration의 carry와 recover의 게시는 한 재계획 구현을 공유해, 그 하나가 거부하면 둘 다 거부한다 | `tests/storage/test_migration_steps.py::test_recovery_and_the_carry_share_one_replan` | 구현 |
| DV-482 | 재계획이 compute lease 안에서 돌 수 없으면 `--plan`, migration과 그 백업은 `ComputeResourceError`로 거부하고 아무것도 쓰지 않는다 | `tests/storage/test_migration_steps.py::test_a_replan_beyond_the_lease_refuses_the_migration` | 구현 |
| DV-483 | carry 재계획은 앞선 검증이 낮춘 연결이 아니라 설치본 한도에서 lease 몫으로 돌아, `--plan`이 허용한 lease에서 migration과 그 백업도 같은 승격을 가지고 간다 | `tests/storage/test_migration_steps.py::test_a_carry_is_planned_alike_after_a_verification_lowered_the_connection` | 구현 |
| DV-484 | 재계획 뒤 게시 전에 head가 옮겨진 승격은 recover에서 예외 없이 pending으로 남고 계획의 임시 table은 지워진다 | `tests/storage/test_migration_steps.py::test_a_head_moved_after_the_replan_leaves_the_promotion_pending` | 구현 |
| DV-485 | v3는 v2 DDL에서 도메인 테이블과 `quality_flags`의 key만 뺀다 | `tests/storage/test_core_v3.py::test_v3_drops_only_the_key_indexes` | 구현 |
| DV-486 | v3 저장소는 key를 뺀 모든 열·NOT NULL·CHECK·FK·기본값 규칙을 지킨다 | `tests/storage/test_core_v3.py::test_a_v3_store_holds_every_rule_but_the_keys` | 구현 |
| DV-487 | 새로 만든 v3 저장소와 migration한 v3 저장소의 core catalog가 같다 | `tests/storage/test_core_v3.py::test_fresh_and_migrated_v3_stores_hold_the_same_catalog` | 구현 |
| DV-488 | v3 재구축은 행 수가 같아도 값이 바뀐 복사를 거부하고 단계 트랜잭션 전체를 되돌린다 | `tests/storage/test_core_v3.py::test_the_rebuild_refuses_a_copy_that_changed_rows` | 구현 |
| DV-489 | v3 단계의 어느 경계에서 멈춰도 같은 명령이 백업 없이 끝내고 모든 행과 hash가 그대로다 | `tests/storage/test_core_v3.py::test_a_kill_at_every_boundary_of_the_v3_step_is_finished` | 구현 |
| DV-490 | v2에서 게시한 승격은 v3에서 같은 요청·generation·delta·chain·manifest hash로 다시 나온다 | `tests/storage/test_core_v3.py::test_a_v2_promotion_replays_on_v3_with_the_same_hashes` | 구현 |
| DV-491 | migration한 v3 설치본은 compact되고 새 루트가 deep으로 검증된다 | `tests/storage/test_core_v3.py::test_a_migrated_v3_installation_compacts_and_verifies_deep` | 구현 |
| DV-492 | 대량 writer는 다른 dataset의 generation이 가진 `(record_id, revision_id)`도 COMMIT 전에 거부한다 | `tests/storage/test_market_integrity.py::test_bulk_writer_refuses_a_revision_another_dataset_holds` | 구현 |
| DV-493 | 한 프로세스의 두 cursor가 같은 key를 게시해도 하나만 commit된다 | `tests/storage/test_market_integrity.py::test_the_judges_two_cursor_counterexample_cannot_commit_both` | 구현 |
| DV-494 | 반복된 flag key, 저장되지 않은 revision의 flag, 검토하지 않은 flag는 COMMIT 전에 거부된다 | `tests/storage/test_market_integrity.py::test_flag_gate_refuses_repeats_orphans_and_unreviewed_flags` | 구현 |
| DV-495 | 기본 감사는 generation별 행 수와 marker를 양쪽으로 대조한다 | `tests/storage/test_market_integrity.py::test_count_audit_compares_rows_and_markers_both_ways` | 구현 |
| DV-496 | 도메인 전역 key와 flag key 중복은 `--deep` 검증만 감사한다 | `tests/storage/test_market_integrity.py::test_only_deep_verify_audits_duplicates` | 구현 |
| DV-497 | 중복 감사의 pass는 잰 행 수와 key bytes로 나뉘고 치우친 긴 key도 끝까지 검사한다 | `tests/storage/test_market_integrity.py::test_adaptive_audit_splits_skewed_wide_keys_by_measured_size` | 구현 |
| DV-498 | core catalog는 같은 버전의 빈 저장소와 열 순서·타입·제약까지 같아야 한다 | `tests/storage/test_market_integrity.py::test_core_catalog_matches_an_empty_store_of_its_version` | 구현 |
| DV-499 | runbook 리허설과 4단계 승격 루프는 첫 실패에서 0이 아닌 상태로 멈추고 리허설 설치본·migration 백업·보고를 남기며 정리는 아무것도 지우지 않는다 | `tests/application/test_cutover.py::test_a_failed_rehearsal_stops_and_keeps_its_root_backups_and_reports` | 구현 |
| DV-500 | 끝까지 마친 리허설만 완료 표시를 남기고 리허설 정리는 그 리허설의 출력만 지운다 | `tests/application/test_cutover.py::test_a_finished_rehearsal_is_the_only_one_its_cleanup_removes` | 구현 |
| DV-501 | 리허설 채택은 계획 확인 뒤 별도 블록에서 실행되고 migration 검증이 실패하면 `recover` 전에 멈춘다 | `tests/application/test_cutover.py::test_an_adopted_rehearsal_stops_before_recover_when_its_migration_check_fails` | 구현 |
| DV-502 | `kind.industry@2`는 업종 판정에 쓰지 않는 열만 다른 원천 행을 instrument당 한 분류 행으로 승격하고 입력 행 순서와 무관하게 같은 record·revision hash를 내며, 한 수집 시각에 업종이 다른 두 행은 거부한다 | `tests/storage/test_classifications.py::test_kind_industry_v2_maps_repeated_rows_once` | 구현 |
