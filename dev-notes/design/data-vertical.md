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
| L1 원천 자료실 | `sl_*` 테이블. 원래 열·값·행 순서를 그대로 보존하는 `source_only` 자료와 legacy 원본 편입 | `storage/source_library*.py`, `storage/legacy_import/` |
| L2 승격 | 승격 명세 → 매퍼 → head 비교 → 품질 flag → generation 게시 | `storage/promotion/` |
| L3 저장 | `market.duckdb`의 typed generation과 `state.sqlite3`의 카탈로그·identity·품질·수집 기록 | `storage/market.py`, `storage/state.py`, `storage/identity.py` |
| L4 소비 | exact pin, cutoff, grant로 head를 투영하는 reader와 실행 준비 | `storage/read_heads.py`, `storage/market_inputs.py`, `application/backtest_prepare.py` |

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
싣는 제출자 문서는 단위를 거부한다. 제출자가 나열했지만 archive에 없는 쪽(`missing_pages`), 어느 제출자도
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
| `identity_snapshot` | 매퍼의 identity key를 해석할 identity snapshot pin(`snapshot_id`, `content_hash`). instrument 도메인은 늘 pin한다. identity key가 없는 매퍼(거시·FX·달력, 영구 anchor에서 주체 ID를 발급하는 분류 매퍼, 발행인 단위의 재무·공시 매퍼)는 null |

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
`unresolved`, `ambiguous`, `refused_*`), 매퍼가 고르지 않은 원천 행 수(`unselected_rows`, 여러 시계열을
담은 원천에서 한 시계열만 읽는 매퍼나 분류가 없는 회사처럼 매퍼가 정의상 고르지 않는 행), 미해결 token 표본, 해석 전 매핑 행 전체의 숫자 flag 분포,
시간 규칙별 null·상한 적용 수, 반복된 자연키, op 분포, 변하지 않은 행과 stale 행 수,
head와 시점이 다르게 계산되는 변하지 않은 행 수(`time_drift`), delta의 flag 분포, 부분 응답 행이 있으면
그 행 수 대조(`partition_row_count`), 계획한 marker를 담는다.

보고는 두 종류의 거부 이유를 따로 싣는다. `blocking`은 설치본이 아직 갖추지 않은 전제다(core
schema v2, 원천의 `sl:` 연결, 등록되지 않은 identity snapshot). `refusals`는 자료 자체의 문제다(규칙이
변환하지 못한 숫자, 비어 있는 필수 열, 수집 시각이 없는 행, 반복된 자연키, 부재를 증명할 수 없는
미해결 행, `partition`이 있는 명세에서 파티션 날짜가 없는 원천 행). 필수 열이 빈 행은 identity 해석 결과와
무관하게 `refused_required`이므로 형식이 잘못된 행이 미해결 행으로 빠지지 않는다. 실행은 둘 중 하나라도
있으면 아무것도 쓰지 않고 거부한다. delta가 비어 있으면
아무것도 쓰지 않는다.

실행은 명세, 요청 문서, 승격 manifest를 `raw/`에 쓰고, 그 manifest의 SHA-256을 payload hash로 한
`promotion` intent를 기록한 뒤, marker·행·quality flag를 DuckDB 트랜잭션 하나에서 게시하고, 마지막에
state 카탈로그를 한 트랜잭션으로 쓰고 intent를 완료한다. 카탈로그는 `datasets`(owner `promotion`),
`dataset_versions`(`manifest_hash`는 marker의 request hash, `transform_hash`는 명세 해시,
`normalizer_version`은 매퍼 `name@major`, `identity_snapshot_hash`, `coverage`는 partition), 원천마다
`dataset_sources`의 `sl:` 행, `quality_checks`의 `promotion_report@1` 행(op·행 상태·flag 수)과 부분 응답
행이 있으면 `partition_row_count@1` 행([품질 검사](#품질-flag와-품질-검사)), 그리고
(매퍼 공급자, dataset, partition)마다 delta의 가장 늦은 `ingested_at_us`까지 앞으로만 가는 `watermarks`다.
`committed_version`은 그 시각까지 처음 나아간 version이며, 시각을 넘지 못한 정정 generation은 바꾸지 않는다.

승격 manifest(`aas-promotion-manifest-v1`, 정규 JSON)는 요청·명세 해시, marker의 dataset·version·
generation·parent·sequence·delta hash·chain hash·행 수, op 분포, 행 상태 수, flag 행의 rowset digest와
수, 원천 pin과 identity pin, 부분 응답 행이 있으면 `partition_row_count`를 담는다. 중단된 승격은 같은 명령을 다시 실행하거나 `aas db recover`로
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

- `instrument_id`가 선택인 도메인(재무)은 identity key가 없는 매퍼로도 승격할 수 있고, 그 매퍼는
  `instrument_id`를 직접 낸다(발행인 단위 행은 null). `instrument_id`가 필수인 도메인은 instrument를
  해석하는 매퍼만 쓴다.
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

분류 매퍼 `norgate.classification@1`, `sec.sic@1`, `kind.industry@1`은 [분류](#분류)가 소유한다.

`sec.submissions@1`과 `sec.companyfacts@1`은 [SEC 공시와 재무](#sec-공시와-재무)를 발행인 단위
`filings`와 `fundamentals`로 옮기며, 재무 매퍼는 pin한 공시 generation을 참조로 조인한다.

예정된 매퍼 목록: `norgate.dividends`,
`fmp.actions`,
`dart.fnltt`, `dart.list`. identity 원천을 읽는 매퍼는 typed generation이
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

- Norgate: asset ID가 양의 정수(18자리 이하)가 아니거나 `first_date`·`last_date`가 `as_of`보다 늦은
  행(선언한 내보내기 날짜와 모순)은 주체가 없고, 유형 단계가 비거나 건너뛴 행과 거래소 이름이 빈 행은
  코드가 없다. 날짜로 읽을 수 없는 날짜 텍스트는 모순의 근거로 쓰지 않는다.
- SEC: member 이름이 `CIK##########.json`이고 문서의 `cik`가 같은 CIK(0 채움 무시)를 말하며 최신
  공시일(`latest_filing_date`)이 `as_of`보다 늦지 않을 때만 issuer가 나온다. 쪽 나눈 이력 member와
  다른 CIK를 말하는 문서는 주체가 없다. 네 자리가 아닌 SIC는 코드가 없다. 설명 없이 SIC만 싣는 회사(미배정 `0000` 포함)는 분류가 없는 행이다.
- KIND: `retrieved_at_utc`가 `Z`로 끝나는 UTC 시각이 아니면 수집 시각과 날짜가 없어 거부된다.
  snapshot에서 해석되지 않는 단축코드는 다른 승격처럼 미해결 보고에 남는다.

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

## 대상 dataset

| dataset | 도메인·역할 | 원천과 규칙 |
| --- | --- | --- |
| `prices.kr.eodhd` | `prices`, canonical unadjusted | EODHD KR 일봉 이력(`eodhd.bars@1`)과 부분 응답 일간 내려받기(`eodhd.bulk_quarantine@1`). `krw_tick@1`, `session_close_plus_lag@1`, 부분 응답 flag와 행 수 대조, 잘못된 가격은 `invalid`. [KR 가격](#kr-가격) |
| `prices.kr.eodhd.ref` | `prices`, reference `total_return`, `fields='close'` | 같은 원천의 adjusted close(`eodhd.bars_adjusted@1`, `eodhd.bulk_quarantine_adjusted@1`), `float_shortest@1` |
| `prices.us.norgate` | `prices`, canonical unadjusted | Norgate 비조정 일봉 내보내기(CSV), `norgate.prices_none@1`. 다섯 값 모두 `decimal_text@1` |
| `prices.us.norgate.ref` | `prices`, reference `split_adjusted`·`total_return` | Norgate 조정 OHLC, `norgate.prices_adjusted@1`. float32 저장값은 `float_shortest@1` |
| `prices.us.eodhd` | `prices`, canonical unadjusted | EODHD US 일간 수집, `eodhd.bars@1`. 다운로드 하나가 generation 하나이고 같은 날짜의 다른 다운로드는 SUPERSEDE. Norgate와 겹치는 구간에 `cross_provider_mismatch@1`(기준 `prices.us.norgate`). identity가 해석한 US 보류 행의 재처리 단계는 Linear AAS-69 |
| `prices.us.fmp.ref` | `prices`, reference unadjusted | FMP 동결 legacy non-split 응답, `fmp.eod_non_split@1`. 정정은 revision 번호순 generation. 원천 자료실 FMP normalized lineage(eod_full, dividend_adjusted)의 매퍼는 Linear AAS-68 |
| `prices.ref.norgate` | `prices`, reference, `fields='close'` | Norgate 기준 시리즈(`norgate.reference_closes@1`)와 지수·기타 데이터베이스 내보내기(`norgate.reference_history@1`). 음수 수준이 있는 시리즈는 `signed`로 고르지 않는다 |
| `sessions.xnys`, `sessions.xkrx` | `calendar_sessions` | [선언 달력](#선언-달력) 문서. 관측 거래일은 대조 보고의 근거. 임시 휴장은 SUPERSEDE |
| `actions.us.norgate`, `actions.us.fmp.ref`, `actions.{us,kr}.eodhd` | `corporate_actions` | `exdate_open@1` |
| `status.us.norgate`, `status.kr.kind` | `instrument_status` | 상장·상폐 이력 |
| `filings.us.sec`, `filings.kr.dart` | `filings`(v2) | SEC submissions(`sec.submissions@1`, `source_column@1`), DART 공시 목록(`local_day_end@1`) |
| `fundamentals.us.sec` | `fundamentals` | companyfacts(`sec.companyfacts@1`). 공시마다 자기 record(accession은 dimensions). 시점은 pin한 `filings.us.sec` generation에서 accession으로 조인한 `accepted_at_us`(`source_column@1`)이고, 조인되지 않은 사실은 null이다. 같은 accession의 값이 다시 수집되어 바뀌면 SUPERSEDE |
| `fundamentals.kr.dart` | `fundamentals` | 재무제표 응답. 연결·별도는 dimensions. 자료 없음 응답은 행 대신 coverage 기록 |
| `macro.us.alfred` | `macro_observations` | ALFRED vintage(`fred.alfred@1`). vintage 구간마다 generation 하나, 정정은 SUPERSEDE. 두 시점은 `local_day_end@1(realtime_start)` |
| `macro.kr.bok`, `macro.kr.oecd` | `macro_observations` | `bok.observations@1`, `oecd.observations@1`. vintage가 없어 두 시점은 `unknown_null@1` |
| `fx.usdkrw.norgate`, `fx.usdkrw.fred` | `fx_rates` | `norgate.fx_closes@1`(source library 기준 시리즈, 동결, 2026-09-08까지) 또는 `norgate.fx_history@1`(legacy 내보내기 편입본), 시간대 `Etc/GMT+12`. `fred.fx_series@1`(DEXKOUS), 시간대 `America/New_York`, H.10 발표 지연 때문에 두 시점은 `unknown_null@1`. 우선순위는 소비자 pin |
| `classifications.us.norgate` | `classifications`(v2) | Norgate 증권 유형(`norgate.security_type`)과 상장 거래소(`norgate.exchange`). [분류](#분류) snapshot이며 과거로 소급하지 않음 |
| `classifications.us.sec` | `classifications`(v2) | SEC SIC(`sec.sic`), issuer 주체 |
| `classifications.kr.kind` | `classifications`(v2) | KIND 업종(`kind.industry`), 단축코드를 identity snapshot으로 해석 |

identity 원천(Norgate master, SEC submissions, FMP profile, DART 고유번호, KIND 목록)은 typed generation이
아니라 아래 [identity 등록](#identity-등록과-chunked-문서)으로 state에 들어간다. 지수 구성과 상장 universe도
같은 방식으로 [universe 등록](#universe-등록)의 `universe_versions`·`universe_members`에 들어간다.

dataset의 백필은 연도 단위 generation, 이후 유지보수는 세션 단위(재무는 일 단위) generation으로
게시한다.

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

KIND 목록과 EODHD 종목 목록 수집물은 `aas identity kr-import`가 내용 원천으로 commit한다. KIND는
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
  행 수와 무관하다. 한 행도 들어가지 않으면 `ComputeResourceError`다. 정렬·spill·색인 유지는 같은
  할당에서 유도한 DuckDB 몫이 맡는다. 한도를 넘으면 트랜잭션 전체가 취소되고 `ComputeResourceError`가
  된다.
- 기본 512 MiB 할당은 Python 몫만 행 수와 무관하게 보장한다. 1e7행 계획의 Python peak는 74 MiB다.
  도메인 테이블의 `PRIMARY KEY(generation_id, record_id, revision_id)`와 `UNIQUE(record_id, revision_id)`
  색인은 삽입과 COMMIT 중 메모리에 올라오므로, DuckDB 몫은 그 테이블의 전체 행 수에 비례해 커진다.
  합성 prices 측정에서 checkpoint된 테이블에 10k행 generation을 게시할 때, 기존 1M행이면 512 MiB로
  통과했고 2M·4M행이면 1 GiB, 8M행이면 1.5 GiB가 필요했다. 빈 테이블에 1M행을 게시할 때는 1 GiB,
  1e7행을 게시할 때는 16 GiB 할당이 필요했다(RSS 약 9.7 GiB). 따라서 수천만 행 테이블의 백필과
  유지보수 게시는 기본 할당으로 끝나지 않는다. 색인 제거(다음 core 버전) 또는 기록된 더 큰 할당 grant
  중 하나를 Linear AAS-54에서 결정하며, 대량 승격(대응표 DV-75)은 그 결정 뒤에 한다.
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
  - coverage 이유는 `ungranted_time_rule`, `flag_excluded`, `outside_cutover`, `tombstone`,
    `unknown_<domain>_evidence`, `<domain>_unavailable`, `reference_price`, `missing_<domain>`과
    head의 `value_state`다(가격은 `price`, 달력은 `session`). head가 없는 record의 이유는
    `market_inputs`의 strict reader와 같게, cutoff까지 알려진 마지막 revision이 정한다. 그 revision의
    공개 시점이 null이면 `unknown_<domain>_evidence`, cutoff 뒤면 `<domain>_unavailable`이다. head를
    돌려준 칸에도 grant가 막은 정정이나 제외한 revision이 있으면 `ungranted_time_rule`이나
    `flag_excluded`를 남기고, 칸은 present로 둔다. 한 칸에 record가 여럿이면(같은 날짜의 canonical과
    reference 가격) head를 준 record의 이유만 그 칸의 이유다. 달력 칸은 개장한 session만 present이고
    휴장 session은 `session_closed`다. 격자의 칸은 결과 행과 함께 fetch 전 할당 검사에 포함된다.
- 분할조정·총수익 가격은 reader가 unadjusted 가격과 cutoff 시점까지 알려진 `corporate_actions`로
  계산한다. 공급자 조정 가격은 reference로만 남는다.

### 연구 실행의 canonical 가격 패널

선언한 미인증 연구 실행(`aas-research-run-v2`, `aas-research-composition-v1`)은 패널 원천으로 보존 관측
pin(`observations`) 대신 canonical 가격 binding(`prices`: `aas-head-binding-v1`의 pin과 cutover, flag 제외
목록)을 들 수 있다. 선언 최상위에는 둘 중 정확히 하나만 있다. `backtest_prepare._price_panels`가
`market_inputs.load_pinned_heads`로 그 binding을 한 번 읽어 open과 close 패널을 함께 만든다.

- query는 연구 모드이고 시간 규칙 grant를 쓰지 않는다. 지식 상한은 선언의 `knowledge_time`이고 subject는
  `instrument_map`의 instrument ID, 역할은 `canonical`, 날짜는 이력·기간 시작 중 이른 날부터 둘의 끝 중
  늦은 날까지다. 그래서 패널은 pin한 chain이 그 instrument에 가진 첫 세션부터 시작한다.
- 행은 `basis='unadjusted'`, `interval='1d'`의 canonical bar여야 하고 통화는 선언 통화와 같아야 한다. `present`가 아닌
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
  hash를 가리킨다. 실행은 여전히 `research-uncertified`이며 엄격 경로의 승인(DV-81)은 다루지 않는다.

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
| DV-28 | 유도 조정 가격은 cutoff 이후 기업행동을 쓰지 않는다 | `tests/storage/test_read_heads.py::test_adjustment_ignores_actions_after_cutoff` | 예정 |
| DV-29 | 티커로 instrument를 만들 수 없다 | `tests/storage/test_identity_mint.py::test_ticker_anchor_is_refused` | 구현 |
| DV-30 | v1→v2 migration은 백업 없이 거부하고 중단 후 재개한다 | `tests/storage/test_migration.py::test_migration_requires_backup_and_resumes` | 구현 |
| DV-31 | migration 후 v1 checksum 행이 남고 알 수 없는 버전은 거부한다 | `tests/storage/test_migration.py::test_migration_keeps_v1_receipt_and_rejects_unknown` | 구현 |
| DV-32 | `fields='close'` 가격은 reference만 될 수 있다 | `tests/storage/test_migration.py::test_close_only_prices_are_reference` | 구현 |
| DV-33 | 참조 중이거나 동치가 아니거나 백업이 없는 원천은 은퇴하지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_requires_proof` | 예정 |
| DV-34 | 은퇴는 `raw/` 원본을 지우지 않는다 | `tests/storage/test_source_retirement.py::test_retirement_keeps_raw_bytes` | 예정 |
| DV-35 | 대응표의 구현 행은 존재하는 테스트를, 예정 행은 아직 없는 테스트를 가리킨다 | `tests/tools/test_data_vertical_contract.py::test_contract_rows_match_tests` | 구현 |
| DV-36 | `record` 근거 규칙의 SUPERSEDE 시점은 정정을 담은 원천의 증거 시각이다 | `tests/storage/test_promotion_engine.py::test_superseding_revision_is_not_known_before_its_source` | 구현 |
| DV-37 | 명세 `scope` 밖의 record는 원천에서 빠져도 TOMBSTONE되지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_stays_within_declared_scope` | 구현 |
| DV-38 | TOMBSTONE 시점은 부재를 증명한 snapshot의 증거 시각이며 record 날짜 규칙을 쓰지 않는다 | `tests/storage/test_promotion_engine.py::test_tombstone_time_comes_from_absence_snapshot` | 구현 |
| DV-39 | 같은 값을 다른 수집 시각에 다시 수집해도 revision이 생기지 않는다 | `tests/storage/test_promotion_engine.py::test_recollection_at_new_ingestion_time_is_not_a_revision` | 구현 |
| DV-40 | head보다 이른 시점의 원천 행은 head를 대체하지 않고 stale로 보고된다 | `tests/storage/test_promotion_engine.py::test_older_source_row_does_not_supersede_newer_head` | 구현 |
| DV-41 | 참조가 없고 동치이며 백업된 원천은 자기 source-link 행이 있어도 은퇴하고 그 행은 남는다 | `tests/storage/test_source_retirement.py::test_unreferenced_equivalent_backed_up_source_is_retired` | 예정 |
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
| DV-75 | 수천만 행 도메인 테이블에 유지보수 generation을 기본 할당 또는 기록된 할당 grant 안에서 게시한다 | `tests/storage/test_bulk_generation.py::test_maintenance_publication_fits_a_large_table` | 예정 |
| DV-76 | `aas-head-binding-v1` binding hash와 `aas-head-read-v1` 영수증 형식은 고정 입력과 기대 값으로 고정돼 있다 | `tests/storage/test_read_heads.py::test_head_binding_and_receipt_formats_are_frozen` | 구현 |
| DV-77 | `read_heads`는 pin이 marker와 다르거나 chain link·행 수가 맞지 않으면 행을 읽지 않고, `rehash`는 모든 delta를 다시 해시한다 | `tests/storage/test_read_heads.py::test_pins_are_verified_before_rows_are_read` | 구현 |
| DV-78 | generation의 시간 규칙 출처는 보존 증거(승격 명세, 연구 변환, 봉인 import 문서)에서 오고, 출처가 없거나 catalog에 없는 pin은 읽지 않는다 | `tests/storage/test_read_heads.py::test_time_rule_provenance_comes_from_retained_evidence` | 구현 |
| DV-79 | `read_heads`는 fetch 전에 결과 크기를 SQL로 재어 할당을 넘으면 `ComputeResourceError`로 거부한다 | `tests/storage/test_read_heads.py::test_head_read_is_admitted_before_rows_are_fetched` | 구현 |
| DV-80 | inspection·연구 읽기는 grant와 무관하고 grant 하나는 그 규칙의 시점만 strict에 허용한다 | `tests/storage/test_read_heads.py::test_rule_grant_changes_strict_reads_only` | 구현 |
| DV-81 | strict 실행 준비는 사용한 `read_heads` 읽기 영수증을 run에 그대로 기록한다 | `tests/application/test_backtest_prepare.py::test_strict_preparation_records_head_read_receipt` | 예정 |
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
| DV-223 | `sec.submissions_filings@1`은 제출자 문서와 나열된 쪽의 공시를 member·배열 순서대로 원문 값의 행으로 편입한다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_reads_every_listed_filing` | 구현 |
| DV-224 | 없는 쪽, 나열되지 않은 쪽, 공시 수가 다른 쪽은 불일치 지표이고 `expect`에 적기 전까지 대조를 실패시킨다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_count_page_discrepancies` | 구현 |
| DV-225 | 알 수 없는 배열, 길이가 다른 배열, 다른 JSON 타입의 값은 submissions 단위를 거부한다 | `tests/storage/test_legacy_import.py::test_sec_submissions_filings_refuse_unknown_shapes` | 구현 |
| DV-226 | `sec.submissions@1`은 합성 원천을 독립 기대값과 같은 발행인 공시 행으로 옮기고, `filingDate`의 UTC 0시·New York 현지 0시인 접수 시각과 다른 표기는 null로 둔다 | `tests/storage/test_promotion_mappers.py::test_sec_submissions_maps_synthetic_fixture` | 구현 |
| DV-227 | `sec.companyfacts@1`은 사실마다 accession을 dimensions로 한 발행인 재무 행을 내고, 접수 시각을 pin한 공시 참조에서 accession으로 조인하며 없거나 서로 다른 접수 시각은 null이다 | `tests/storage/test_promotion_mappers.py::test_sec_companyfacts_maps_synthetic_fixture` | 구현 |
| DV-228 | SEC 공시는 기록된 접수 시각을 시점으로 승격되고 공동 제출자는 따로 record이며, 발행인 매퍼의 명세는 identity snapshot을 pin하지 않는다 | `tests/storage/test_sec_promotion.py::test_filings_take_the_recorded_acceptance_instant` | 구현 |
| DV-229 | SEC 재무는 pin한 공시 generation의 접수 시각부터 알려지고, 조인되지 않은 사실은 시점이 null이며, 같은 원천의 재승격은 빈 delta다 | `tests/storage/test_sec_promotion.py::test_facts_are_known_from_their_filing_acceptance` | 구현 |
| DV-230 | 매퍼 참조 pin은 parent의 generation이나 그 후손으로만 옮겨지고, 후손 공시 generation이 접수 시각을 준 사실은 SUPERSEDE다 | `tests/storage/test_sec_promotion.py::test_a_later_filings_generation_completes_unmatched_facts` | 구현 |
| DV-231 | 같은 accession의 바뀐 값은 SUPERSEDE이고 그 시점은 공시 접수 시각이며, 명세 파티션은 공시일로 사실을 고른다 | `tests/storage/test_sec_promotion.py::test_a_changed_value_of_one_accession_supersedes_and_partitions_select_by_filing` | 구현 |
| DV-232 | 매퍼 참조는 참조 도메인의 dataset generation만 pin할 수 있다 | `tests/storage/test_sec_promotion.py::test_a_filings_reference_must_pin_a_filings_generation` | 구현 |
| DV-233 | `aas-dimensions-v1` 형식은 고정 입력과 기대 값으로 고정돼 있고 SQL 계산이 Python과 같다 | `tests/storage/test_promotion_formats.py::test_dimensions_hash_format_is_frozen` | 구현 |
| DV-234 | SQL의 JSON 문자열 표기는 모든 code point에서 `json.dumps`와 같다 | `tests/storage/test_promotion_formats.py::test_json_string_sql_matches_json_dumps` | 구현 |
| DV-235 | SEC가 두 번 나열한 같은 공시는 한 번 읽히고, 값이 다른 같은 accession의 행은 둘 다 매핑되어 승격을 거부한다 | `tests/storage/test_sec_promotion.py::test_a_repeated_listing_is_read_once` | 구현 |
