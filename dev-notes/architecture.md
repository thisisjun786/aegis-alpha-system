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
| `aas backtest` | 명시한 ETF 목표 비중의 다음 거래일 시가 체결·비용·NAV 계산 | 원본 전략 규칙 자동 해석·제출 가격의 출처 및 시점 인증 |
| `aas prepare` | 등록 전략과 고정 pin에서 판단별 목표 비중을 계산해 `aas backtest` 봉투와 출처 sidecar로 내보내기 (SELECT만) | 체결·NAV 계산, run·요청 등록, 결과 저장·복원, 원본 자료의 PIT 인증 |
| `aas run` | 준비·요청 등록·`open_run`·잠금 없는 회계·`commit_run`을 한 명령으로 잇고 run ID로 결과 조회 (`db run-install` 필요) | 원본 자료의 PIT 인증, 알파·헷지 전략 실행, 실주문 |
| `aas run research` / `aas run rerun` | 선언 문서 하나로 준비·선언 등록·`open_run`·회계·`commit_run`을 잇고, 봉인한 증거에서 결과와 준비를 다시 만들어 대조 (`db run-install`과 `db run-migrate` 필요) | 관측 자료의 실행 자격, 원본 동치, PIT 인증 |
| `aas init/doctor/db/strategy/data` | `storage/`의 내장 DB 설치·등록·조회·복구, 관례·pin 문서 등록, run 추가 스키마 설치, 기록한 run을 담은 백업과 새 home 복원 | 실행 자격 부여, 기존 home 덮어쓰기 |
| `aas providers/collect` | 기존 공급자·예산·실행 영수증 도구 | 수집기의 내장 DB 이식·스케줄러 자동 활성화 |

외부 도구는 현재 Python 계산 API 또는 CLI를 재사용할 수 있다. 모든 저장·수집 기능이
하나의 안정된 외부 API로 통합됐다는 뜻은 아니다. HTTP/MCP 서버는 제공하지 않는다.

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

`engine/requirements.py`는 검증된 bundle에서 실행 정의(`ExecutionDefinition`)를 결정적으로
도출한다. 실제 가격·방어·현금 자산 ID, 역할별 입력 요구(최소 이력·시차·파생 입력),
달력·stale gate 규약, 그리고 필수 관례 역할 `calendar`·`basis`·`cost`·`execution`을
담는다. 저장된 v1 `strategy_requirements` 행은 같은 도출의 투영이며 의미가 바뀌지 않는다.
bundle 자체에는 가격 기준(basis)이 없으므로 모든 역할이 미해결이고 `executable=false`다.
정의를 조회할 수 있다는 사실과 실행 가능한 결합 입력이 있다는 사실은 다르다.
`storage/strategy_requirements.read_execution_definition`은 SELECT만 수행하며, 선택적
`ConventionPin` 결합을 state DB의 등록 관례와 대조한다. 호환되는 basis pin만 가격 요구에
반영되고 나머지 역할은 미해결로 남으며 `executable`은 여전히 false다. `capital` 요구에
`total_return` 기준을 결합하면 거부한다. 관례 등록은 `aas data convention-import`와 Python
`register_convention`·`read_convention`이 맡는다. 전략 계보는 새 버전 등록 시에만 선택적으로
기록하며 등록된 부모가 없으면 `unresolved`로 남고 이후 부모 등록으로 바뀌지 않는다.

`application/backtest_prepare.py`는 `strategy show`와 같은 SELECT-only 경로로 등록 전략을
읽고, 요청이 고정한 관례·세션·가격·identity·universe·membership과 조건부 거시·파생·프록시
pin을 실제 저장 소유자에서 검증한 뒤 판단 슬롯마다 `engine.replay`를 호출한다.
`engine/schedule.py`가 pin된 세션과 지연·지식 상한에서 판단·체결 쌍을 만들고,
`engine/backtest_request.py`가 요청 스키마, 의미 투영 해시(`request_hash`), 기존 봉투
내보내기를 소유한다. 엔진은 저장소·DuckDB·환경을 import하지 않고 application이 계산 소스
해시와 Python·Decimal 컨텍스트 정체성을 넘긴다. 결과는 `aas backtest`가 읽는 봉투와
`aas-prepared-backtest-v1` 출처 문서이며 `certified=false`다. 이 경로는 체결을 계산하거나
run·요청을 등록하지 않는다. `storage/input_pins.py`·`membership_pins.py`는 관례·정의·
identity·universe·입력 묶음 문서를 불변으로 등록·재해시하고, `storage/run_schema.py`와
`backtest_requests.py`는 후속 run 소비자를 위한 추가 스키마와 정규 요청 바이트 저장 API다.
요청 형식·등록 순서·실행 조건은 [operations](operations.md#저장한-전략과-입력의-준비)가 소유한다.

같은 모듈의 `prepare_research_run`은 선언된 미인증 연구 실행을 맡는다. 보존 관측 패널은
실행 경로에 오를 수 없고 이 함수도 그 길을 열지 않는다. 가격 경로는 여전히 도메인에서 pin을
거부하고 엄격 PIT는 여전히 아무것도 고르지 않으며, 패널은 참조 자료 전용 판독기인
`load_pinned_observations` 로만 읽는다. `aas-backtest-request-v1` 은 체결 가격을
조정 없는 canonical 자료로 묶어 두므로 참조 관측을 체결 입력으로 이름 붙일 수 없고, 그 거부는
그대로 둔다. 그래서 선언 문서 `aas-research-run-v2` 가 달력·membership·기간·이력·계정
조건을 직접 들고 실행 근거가 된다. 패널은 세션 날짜만 있고 시각이 없으므로 달력은 관측된 날짜 집합으로
선언하고(`observed-sessions-date-only`), 일정도 그 날짜 위에서 만든다. 결과는 `aas backtest` 봉투와
`aas-prepared-research-run-v1` 봉인 문서이며, 실행 모드는 `research-uncertified` 로
고정돼 소비자가 뒤집을 수 없다. run 식별자는 선언과 그것이 만든 봉투의 내용에서 나오므로 같은
선언은 같은 run을 가리키고, 복원한 설치본에서 바이트 단위로 재현된다. 월별 기준 실행은
`fill-next-session-open-v1` 을 이름으로 선언하고 일별 기준 실행은 체결 시점을 미해결로
남긴다. 어느 쪽도 원본 동치를 주장하지 않으며 봉인 문서의 `source_parity` 는 `unknown` 이다.
정식 run 기록은 run add-on 버전에 달려 있다. `run_details` 의 `request_schema` 는 명시적 허용
목록이고 v1은 `aas-backtest-request-v1` 하나만 담았으므로, 선언된 연구 실행을 기록하려면
`aas db run-migrate` 가 설치본을 먼저 넓힌다. 그 명령은 백업·내구 의도·한 트랜잭션짜리 리빌드·
검증·완료 순으로 움직이고, 중단되면 같은 명령을 다시 부르거나 `aas db recover` 가 이미 내려앉은
리빌드를 마무리한다. 옛 CHECK 아래 기록된 run은 그대로 옮겨져 계속 읽힌다. 마이그레이션 뒤에는
선언 자체가 run의 등록 요청이 되고, 봉인한 준비 문서가 엔진·환경 신원을 대신 들며, run ID 재조회와
저장된 봉투로부터의 재실행, 백업과 새 루트 복원이 그 run을 그대로 포함한다. 기록은 가독성이지
자격이 아니다: 저장한 run은 자기 스키마를 밝히고 `research_only` 로 읽히며, 엄격 경로의 거부는
하나도 풀리지 않는다.

이 수명주기를 실행하는 것은 `aas run research` 한 명령이다. 선언 파일과 그 파일의 SHA-256을
받아 준비·선언 등록·run 개시·회계·확정까지 잇고, 슬리브 선언과 표본 조합 선언을 모두 받는다.
run ID는 받지 않는다. 식별자는 선언이 정하므로 호출자가 이름을 고를 수 있으면 한 계산을 다른
이름으로 접수하는 길이 열린다. 다시 읽는 것은 `aas run show`가 그대로 맡는다. 기록한 run은
어느 계약으로 열렸는지 자기 행에서 밝히므로 판독기를 하나 더 두면 같은 질문에 답이 둘이 된다.
`aas run rerun`은 봉인한 봉투를 같은 회계에 다시 넣어 저장한 결과 바이트와 맞춰 본다. 선언을
함께 주면 준비까지 다시 해서 run 식별자·봉투·봉인 문서를 각각 비교하고, 결과 재현과 준비
재현은 서로 다른 주장이므로 따로 보고한다. 표본 조합은 묶음 층에서 membership을 묶지 않는다.
묶음 어휘가 membership 하나만 들 수 있어 두 슬리브 중 하나만 묶으면 절반짜리 묶음이 완전해
보이므로, 조합은 아무것도 묶지 않고 선언의 내용 해시가 둘을 함께 덮는다. 영수증은 그 약한
결속을 그대로 적는다.


## 데이터와 DB

신규 DB는 [0014](decisions/0014-local-embedded-databases.md)에 따라 **SQLite 두 파일과
DuckDB 한 파일**을 기본으로 사용한다. PostgreSQL과 Parquet를 기본 설치에서 제외한다.
종목 이력·시점 재생·원본 출처·버전·사용 자격 모델은 유지한다. 상세 테이블·키·시점·게시·복구 기준은
[백테스트 데이터 설계](design/backtest-data-foundation.md)가 소유한다.

```text
목표 연결: 외부 앱·에이전트
  → AAS의 데이터·연구 요청 경계 (준비·봉투 회계·run 확정까지 연결, 복원은 별도)
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
저장 전략과 고정 입력의 조합·계산은 `aas prepare`가, 그 결과의 run 확정과 재조회는
`aas run`이 맡는다. 결과 복원과 공급자 수집기의 전환은 별도로 검증한다.

`storage/source_library`는 기존 전략·연구·시장 자료를 조회할 수 있는 원본 자료실이다.
원본 SQLite의 테이블은 비공개 SQLite에, 명시적으로 전달한 Arrow 자료는 DuckDB에
원래 값과 출처를 보존한다. 별도 버전·체크섬을 가진 선택적 저장 확장이며 기존 core v1
스키마나 `EngineBundle` 계약을 바꾸지 않는다. 상태 DB의 저장 의도와 대상 DB의 완료
기록을 대조하고 내용·행 수를 검증한다. 원본 자료는 실행 전략이나 PIT 데이터 게시물로
자동 승격되지 않으며 `aas db sources/source-tables/source-read`로 조회한다.

`storage/source_reader`는 명시한 원본·테이블 해시와 완료 기록을 확인하고, 선택한 테이블의
내용 해시를 다시 계산한 뒤 제한된 크기로 읽는다. 다른 테이블 전체를 재검사하지 않는다.
읽은 자료가 시점에 적합하거나 거래 가능한지는 별도 입력 계약이 판단한다.

`storage/research_inputs`는 원본 자료실의 테이블을 해시로 고정한 변환 문서로만 시장
generation에 게시한다. 문서가 원본 pin, 대상 dataset, 열 연결, 종목, 기준·통화·역할, 달력과
숫자 변환 정책을 명시하고 원문은 `raw/`에 보존된다. 공통 revision 열은 원본에서 읽으며
만들어 채우지 않는다. 십진 변환은 정확해야 하고 일부만 있는 OHLCV는 거부한다. 프록시 지수
점은 `feature_values`에 DOUBLE로 저장하며 정의별 contract ID·버전과 입력 pin을
`feature_contracts`·`feature_inputs`에 남긴다. 프록시는 실행 불가이고 체결 재원은 실제 ETF
OHLC뿐이다. 관측 open·close만 있고 고가·저가·거래량이 없는 보존 연구 패널은 실행용
OHLC가 아니므로 `prices`에 들어가지 않는다. 정확한 DECIMAL(38,12) 승인과 부분 OHLCV
거부는 그대로 두고, 관측 연구 경로가 그 값을 `feature_values`에 binary64로 보존한다.
원본이 이미 binary64인 `ieee_float`는 비트가 그대로 남는다. `decimal_string`은
binary64가 표현하지 못하는 십진수를 변환하면서 반올림하며, 이는 표현 손실을 허용한다는
선언이지 몰래 일어나는 일이 아니다. 이 계약은 price_role을 reference로, certified를 false로 고정하고 값 범위를
명시하며, contract 이름은 series와 open·close 역할을 합쳐 두 역할을 서로 다른 행으로
남긴다. 공개 시각이 불명확하면 null로 남기고 거래일로 채우지 않는다. 패널은 여러 개의
제한된 generation으로 나뉘어 오므로 `feature_inputs`는 변환 하나를 고정하지 않고, 확장은
조상의 변환까지 같은 정의인지 확인한 뒤에만 게시된다. 검증은 그 chain이 끝까지 관측
generation인지도 확인하므로 다른 경로가 관측 head에 덧붙인 generation은 거부된다. 연구 수익률 프록시와 관측 가격은
계속 서로 다른 계약이다. `storage/market_inputs`는 명시한 generation pin과 compute 예산으로 전체 revision
chain을 준비하고 각 결정 시각을 그 chain에서 투영한다. strict PIT는 이후 revision, 알 수 없는
인지 시각, 참조 가격을 제외한다. 관측 연구 계약은 언제나 reference이므로 strict PIT는 인지
시각이 모두 알려져 있어도 한 행도 고르지 않는다. 명시한 observed snapshot 연구 모드는 인증되지 않은 채 남고
strict 읽기를 바꾸지 않는다. coverage는 요청 격자 전체를 기록하며 잘린 이력을 성공으로
돌려주지 않는다. 등록·조회 명령과 문서 필드는
[operations](operations.md#원본-자료의-연구-입력-등록과-고정-조회)가 소유한다. 이 경로는
원본의 진위나 PIT 자격을 인증하지 않으며 백테스트 입력으로 자동 승격하지 않는다.

`engine/execution.py`는 외부의 목표 비중·시가·종가·거래일과 비용을 받아 일별 NAV와 체결
원장을 계산한다. `aas backtest --input ... --sha256 ...`는 이지스의 ETF 양수 비중만
허용한다. 파일 해시는 제출한 입력의 동일성을 확인하며, 입력 가격의 출처를 인증하지
않는다. 따라서 이 명령은 `source_pins_verified=false`와 시점 미검증 상태를 명시한다.

`engine/proxy.py`와 `aas proxy`는 두 수익률 시계열을 명시한 전환일에서 이어 붙여
기준값 100의 연구 지수를 계산한다. 통화·가격/총수익 기준·종가 시각 규약이 같아야 하며,
전환일의 수익률을 중복 계산하거나 미래 ETF 가격으로 과거 수준을 맞추지 않는다.
비용은 선택한 모델에 따라 전환 전 기초 시계열에만 반영한다. 출력에는 거래 시가나
주문이 없으며 항상 `research_only=true`, `non_executable=true`다. 호출자가 붙인
종목 유형과 시점·출처 표시는 인증되지 않는다. 실제 ETF 체결의 상장일 제한은 유지한다.

`engine/reset_returns.py`의 Python 함수 `build_reset_returns`는 호출자가 지정한 각
기간마다 배율을 재설정하고, 기간 시작 NAV 대비 자금조달비·담보수익·보수를 각각
반영한다. 기초 수익률과 비용 입력의 날짜는 제출한 달력과 정확히 같아야 한다.
0 이하가 되는 자산 가치는 거부하며 청산이나 가격 보정을 추정하지 않는다. 출력은
연구 지수이고 거래 가격이 아니다. 달력·비용·원본 출처의 진위는 인증하지 않는다.

`engine/research.py`는 명시한 ETF 유형·후보 조합·평가 구간·비용 참조에서 중복 없는
연구 후보를 만들고, 호출자가 제공한 평가 함수로 학습·검증 결과를 모은다. 검증 결과의
승자만 별도 최종 평가에 넘기며 같은 부모의 평가 영수증이 있으면 반복 사용으로 표시한다.
호출자가 이전 기록을 누락했는지는 알 수 없어 전체 평가 이력은 미검증으로 남긴다.
`aas research`는 해시로 고정한 입력에서 후보 생성만 실행한다. 가격 평가·DB 저장은
시작하지 않는다. Python 평가 API는 호출자가 제공한 함수와 이전 영수증을 사용한다.

`execution.replay_next_open_cashflows`는 명시한 거래일 시가에서 납입·출금을 반영하고
직전 종가의 목표 비중을 실행한다. 입출금 직전 자산 가치로 펀드 단위를 발행·상환해
계좌 잔액과 입출금 영향을 제거한 단위당 NAV를 분리한다. 출금은 기존 현금만 사용하며
보유 자산을 자동 매도하지 않는다. 기존 `replay_next_open`과 백테스트 v1 출력은 유지한다.
백테스트 v2는 날짜별 현금 흐름을 명시해야 한다. 원본 전략의 납입일·환율·거래일 규칙을
추정하거나 원본 성과와의 일치를 인증하지 않는다.

`engine/risk.py`는 명시한 거래일과 종가에서 표본 공분산·변동성·역변동성 비중을
계산한다. 연율화 횟수, 두 자산의 비중 제한, 낙폭 고점·재진입·초기화 규칙을 호출자가
지정한다. 원본 전략의 미확정 규칙을 인증하지 않으며 이 함수만으로 주문을 만들지 않는다.

`engine/etf_candidates.py`는 명시한 ETF 프로필의 노출·통화·환헤지·배율·재설정 조건을
먼저 대조하고, 같은 기간의 추적오차·유동성과 보수를 비교한다. 누락되거나 오래된 자료는
판단 불가 사유로 남긴다. 반환값에는 입력 프로필과 비교 정책이 포함되며 종목을 자동
교체하지 않는다. 추적오차의 `basis`가 정책의 `tracking_basis`와 다르면 판단 불가로
남긴다. 기존 Python 호출의 두 기본값은 `net_total_return`이다. 다른 수익률 기준은
명시한 문자열이 정확히 일치해야 하며, 일치 자체가 원천 자료의 진위를 인증하지 않는다.
`aas etfs`는 해시로 고정한 프로필과 정책으로 같은 비교를 실행한다. 프로필 수집이나
정기 탐색 작업은 별도 실행 측에서 맡는다. 실행 측은 명시한 비교 그룹별로 기준지수와
후보를 분리하고, 과거 결과를 검증할 때 당시 설정과 자료의 해시를 유지한다. 그룹 추가가
전체 대상의 자료 확보나 자동 교체를 뜻하지는 않는다.

`data/nifty_history.py`는 외부에서 받은 Nifty 가격지수 응답의 원본 해시, 명시한 지수명,
조회 기간과 중복 날짜를 검증한다. 날짜·종가·원본 행을 불변 결과로 반환하며 네트워크나
DB에 접근하지 않는다. 가격지수 파싱은 ETF 조정가격·총수익·과거 가용 시각의 인증이 아니다.

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

Qveris는 별도의 네이티브 원문 수집 경로다. 작업 파일 해시를 확인한 뒤 명시한 도구와
하위 공급자만 호출하고, 호출 수·크레딧·HTTP 요청 수·시간을 제한한다. 원문과 사용량
정산을 보존하며 이 경로는 PostgreSQL 설정을 열지 않는다. `data/qveris_native.py`는
완료 원문의 해시와 정산 기록을 확인하고 가격과 격리 사유를 나눈다. 공급자 경고,
종목 식별 실패, 잘못된 가격은 실행 가격으로 승인하지 않는다. 원본 자료실 적재와
시점 조건을 갖춘 시장 데이터 게시는 별도 단계다.

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

[이지스 ETF 연구 계획](design/aegis-etf-research.md)은 원본 전략 실행, 내장 DB 갱신,
프록시 연구와 후보 탐색의 구현 순서를 정한다. 계획의 항목은 아래 현재 진입점의 구현
증거를 대신하지 않는다. 알파·헷지와 실주문은 이 계획에서 변경하지 않는다.

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
