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

## 전환 중인 공급자 도구

`aas providers`와 기존 수집기는 유지되지만 일부는 아직 PostgreSQL/Parquet adapter를 쓴다.
이 경로는 `uv tool install '.[legacy]'` 또는 개발 환경과 명시한 이전 설정이 필요하다.
기존 DB 명령은 `aas legacy-db`, publication 조회는 `aas legacy-data`로 구분한다.
이 도구를 새 `state.sqlite3`에 연결하거나 실제 보관 데이터를 자동 채택하지 않는다.
`docker-compose.data.yml`과 이전 설치 실행기는 이 전환 경로이며 새 설치 절차가 아니다.

라이브 공급자 검증·기존 DB 이전·스케줄러 활성화·실주문은 위 오프라인 설치 검사의 범위에
포함되지 않는다. CLI preview는 합성 비중 계산이고 전체 백테스트는 아직 별도 구현이다.
