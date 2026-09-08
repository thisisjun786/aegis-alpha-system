# AAS decisions

현재 설계 결정의 index다. 파일명은 `NNNN-slug.md`, 상태는
`draft | accepted | superseded`를 사용한다. accepted는 오너 지시가 있는 결정에만 쓴다.
코드·config·lock이 소유하는 버전과 pin은 이곳에 복제하지 않는다.

| 결정 | 상태 | 책임 |
| --- | --- | --- |
| [0009](0009-standalone-three-modules.md) | accepted | 독립 CLI와 이지스·알파·헷지 모듈 |
| [0010](0010-backtest-data-foundation.md) | accepted | 필수 데이터 모델·시점·원장·사용 자격 |
| [0011](0011-public-engine-private-strategies.md) | accepted | 공개 엔진과 비공개 전략 DB, Apache-2.0 |
| [0012](0012-retire-legacy-runtime.md) | accepted | 구버전 실행 도구 제거, 데이터 계약 보존 |
| [0013](0013-first-install-workspace.md) | accepted | 로컬 사용자 저장소, 단일 앱·선택적 컨테이너 설치와 백업 |
| [0014](0014-local-embedded-databases.md) | accepted | SQLite 상태·비공개 전략, DuckDB 시장 저장·분석; 0010의 저장 기술 대체 |

구버전 결정과 운영 기록은 비공개로 보관한다. 현재 작업은 이 index와
[POLICY.md](../../POLICY.md)를 따르며 과거 이력에서 폐기된 요구사항을 되살리지 않는다.
식별자·시점·해시·결측·권한·불변 publication의 실패 조건은 현재 코드와 테스트가 유지한다.
