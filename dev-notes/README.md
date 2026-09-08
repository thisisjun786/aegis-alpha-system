# AAS dev notes

문서마다 다음 질문을 맡는다.

| 문서 | 질문 |
| --- | --- |
| [`../README.md`](../README.md) | AAS는 지금 무엇인가 |
| [`architecture.md`](architecture.md) | 무엇이 존재하고 누가 소유하는가 |
| [`operations.md`](operations.md) | 오늘 무엇을 어떻게 실행하는가 |
| [`design/`](design/) | 상세 데이터 계약과 남은 구현은 무엇인가 |
| [`decisions/`](decisions/) | 왜 그렇게 결정했고 언제 폐기하는가 |

## 운영 규칙

1. version, digest, pin, permission 값은 code/config/lock/script가 기준이다.
2. 구조는 `architecture.md`, 명령은 `operations.md`, 상세 계약은 `design/`,
   결정 이유와 적용 범위는 `decisions/`가 소유한다. 구현 상태를 중복 서술하지 않고 연결한다.
3. 새 decision은 index와 같은 변경에서 추가한다.
4. 임시 decision은 machine-checkable removal condition을 가진다.
5. 운영 기록과 전략 원문은 비공개로 보관한다.
6. 현재 결정과 supersession은 `decisions/README.md`에서 관리한다.
7. `[owner]` 권한이 필요한 단계는 명시적으로 표시한다.

## Private history

Historical operation records and strategy-specific evidence are preserved outside
this checkout. Current architecture and generic failure contracts remain here.
See [0011](decisions/0011-public-engine-private-strategies.md).
