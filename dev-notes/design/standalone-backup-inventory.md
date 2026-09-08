# 구형 PostgreSQL 백업 검토 기준

이 문서는 전환 전 PostgreSQL 백업을 검토할 때의 조건이다. 현재 내장 SQLite·DuckDB
백업·복원 명령은 [운영 안내](../operations.md#검사복구백업)를 따른다. `aas db restore`는
PostgreSQL archive나 dump를 읽는 명령이 아니다. 이 파일에는 실제 백업 재고를 기록하지 않는다.

실제 백업 위치, 호스트 구성, 수집 재고와 검증 영수증은 비공개 운영 저장소에 둔다.
이 레포에는 복구 절차와 검증 코드를 둔다.

물리 PostgreSQL archive와 논리 dump는 서로 다른 형식이다. 파일 이름이나 크기로
운영 후보를 결정하지 않는다. checksum, archive 구성, 서버 버전과 당시 mount 기록을
검증하고, 별도 디렉터리와 DB에서 복원한 뒤 schema·행 수·manifest·대표 조회를 비교한다.

목록 확인이나 정상 기동만으로 복원 성공을 판정하지 않는다. 운영 DB에 직접 덮어쓰지
않는다. `legacy-db adopt`는 지원하는 snapshot의 검증·채택 경로이며 물리 archive 복원
명령이 아니다. 실제 PostgreSQL 복원 절차와 대상 선택은 별도 운영 범위에서 검증한다.
[전환 도구 안내](../operations.md#전환-중인-공급자-도구)는 현재 명령의 경계만 설명한다.
