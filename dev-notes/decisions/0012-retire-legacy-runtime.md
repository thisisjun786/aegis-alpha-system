---
title: "Retire superseded runtime implementations"
status: accepted
date: 2026-09-07
---

# 구버전 실행 코드 제거

오너는 독립 AAS 재설계에 맞춰 구버전 코드와 개인 운영 설정을 공개 후보에서 제거하도록
지시했다. 이 결정은 과거 구현을 공개 트리에 계속 보존하던 지침을 대체한다.
[0009](0009-standalone-three-modules.md)의 앱 경계,
[0010](0010-backtest-data-foundation.md)의 데이터 계약,
[0011](0011-public-engine-private-strategies.md)의 전략 분리는 유지한다.

추가 결정 [0014](0014-local-embedded-databases.md)는 신규 저장소를 SQLite·DuckDB로 선택했다.
아래의 SQLite 제거는 당시 구버전 구현의 폐기를 뜻한다. 현재 PostgreSQL·Parquet 경로도
새 계약의 검증과 호출자 전환이 끝나면 제거하며, 이 문서는 영구 보존을 요구하지 않는다.

## 남기는 것

- 독립 CLI, 이지스·알파·헷지 계약, 포트폴리오 합성, 외부 입력을 받는 범용 엔진.
- 현재 FMP·FRED/ALFRED·SEC·FinImpulse 수집기와 명시적인 권한·예산·복구 검증.
  FMP의 일회 수집·과거 데이터 수집 명령도 유지한다.
- 종목 식별, 원본 출처, 불변 파일, 카탈로그, 고정 버전·해시·시점 조회.
- DB 설치·검증된 데이터 채택, 현재 migration chain과 DB 제약 테스트.
- 현재 구현의 회귀 테스트, 합성 예제, 출처·라이선스 고지.

## 제거하는 것

구형 canonical build/publish/postflight/rebind 실행 도구, 특정 Norgate snapshot에
고정된 import/bootstrap 도구, SQLite persistence와 package-root facade,
VT·broker 전용 환경·patch·실행기는 제거한다. 그 구현만 검증하던 테스트와 사용하지
않는 fixture도 함께 제거한다. 과거 운영 보고서·개인 경로·계정·서명자 설정·실제 전략
자료는 비공개 보관소가 소유한다. 구버전 호출 경로의 호환 wrapper는 제공하지 않는다.

폴더 이름만 바꾸거나 사용하지 않는 코드를 별도 공개 legacy 패키지에 남기는 방법은
채택하지 않았다. 사용 경로와 신뢰 경계를 계속 복잡하게 만들기 때문이다.

## 유지되는 데이터 의미

가격 조회가 사용하던 Arrow schema는 `data/price_schema.py`로 분리했다.
필드·타입·metadata·직렬화 결과가 이전 schema와 같아 기존 파일을 읽을 수 있다.
SEC 정책 검사는 독립된 소유 모듈에서 검토된 registry 해시를 확인한다.
기본 정책에는 수집·백테스트·주문 권한을 주지 않으며 운영자 승인은 외부 입력으로 받는다.

현재 migrations와 데이터 bytes는 변경하지 않는다. generation SQL 모델과 제약은
설치·채택 호환성 때문에 남긴다. 그 테이블이 존재해도 제거한 게시 실행기가 작동한다는
뜻은 아니다. 과거 데이터 조회·수집 복구·실제 전략 계산의 검증 범위는 각각 구분한다.

## 검증과 이전

제거 전 파일과 Git 이력은 해시를 확인한 비공개 백업으로 보존한다. 공개 이전은 검토한
현재 파일만 새 Git 이력으로 가져간다. 기존 레포의 공개 설정을 바꾸지 않는다.
남은 import, 합성 입력 테스트, 일회용 PostgreSQL, 설치 wheel/sdist와 AAS 이미지를
검사한다. 배포 파일에는 실제 전략 DB·개인 설정·개발 대화 기록을 포함하지 않는다.
자동 검사만으로 임의의 개인정보가 모두 없다고 단정하지 않는다.
