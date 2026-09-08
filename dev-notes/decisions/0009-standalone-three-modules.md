---
title: "Standalone Aegis, Alpha and Hedge application"
status: accepted
date: 2026-09-05
---

# 독립 AAS 앱

2026-09-08: [0015](0015-research-engine-product-boundary.md)가 아래의 CLI 중심 제품 설명을
외부 도구용 데이터·연구 엔진 목표로 대체한다. 현재 CLI·세 모듈 계약과 외부 실행 플랫폼
비필수 원칙은 유지한다. 아래 본문은 이 결정이 채택된 당시의 방향이다.

AAS는 CLI를 첫 사용 표면으로 삼는 독립 앱이다. 이지스는 자산배분, 알파는 개별 종목,
헷지는 보호 목적 배분을 맡고 포트폴리오 계층이 명시적인 모듈 예산과 결과를 합성한다.
외부 실행 플랫폼이나 에이전트를 필수로 설치하지 않는다.
책임과 현재 구현 상태는 [architecture](../architecture.md)가 소유한다.

이 결정은 과거 VT 중심 제품 경계를 대체한다. 로컬과 컨테이너에서 같은 CLI를 실행하며,
미리보기는 DB 없이 입력을 검증하고 비중을 계산한다. DB 계층은
[0010](0010-backtest-data-foundation.md)을, 실제 전략의 비공개 경계는
[0011](0011-public-engine-private-strategies.md)을 따른다.

외부 구현을 도입하면 원본 revision·라이선스·변경·동작 검증을 남긴다.
구버전 코드의 제거와 유지되는 데이터 계약은
[0012](0012-retire-legacy-runtime.md)가 소유한다. DB 복원, 수집 재개, 실주문과
배포는 설계 문서나 미리보기 성공만으로 승인되지 않는다.
