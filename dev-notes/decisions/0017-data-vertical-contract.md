---
title: "Data vertical: versioned time rules with consumer grants"
status: accepted
date: 2026-10-03
---

# 데이터 수직: 시간 규칙 grant, 공급자별 dataset, 증명된 은퇴

원천 자료실의 공급자 자료를 시점 조건이 있는 typed generation으로 승격하는 경로를 하나로
정한다. 상세 계약과 검증 테스트는 [데이터 수직 계약](../design/data-vertical.md)이 소유한다.
데이터 의미·시점·계보 요구는 [0010](0010-backtest-data-foundation.md), 저장 기술은
[0014](0014-local-embedded-databases.md), 계산 할당은 [0016](0016-maintenance-admission-budget.md)을 따른다.

## 시간 규칙은 시점 근거다

날짜 단위로만 공개 시점을 알 수 있는 원천에는 버전 있는 시간 규칙(`session_close_plus_lag`,
`local_day_end`, `exdate_open`)이 `available_at_us`와 `revision_known_at_us`를 정한다. 규칙 값은 실제
공개 시각보다 이르지 않은 보수적 상한이고, 규칙의 ID와 버전은 승격 명세와 transform hash에 들어간다.
수집 시각으로 시점을 채우지 않는다는 0010의 요구는 그대로다.

규칙에서 나온 시점을 strict 경로에 쓸지는 소비자가 binding의 grant(허용 규칙 `id@version` 목록)로
정하고, run 영수증이 그 grant를 기록한다. grant가 없는 규칙의 시점은 알 수 없는 시점과 같게 취급된다.
규칙 행을 막는 기본 거부 대신 허용과 기록으로 책임을 남긴다.

## 공급자별 dataset과 소비자 cutover

dataset은 `<domain>.<market>.<provider>[.ref]`이고 한 chain에 한 공급자만 들어간다. 공급자를 잇는
일은 소비자 binding의 순서 있는 pin과 cutover 구간이 한다. 공급자 정정이 다른 공급자의 이력에
섞이지 않고, 공급자 교체는 새 binding이다. FMP는 구독이 끝난 동결 원천이며 reference 역할로만
승격한다.

## 결정적 승격

승격은 해시로 고정한 `aas-promotion-v1` 명세 하나로 요청하고, 공통 revision 열은 원천과 공개된
규칙에서만 계산한다. 원천 값을 바꾸는 숫자 규칙(`krw_tick@1` 등)은 명세에 이름으로 선언하고 행마다
`quality_flags`를 남긴다. 새 rowset 해시 형식은 만들지 않고 `aas-rowset-v1`과 parity를 유지한다.

## 스키마 v2와 은퇴

market v2는 `filings`, `classifications`, `quality_flags`, `prices.fields`를, state v2는
`source_retirements`를 더한다. 업그레이드는 백업이 있어야 시작하고 중단되면 재개한다.

대체된 원천 자료실 테이블은 참조 없음·동치 digest·다른 장치 백업을 모두 통과하면 일괄 은퇴하고
기록과 함께 보고한다. `raw/`의 원본 bytes와 보관 archive는 은퇴 대상이 아니다.

## 수집 범위

FRED/ALFRED, SEC 일간, DART 공시 목록과 분기 rolling cohort, Qveris 분할·배당·환율, KIND 상장·업종
수집을 승인한다. 유료 호출은 설정이 정한 실행별 상한과 남은 잔액 안에서만 하고, 모든 시도는
`collection_attempts`·`usage_events`에 기록한다. 상한 값은 설정 파일이 소유한다.

## 범위 밖

이 결정은 구현 완료를 뜻하지 않는다. 각 계약의 구현 상태는 대응표의 테스트가 판정한다.
실주문, 전략 자격, 비공개 자료의 위치는 각자의 결정이 소유한다.
