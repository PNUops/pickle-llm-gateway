# pickle-llm-gateway

부산대학교 클라우드 플랫폼(PNU Cloud)의 교내 LLM API 게이트웨이입니다. 학생 코드가 보내는
OpenAI 호환 요청을 받아 API Key를 검증하고 사용량 한도를 적용한 뒤, 업스트림 모델 서버로
전달합니다.

## 요청 경로

```
학생 코드 (OpenAI SDK + API Key)
    │  POST /v1/chat/completions, GET /v1/models
    ▼
llm-gateway ── 키 검증 · 한도 적용 · 모델명 변환 · 사용량 기록
    │
    ▼
업스트림 모델 서버 (OpenAI 호환 API)
```

학생 코드에는 공개 모델명만 보입니다. 실제 모델과 업스트림 주소는 게이트웨이 설정에만
있으므로, 업스트림을 교체해도 학생 코드는 바뀌지 않습니다.

## 주요 기능

이 게이트웨이가 맡는 부분은 아래와 같습니다.

- **API Key 검증**: 발급된 Key만 통과시키고, 폐기와 정지, 만료 상태를 반영합니다.
- **사용량 한도**: Key마다 분당 요청 횟수와 토큰 사용량, 동시 요청 수를 제한합니다.
- **모델 이름 분리** — 요청과 응답의 모델명을 공개 이름으로 유지합니다.
- **사용량 계측**: 요청마다 토큰 사용량과 처리 결과를 기록합니다. 프롬프트와 응답 본문은
  기록하지 않습니다.

## 동작 방식

인증 상태는 스냅샷 문서 한 개로 관리합니다. 어떤 Key가 있고 어떤 모델을 서빙하며 한도가
얼마인지가 JSON 문서 하나에 들어 있고, 게이트웨이는 이 문서를 통째로 읽어 원자적으로
교체합니다. 문서가 바뀌면 폴링 주기 안에 반영되므로, Key 폐기는 수 초 안에 적용됩니다.
세대 번호가 뒤로 가는 문서는 거부합니다. 폐기를 조용히 되돌릴 수 있기 때문입니다.
최고 세대 번호는 문서 옆 `snapshot.json.highwater` 파일에 남으므로, 재시작 뒤에 예전
문서가 되살아나도 거부합니다. 모델의 `upstreamRef`가 설정에 없는 업스트림을 가리키면
그 문서는 적재 자체를 거부해, 오타가 요청 시점의 오류가 아니라 적재 시점에 드러납니다.

현재는 로컬 파일을 폴링합니다. HTTP 동기화는 아직 제공하지 않습니다.

요청 처리는 이렇게 흐릅니다. Authorization 헤더의 Key를 sha256으로 해시해 스냅샷에서
찾고, 한도를 확인한 뒤 요청 본문의 모델명을 실제 모델로 바꿔 업스트림에 전달합니다.
스트리밍 응답은 조각 단위로 그대로 흘려보내면서 모델명만 공개 이름으로 되돌립니다.
지원하지 않는 파라미터는 이름을 밝혀 거부하므로, 업스트림을 바꿔도 학생 코드가 보내는
요청의 의미는 달라지지 않습니다.

토큰 사용량은 업스트림이 돌려주는 usage 값으로 기록합니다. 스트리밍 요청은 업스트림에
`stream_options.include_usage`를 강제로 켜서 마지막 조각의 사용량을 받되, 학생이 직접
요청하지 않았다면 그 조각은 계측에만 쓰고 응답으로는 보내지 않습니다. 사용량을 얻지
못한 채 끝난 요청은 크기 기반 추정치를 `estimated` 표시와 함께 기록합니다. 기록은 하루 단위
JSONL 파일에 쌓이며, 항목마다 고유한 `eventUuid`가 있어 이후 수집이 중복 없이
재시도될 수 있습니다.

오류 응답은 OpenAI와 같은 형식이며 메시지는 한국어입니다. 업스트림 오류의 본문과 주소는
외부 응답에 싣지 않습니다.

## API Key 발급

`llm-keygen`이 Key 평문을 한 번만 출력하고, 스냅샷에는 해시만 넣습니다.

```bash
go run ./cmd/llm-keygen -snapshot /var/lib/pickle-llm-gateway/snapshot.json \
  -expires-days 90 -rpm 20 -tpm 20000 -concurrency 2
```

분실한 Key는 다시 조회할 수 없습니다. 스냅샷에서 해당 항목의 `status`를 `REVOKED`로
바꾸고 새로 발급합니다.

## 시작하기

Go 1.26 이상이 필요합니다.

```bash
go build ./...          # 빌드
bash scripts/verify.sh  # 셸 린트, Go 포맷·정적 검사·테스트, 게시 위생 검사
```

로컬 실행 예시:

```bash
export LLMGW_LISTEN=127.0.0.1:8081
export LLMGW_SNAPSHOT_PATH=./snapshot.json
export LLMGW_SPOOL_DIR=./spool
export LLMGW_UPSTREAM_MAIN_BASE_URL=http://198.51.100.10:8000/v1
go run ./cmd/llm-gateway
```

## 구성

필수 값이 하나라도 비면 실행되지 않습니다.

| 환경 변수 | 필수 | 설명 |
|---|---|---|
| `LLMGW_LISTEN` | ✔ | 수신 주소 (`127.0.0.1:8081`) |
| `LLMGW_SNAPSHOT_PATH` | ✔ | 스냅샷 문서 경로 |
| `LLMGW_SPOOL_DIR` | ✔ | 사용량 기록 디렉터리 |
| `LLMGW_UPSTREAM_<REF>_BASE_URL` | ✔ | 업스트림 주소. `<REF>`는 스냅샷의 `upstreamRef`와 대소문자 무관하게 대응 |
| `LLMGW_UPSTREAM_<REF>_API_KEY` | | 업스트림 인증 토큰. 비우면 인증 없이 호출 |
| `LLMGW_UPSTREAM_<REF>_CAP_FIELD` | | 출력 상한을 주입할 필드. 기본 `max_completion_tokens`, 구형 서버는 `max_tokens` 지정 |

<details>
<summary>조정 값 (기본값으로 동작)</summary>

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `LLMGW_SNAPSHOT_POLL_INTERVAL` | `5s` | 스냅샷 변경 확인 주기 |
| `LLMGW_REQUEST_BODY_MAX_BYTES` | `2097152` | 요청 본문 크기 상한 |
| `LLMGW_UPSTREAM_HEADER_WAIT` | `60s` | 업스트림 응답 시작 대기 상한 |
| `LLMGW_REQUEST_MAX_DURATION` | `10m` | 요청 한 건의 전체 시간 상한 |
| `LLMGW_MAX_IN_FLIGHT` | `16` | 게이트웨이 전체 동시 요청 상한 (호스트 메모리에 맞춰 조정) |
| `LLMGW_DEFAULT_RPM` | `20` | Key에 한도가 없을 때의 분당 요청 수 |
| `LLMGW_DEFAULT_TPM` | `20000` | Key에 한도가 없을 때의 분당 토큰 수 |
| `LLMGW_DEFAULT_CONCURRENCY` | `2` | Key에 한도가 없을 때의 동시 요청 수 |
| `LLMGW_ALLOW_GENERATION_RESET` | (off) | 스냅샷 세대가 기록된 최고값보다 낮아도 적재 허용(의도적 세대 초기화 시에만) |

</details>

스냅샷 문서 예시:

```json
{
  "generation": 1,
  "serviceEnabled": true,
  "models": [
    {
      "publicName": "pnu-general",
      "upstreamRef": "main",
      "upstreamModel": "example-model-32b",
      "maxOutputTokens": 4096
    }
  ],
  "keys": [
    {
      "keyId": "key-0a1b2c3d4e5f",
      "tokenHash": "<sha256 hex>",
      "status": "ACTIVE",
      "expiresAt": "2026-11-08T00:00:00Z",
      "limits": { "rpm": 20, "tpm": 20000, "concurrency": 2 }
    }
  ]
}
```
