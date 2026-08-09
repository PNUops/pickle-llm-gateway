# pickle-llm-gateway

부산대학교 클라우드 플랫폼(PNU Cloud)의 교내 LLM API 게이트웨이입니다. 학생 코드가 보내는
OpenAI 호환 요청을 받아 API Key를 검증하고 사용량 한도를 적용한 뒤, 업스트림 모델 서버로
전달하는 데몬입니다.

현재는 초기 구성 단계입니다. 게이트웨이 동작은 아직 제공하지 않습니다.

## 시작하기

Go 1.26 이상이 필요합니다.

```bash
go build ./...          # 빌드
bash scripts/verify.sh  # 셸 린트, Go 포맷·정적 검사·테스트, 게시 위생 검사
```
