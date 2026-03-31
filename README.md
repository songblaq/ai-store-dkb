![CI](https://github.com/songblaq/ai-store-dkb/actions/workflows/ci.yml/badge.svg)

# ai-store-dkb

AI 생태계 전체를 리서치·수집하는 **DKB 스토어 인스턴스**.

## What is this?

ai-store-dkb는 AI 에이전트, 스킬, LLM 도구, 프레임워크 등 오픈소스 생태계 전반을 수집·관리합니다. dkb-runtime 엔진으로 운영됩니다.

## Part of DKB Ecosystem

| Repository | Role |
|---|---|
| [directive-knowledge-base](../directive-knowledge-base) | 개념, 명세, 웹 문서 |
| [dkb-runtime](../dkb-runtime) | 설치 가능한 구현체 |
| **ai-store-dkb** (this) | AI 리서치/수집 스토어 |
| [agent-prompt-dkb](../agent-prompt-dkb) | 에이전트 프롬프트 큐레이션 |

## Categories

- **Official Agent Skills**: Anthropic, Microsoft, Google, Vercel, Block
- **Claude Code Extensions**: Plugins, skills, subagents, hooks
- **Codex Extensions**: Subagents for OpenAI Codex
- **Community Agent Skills**: Curated lists and collections
- **MCP Servers**: Model Context Protocol servers
- **LLM Tools**: vLLM, Ollama, etc.
- **AI Frameworks**: Agent orchestration frameworks

## Quick Start

```bash
# Prerequisites: dkb-runtime installed, PostgreSQL running
pip install -e .
cp .env.example .env
docker compose up -d postgres

# Collect sources
python scripts/collect_all.py

# Run full pipeline
python scripts/run_pipeline.py
```

## License

MIT
