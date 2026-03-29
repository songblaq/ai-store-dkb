# AI Store DKB

## What is this?
An AI ecosystem research and collection store — a DKB instance specialized for collecting AI agents, skills, plugins, workflows, and tools from the open-source ecosystem.

## Tech Stack
- Python 3.12+
- Depends on: `dkb-runtime` (from git)
- PostgreSQL 16+ with pgvector

## Setup
```bash
pip install -e ".[dev]"           # Install (pulls dkb-runtime)
cp .env.example .env              # Configure
docker compose up -d postgres     # Start database
python scripts/collect_all.py     # Run collection
python scripts/run_pipeline.py    # Run full pipeline
python scripts/export_catalog.py  # Export results
```

## Project Structure
```
config/
  instance.yml      # Instance metadata (name: ai-store)
  sources.json      # 13 collection sources across 7 categories
  categories.yml    # Category metadata and trust levels
scripts/
  collect_all.py    # Entry point for collection
  run_pipeline.py   # Full pipeline: collect -> extract -> canonicalize -> score -> verdict
  export_catalog.py # Export to dist/catalog/
```

## Categories (from sources.json)
- agent-skills-official: Anthropic, Block, Microsoft, Vercel, Google
- claude-code: awesome-claude-code, oh-my-claudecode, gstack
- codex: awesome-codex-subagents
- agent-skills-community: awesome-agent-skills, agency-agents
- mcp: MCP official servers, awesome-mcp-servers

## Ecosystem
Upstream collection store. Data flows to agent-prompt-dkb for curation.
