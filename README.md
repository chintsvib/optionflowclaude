# OptionFlowClaude

A project built on the **WAT framework** (Workflows, Agents, Tools) - an architecture that separates probabilistic AI reasoning from deterministic code execution.

## Project Structure

```
.tmp/               # Temporary files (scraped data, intermediate exports)
tools/              # Python scripts for deterministic execution
workflows/          # Markdown SOPs defining operational procedures
.env                # API keys and environment variables (not tracked)
CLAUDE.md           # Agent instructions and framework documentation
```

## Getting Started

1. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and add your API keys
   ```

2. **Install dependencies** (as needed)
   ```bash
   pip install -r requirements.txt
   ```

3. **Add workflows**
   - Create markdown SOPs in `workflows/` that define objectives, inputs, tools, and outputs

4. **Create tools**
   - Build Python scripts in `tools/` for specific execution tasks
   - Tools should be deterministic, testable, and single-purpose

## Architecture Principles

- **Workflows** (Layer 1): Plain language instructions defining what to do
- **Agents** (Layer 2): AI-powered decision-making and orchestration
- **Tools** (Layer 3): Deterministic Python scripts that execute tasks

Final deliverables are stored in cloud services (Google Sheets, Slides, etc.). Local files in `.tmp/` are temporary and regenerated as needed.

See [CLAUDE.md](CLAUDE.md) for complete framework documentation.
