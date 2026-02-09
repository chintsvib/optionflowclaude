# Workflows Directory

This directory contains markdown SOPs (Standard Operating Procedures) that define how to accomplish specific tasks.

## Workflow Structure

Each workflow should include:

1. **Objective**: What this workflow accomplishes
2. **Required Inputs**: What information/data is needed to start
3. **Tools Used**: Which scripts from `tools/` are executed
4. **Process Steps**: Step-by-step instructions
5. **Expected Outputs**: What gets created/delivered
6. **Edge Cases**: How to handle errors, rate limits, and failures
7. **Learnings**: Documented improvements from past runs

## Example Workflow Template

```markdown
# Workflow: [Name]

## Objective
Brief description of what this accomplishes.

## Required Inputs
- Input 1: Description
- Input 2: Description

## Tools Used
- `tools/script_name.py`

## Process
1. Step one
2. Step two
3. Step three

## Expected Outputs
- Output location and format

## Edge Cases
- Rate limits: How to handle
- Missing data: What to do
- API errors: Recovery steps

## Learnings
- Document improvements and discoveries here
```

## Best Practices

- Keep workflows focused on a single objective
- Update workflows when you discover better approaches
- Document rate limits, timing quirks, and gotchas
- Write in plain language as if briefing a team member
