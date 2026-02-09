# Tools Directory

This directory contains Python scripts that perform deterministic execution tasks.

## Tool Design Principles

1. **Single Purpose**: Each tool does one thing well
2. **Deterministic**: Same inputs = same outputs
3. **Testable**: Can be run independently
4. **Fast**: Optimized for efficiency
5. **Error Handling**: Graceful failures with clear messages

## Tool Template

```python
#!/usr/bin/env python3
"""
Tool: [Name]
Description: What this tool does
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    """Main execution function"""
    # Your code here
    pass

if __name__ == "__main__":
    main()
```

## Common Patterns

**Loading API Keys:**
```python
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv('API_KEY_NAME')
```

**File Operations:**
```python
# Write to .tmp/
output_path = '.tmp/output.json'
with open(output_path, 'w') as f:
    json.dump(data, f)
```

**Error Handling:**
```python
try:
    result = api_call()
except RateLimitError as e:
    print(f"Rate limit hit: {e}")
    # Handle gracefully
```

## Before Creating a New Tool

1. Check if a similar tool already exists
2. Consider if this should be part of an existing tool
3. Ensure the workflow clearly requires this functionality
