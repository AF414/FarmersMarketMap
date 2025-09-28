#!/usr/bin/env python3
"""
Test Claude response parsing to debug JSON parsing issues
"""

import json
import re

def improved_json_parser(response_text: str):
    """Improved JSON parsing that handles various Claude response formats"""

    print("=== CLAUDE RESPONSE ===")
    print(f"Response length: {len(response_text)} characters")
    print("First 500 characters:")
    print(response_text[:500])
    print("\nLast 500 characters:")
    print(response_text[-500:])
    print("\n=== PARSING ATTEMPTS ===")

    try:
        # Method 1: Try parsing entire response as JSON
        try:
            result = json.loads(response_text)
            print("✅ Method 1: Entire response is valid JSON")
            return result
        except json.JSONDecodeError:
            print("❌ Method 1: Entire response is not valid JSON")

        # Method 2: Look for JSON array with better regex
        json_patterns = [
            r'\[[\s\S]*?\]',  # Array pattern (non-greedy)
            r'\{[\s\S]*?\}',  # Single object pattern
            r'```json\s*(\[[\s\S]*?\])\s*```',  # Markdown code block
            r'```\s*(\[[\s\S]*?\])\s*```',      # Code block without json tag
        ]

        for i, pattern in enumerate(json_patterns, 2):
            matches = re.findall(pattern, response_text, re.DOTALL)
            if matches:
                print(f"✅ Method {i}: Found {len(matches)} JSON candidates with pattern {i-1}")
                for j, match in enumerate(matches):
                    try:
                        # If it's a tuple (from capture group), take the captured part
                        json_str = match[0] if isinstance(match, tuple) else match
                        result = json.loads(json_str)
                        print(f"   ✅ Candidate {j+1} is valid JSON with {len(result) if isinstance(result, list) else 1} items")
                        return result
                    except json.JSONDecodeError as e:
                        print(f"   ❌ Candidate {j+1} failed: {str(e)[:100]}")
                        continue
            else:
                print(f"❌ Method {i}: No matches for pattern {i-1}")

        # Method 3: Try to extract vendor information even if JSON is malformed
        print("🔍 Method 6: Attempting text-based vendor extraction as fallback")
        vendors = extract_vendors_from_text(response_text)
        if vendors:
            print(f"✅ Extracted {len(vendors)} vendors from text analysis")
            return vendors

        print("❌ All parsing methods failed")
        return []

    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return []

def extract_vendors_from_text(text):
    """Extract vendor information from text even if JSON is malformed"""
    vendors = []

    # Look for vendor-like patterns in the text
    vendor_patterns = [
        r'"name":\s*"([^"]+)"',
        r'"business_type":\s*"([^"]+)"',
        r'name:\s*"([^"]+)"',  # Without quotes around key
        r'- ([A-Z][a-zA-Z\s&]+(?:Farm|Bakery|Kitchen|Market|Company|LLC))',  # List format
    ]

    name_matches = re.findall(vendor_patterns[0], text, re.IGNORECASE)
    if name_matches:
        for name in name_matches:
            vendors.append({"name": name, "source": "regex_extraction"})

    return vendors

# Test with some example Claude responses
test_responses = [
    # Valid JSON
    '[{"name": "Test Farm", "business_type": "farm"}]',

    # JSON in markdown
    '''Here are the vendors I found:

```json
[
  {"name": "AquaSprout Farms", "business_type": "farm", "products": ["greens", "lettuce"]},
  {"name": "Backer Farm", "business_type": "farm", "products": ["meat", "poultry"]}
]
```

These vendors were extracted from the farmers market website.''',

    # Malformed JSON (common Claude issue)
    '''I found these vendors:
[
  {
    "name": "AquaSprout Farms",
    "business_type": "farm",
    "products": ["greens", "lettuce"],
  },
  {
    "name": "Backer Farm",
    "business_type": "farm"
  }
]''',
]

if __name__ == "__main__":
    for i, response in enumerate(test_responses, 1):
        print(f"\n{'='*60}")
        print(f"TEST CASE {i}")
        print('='*60)
        result = improved_json_parser(response)
        print(f"Final result: {len(result) if isinstance(result, list) else 'Not a list'} items")
        if result:
            print("Sample items:", result[:2])