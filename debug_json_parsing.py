#!/usr/bin/env python3
"""
Debug Claude JSON parsing without needing API key
"""

import json
import re

def debug_parse_json_response(response_text: str):
    """Debug version of the JSON parsing function"""

    print(f"=== DEBUGGING JSON PARSING ===")
    print(f"Response length: {len(response_text)} characters")
    print(f"First 200 chars: {response_text[:200]}")
    print(f"Last 200 chars: {response_text[-200:]}")
    print()

    try:
        print(f"DEBUG: Attempting JSON parsing...")

        # Look for JSON array pattern
        json_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            print(f"DEBUG: Found JSON array, length: {len(json_str)} chars")
            print(f"DEBUG: JSON preview: {json_str[:100]}...")
            try:
                result = json.loads(json_str)
                print(f"DEBUG: Successfully parsed JSON array with {len(result)} items")
                return result
            except json.JSONDecodeError as e:
                print(f"DEBUG: JSON array parsing failed: {str(e)}")
                print(f"DEBUG: Problematic JSON: {json_str}")

        # Try parsing the entire response as JSON
        print(f"DEBUG: Trying to parse entire response as JSON...")
        try:
            result = json.loads(response_text)
            print(f"DEBUG: Successfully parsed entire response as JSON")
            return result
        except json.JSONDecodeError as e:
            print(f"DEBUG: Entire response parsing failed: {str(e)}")
            # Show exactly where it fails
            error_pos = getattr(e, 'pos', None)
            if error_pos:
                print(f"DEBUG: Error at position {error_pos}")
                start = max(0, error_pos - 50)
                end = min(len(response_text), error_pos + 50)
                print(f"DEBUG: Context around error: {response_text[start:end]}")

    except Exception as e:
        print(f"DEBUG: Unexpected error in JSON parsing: {str(e)}")

    print(f"Warning: Could not parse JSON from response")
    return []

# Test cases that might represent real Claude responses
test_cases = [
    # Case 1: Perfect JSON
    '[{"name": "AquaSprout Farms", "business_type": "farm"}]',

    # Case 2: JSON with markdown
    '''I found these vendors from the farmers market website:

```json
[
  {"name": "AquaSprout Farms", "business_type": "farm", "products": ["greens"]},
  {"name": "Backer Farm", "business_type": "farm", "products": ["meat"]}
]
```

These are the main vendors listed.''',

    # Case 3: JSON with trailing comma (common issue)
    '''[
  {"name": "AquaSprout Farms", "business_type": "farm"},
  {"name": "Backer Farm", "business_type": "farm"},
]''',

    # Case 4: No JSON, just explanation
    '''I searched through the website content but was unable to find a clear list of vendors. The page appears to be a general information page about the farmers market without specific vendor listings.''',

    # Case 5: Malformed JSON with quotes
    '''[
  {"name": "Tom's Farm", "business_type": "farm"},
  {"name": "Bob & Sue's Kitchen", "business_type": "food"}
]''',
]

if __name__ == "__main__":
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*60}")
        print(f"TEST CASE {i}")
        print('='*60)
        result = debug_parse_json_response(test_case)
        print(f"RESULT: {len(result) if isinstance(result, list) else 'Not a list'} items")
        if result and len(result) > 0:
            print(f"Sample item: {result[0]}")
        print()