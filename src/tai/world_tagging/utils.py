import re

# Characters to be replaced by whitespace
REPLACEMENT_CHARS = set([' ', ';', ':', '!', '-', '/', '\\', '[', ']', '|', '+', '.', '<', '>'])


def normalize_name(name: str) -> str:
    """
    Normalizes a world name.

    1. Removes sequences between curly braces (e.g. color codes {FFFFFF}).
    2. Replaces special characters with whitespace.
    3. Collapses multiple spaces and converts to lowercase.
    """
    if not name:
        return ''

    # Remove content between curly braces (including braces)
    # Using non-greedy matching for content inside braces
    name = re.sub(r'\{.*?\}', '', name)

    trans_table = str.maketrans({c: ' ' for c in REPLACEMENT_CHARS})
    cleaned = name.translate(trans_table)
    return ' '.join(cleaned.split()).lower()
