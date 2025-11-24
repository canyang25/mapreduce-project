"""
Word Count MapReduce Job Template

This file demonstrates a classic MapReduce example: counting word occurrences
across multiple text files.
"""

from typing import List, Tuple

def iterator_fn(file_bytes: bytes, metadata: dict) -> List[Tuple[str, str]]:
    """
    Iterator function to split file bytes into (key, value) pairs to pass to map_function.
    Here, key is the file path and value is the file content as a string.

    Args:
        file_bytes: The raw bytes of the input file.
        metadata: Dictionary containing metadata such as 'file_path' and 'file_size'.
    """
    content = file_bytes.decode("utf-8")
    doc_id = metadata.get("file_path", "unknown_document")
    yield (doc_id, content)

def map_function(input_key: str, input_value: str) -> List[Tuple[str, int]]:
    """
    Map phase: tokenize text and emit (word, 1) pairs.

    Args:
        input_key: Identifier for the input record (e.g., file path).
                  Not used in this job but included for a standardized interface.
        input_value: Text content of the document as a string.

    Returns:
        List of (word, 1) tuples.
    """
    import re

    # Normalize to lowercase and extract words a–z
    words = re.findall(r"\b[a-z]+\b", input_value.lower())

    # Emit (word, 1) for each word
    return [(word, 1) for word in words]


def reduce_function(key: str, values:list[str]) -> Tuple[str, int]:
    """
    Reduce phase: sum up the counts for each word.

    Args:
        key: The word (string).
        values: List of counts for this word [1, 1, 1, ...].

    Returns:
        (word, total_count) tuple.
    """
    total_count = sum([int(v) for v in values])
    return key, total_count