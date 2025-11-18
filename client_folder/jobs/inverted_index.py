"""
Inverted Index MapReduce Job

This file demonstrates a classic MapReduce example: inverting word indexes.

This job builds an inverted index:
    word -> list of documents where the word appears
"""

from typing import List, Tuple

def iterator_fn(file_bytes: bytes, metadata: dict) -> List[Tuple[str, str]]:
    """
    Iterator function to split file bytes into (key, value) pairs to pass to map_function.
    Here, key is the file path and value is the file content as a string.

    Args:
        file_bytes: The raw bytes of the file.
        metadata: Dictionary containing metadata such as 'file_path' and 'file_size'.
    """
    content = file_bytes.decode("utf-8")
    doc_id = metadata.get("file_path", "unknown_document")
    yield (doc_id, content)

def map_function(input_key: str, input_value: str) -> List[Tuple[str, str]]:
    """
    Map phase: Extract all unique words from the document and emit
    (word, document_id) pairs.

    Args:
        input_key: Document identifier (usually the file path).
        input_value: The full text content of the document.

    Returns:
        List of (word, doc_id) tuples. Each (word, doc_id) pair appears
        at most once per document.
    """
    import re

    # Normalize and extract words
    words = re.findall(r"\b[a-z]+\b", input_value.lower())

    # Deduplicate words within the same document
    unique_words = set(words)

    # Emit (word, document_id) for each unique word
    return [(word, input_key) for word in unique_words]


def reduce_function(key: str, values: List[str]) -> Tuple[str, List[str]]:
    """
    Reduce phase: Aggregate the list of document IDs for each word.

    Args:
        key: The word.
        values: List of document IDs (strings), may contain duplicates.

    Returns:
        (word, sorted_list_of_unique_doc_ids)
    """
    unique_docs = sorted(set(values))
    return key, unique_docs
