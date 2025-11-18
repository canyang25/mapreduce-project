"""
Word Count MapReduce Job Template

This file demonstrates a classic MapReduce example: counting word occurrences
across multiple text files.

Usage:
    # In your client code:
    from client_folder.jobs.word_count import map_function, reduce_function
    
    client = Client(...)
    result = client.map_reduce(
        file_paths=["/client_folder/data/small/file1.txt", "/client_folder/data/small/file2.txt"],
        map=map_function,
        reduce=reduce_function
    )
"""

from typing import List, Tuple

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