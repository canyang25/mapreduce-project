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

def map_function(text):
    """
    Map phase: Tokenize text and emit (word, 1) pairs.
    
    Args:
        text: String containing the text content to process
        
    Returns:
        List of tuples: [(word, 1), (word, 1), ...]
    """
    # Split text into words (lowercase, remove punctuation)
    import re
    words = re.findall(r'\b[a-z]+\b', text.lower())
    
    # Emit (word, 1) for each word
    return [(word, 1) for word in words]


def reduce_function(key, values):
    """
    Reduce phase: Sum up the counts for each word.
    
    Args:
        key: The word (string)
        values: List of counts for this word [1, 1, 1, ...]
        
    Returns:
        Tuple: (word, total_count)
    """
    total_count = sum(values)
    return (key, total_count)
