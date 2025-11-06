"""
Word Count MapReduce Job Template

This file demonstrates a classic MapReduce example: counting word occurrences
across multiple text files.

Usage:
    # In your client code:
    from examples.jobs.word_count import map_function, reduce_function
    
    client = Client(...)
    result = client.map_reduce(
        file_paths=["/examples/data/small/file1.txt", "/examples/data/small/file2.txt"],
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


# Example of how the MapReduce framework would use these functions:
if __name__ == "__main__":
    # This is just for demonstration/testing purposes
    # In the actual system, the master will call these functions
    
    sample_text = "The quick brown fox jumps over the lazy dog. The dog was sleeping."
    
    print("=== Map Phase ===")
    map_results = map_function(sample_text)
    print(f"Input: {sample_text}")
    print(f"Map output: {map_results}")
    
    print("\n=== Reduce Phase (simulated) ===")
    # Simulate grouping by key
    from collections import defaultdict
    grouped = defaultdict(list)
    for word, count in map_results:
        grouped[word].append(count)
    
    # Apply reduce function
    reduce_results = []
    for word, counts in grouped.items():
        result = reduce_function(word, counts)
        reduce_results.append(result)
    
    print(f"Reduce output: {reduce_results}")
    print("\n=== Final Word Count ===")
    for word, count in sorted(reduce_results):
        print(f"{word}: {count}")
