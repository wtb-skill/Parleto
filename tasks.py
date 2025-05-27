import csv
from io import StringIO
from typing import Tuple, List, Dict, Iterator, Union, IO

# This solution prioritizes memory and CPU efficiency:
# - Uses streaming CSV parsing (DictReader over file-like object)
# - Avoids caching or preloading entire datasets
# - Uses set lookup for fast matching
# External libraries like pandas could improve performance,
# but were avoided to ensure compatibility with a standard-library-only test environment.


def parse_data_iter(data: Union[str, IO[str]]) -> Tuple[List[str], Iterator[Dict[str, str]]]:
    """
    Parses CSV data from a string or a file-like object using an iterator to avoid loading all rows into memory.

    Args:
        data (Union[str, IO[str]]): CSV string or a file-like object.

    Returns:
        Tuple[List[str], Iterator[Dict[str, str]]]: A tuple containing the list of headers and an iterator over row dictionaries.

    Raises:
        ValueError: If the CSV data does not contain headers.
    """
    if isinstance(data, str):
        # If input is a string, wrap in StringIO — creates an in-memory stream (requires a copy)
        stream = StringIO(data)
    else:
        # If input is already a stream, use directly — avoids memory copy
        stream = data

    reader = csv.DictReader(stream)
    if reader.fieldnames is None:
        raise ValueError("CSV data must have headers.")
    return list(reader.fieldnames), reader


def task1(search: Dict[str, str], data: Union[str, IO[str]]) -> str:
    """
    Searches for the first row in the CSV data that matches the given key-value pairs.

    Args:
        search (Dict[str, str]): Dictionary of key-value pairs to search for.
        data (Union[str, IO[str]]): CSV string or file-like object.

    Returns:
        str: The value of the 'value' column if a match is found, otherwise '-1'.

    Raises:
        Exception: If the keys in the search dictionary do not match the expected keys in the data.

    Note:
        Caching was considered for repeated queries, but intentionally omitted to maintain streaming efficiency
        and minimal memory usage for large datasets.
    """
    headers, row_iter = parse_data_iter(data)
    expected_keys = set(headers) - {'value'}

    if set(search.keys()) != expected_keys:
        raise Exception("Key mismatch")

    search_key = tuple(sorted((k, str(v)) for k, v in search.items()))

    # Linear scan with streaming — minimal memory footprint
    for row in row_iter:
        row_key = tuple(sorted((k, row[k]) for k in expected_keys))
        if row_key == search_key:
            return row['value']
    return '-1'


def task2(search_list: List[Dict[str, str]], data: Union[str, IO[str]]) -> str:
    """
    Computes the weighted average of values matching any dictionary in a list of search criteria.

    Args:
        search_list (List[Dict[str, str]]): List of dictionaries containing search key-value pairs.
        data (Union[str, IO[str]]): CSV string or file-like object.

    Returns:
        str: The weighted average of matched values rounded to one decimal as a string, or '-1' if no matches are found.

    Raises:
        Exception: If the keys in any search dictionary do not match the expected keys in the data.

    Note:
        Caching was considered but avoided to maintain performance on large streamed datasets.
    """
    headers, row_iter = parse_data_iter(data)
    expected_keys = set(headers) - {'value'}

    if not all(set(search.keys()) == expected_keys for search in search_list):
        raise Exception("Key mismatch")

    # Use set of tuples for O(1) lookups instead of O(n) scan per row
    search_set = {
        tuple(sorted((k, str(v)) for k, v in search.items()))
        for search in search_list
    }

    total_weighted_sum = 0
    total_weight = 0

    # Streamed processing — handles large datasets efficiently without loading all data
    for row in row_iter:
        row_key = tuple(sorted((k, row[k]) for k in expected_keys))
        if row_key in search_set:
            val = int(row['value'])
            weight = 10 if val % 2 else 20
            total_weighted_sum += val * weight
            total_weight += weight

    if total_weight == 0:
        return '-1'

    average = total_weighted_sum / total_weight
    return f"{round(average, 1):.1f}"
