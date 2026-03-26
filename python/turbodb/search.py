"""
TurboDB Trigram Search Index
============================
Cursor-style trigram index for fast regex/substring search over TurboDB documents.

Design (from Zobel et al. 1993 / Russ Cox 2012 / Cursor blog 2026):
  1. Extract overlapping 3-char trigrams from document values
  2. Store inverted index: trigram -> set of (collection, key) pairs
  3. Query: decompose pattern into trigrams, intersect posting lists,
     then full-match only the candidate documents (not the whole DB)

This gives O(candidates) search instead of O(all_docs) linear scan.
"""

import re
import time
from collections import defaultdict


def extract_trigrams(text):
    """Extract all overlapping 3-character trigrams from text."""
    text = text.lower()
    trigrams = set()
    for i in range(len(text) - 2):
        trigrams.add(text[i:i+3])
    return trigrams


def extract_trigrams_from_regex(pattern):
    """
    Extract trigrams from a regex pattern for index lookup.
    
    Strategy: find literal runs in the pattern, extract trigrams from those.
    This is a simplified version of the decomposition described in
    Russ Cox's "Regular Expression Matching with a Trigram Index".
    """
    # Strip regex metacharacters to find literal segments
    # Split on non-literal chars: . * + ? [ ] ( ) { } | ^ $ \\
    literals = re.split(r'[.*+?\[\](){}|^$\\]', pattern)
    trigrams = set()
    for lit in literals:
        if len(lit) >= 3:
            trigrams.update(extract_trigrams(lit))
    return trigrams


class TrigramIndex:
    """
    In-memory trigram inverted index over TurboDB documents.
    
    Usage:
        idx = TrigramIndex()
        idx.index_collection(col, col_name)   # index all docs in a collection
        results = idx.search("pattern")        # substring search
        results = idx.regex_search(r"fo+bar")  # regex search
    
    The index stores: trigram -> set of (collection_name, key) pairs.
    Search returns candidate keys, then verifies with full scan on candidates only.
    """

    def __init__(self):
        self.posting_lists = defaultdict(set)  # trigram -> {(col_name, key), ...}
        self.doc_values = {}                    # (col_name, key) -> value
        self.stats = {"docs_indexed": 0, "trigrams": 0, "index_time_ms": 0}

    def index_doc(self, col_name, key, value):
        """Add a single document to the index."""
        entry = (col_name, key)
        self.doc_values[entry] = value
        for tri in extract_trigrams(value):
            self.posting_lists[tri].add(entry)

    def index_collection(self, collection, col_name, limit=None):
        """Index all documents in a TurboDB collection."""
        t0 = time.perf_counter()
        docs = collection.scan(limit=limit or 1_000_000)
        count = 0
        for doc in docs:
            self.index_doc(col_name, doc["key"], doc.get("value", ""))
            count += 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.stats["docs_indexed"] += count
        self.stats["trigrams"] = len(self.posting_lists)
        self.stats["index_time_ms"] += elapsed_ms
        return count

    def index_bulk(self, col_name, docs):
        """Index a list of (key, value) pairs directly (no DB round-trip)."""
        t0 = time.perf_counter()
        for key, value in docs:
            self.index_doc(col_name, key, value)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.stats["docs_indexed"] += len(docs)
        self.stats["trigrams"] = len(self.posting_lists)
        self.stats["index_time_ms"] += elapsed_ms

    def _candidates(self, trigrams):
        """Intersect posting lists for a set of trigrams -> candidate entries."""
        if not trigrams:
            return set(self.doc_values.keys())
        
        lists = []
        for tri in trigrams:
            tri = tri.lower()
            if tri in self.posting_lists:
                lists.append(self.posting_lists[tri])
            else:
                return set()  # trigram not in index -> no matches
        
        if not lists:
            return set(self.doc_values.keys())
        
        # Intersect all posting lists (start with smallest for efficiency)
        lists.sort(key=len)
        result = set(lists[0])
        for lst in lists[1:]:
            result &= lst
            if not result:
                return set()
        return result

    def search(self, query, col_name=None):
        """
        Substring search using trigram index.
        
        Returns list of (col_name, key, value) for documents containing the query.
        """
        t0 = time.perf_counter()
        trigrams = extract_trigrams(query)
        candidates = self._candidates(trigrams)
        
        # Filter by collection if specified
        if col_name:
            candidates = {c for c in candidates if c[0] == col_name}
        
        # Full verification on candidates
        query_lower = query.lower()
        results = []
        scanned = 0
        for entry in candidates:
            val = self.doc_values.get(entry, "")
            scanned += 1
            if query_lower in val.lower():
                results.append({"collection": entry[0], "key": entry[1], "value": val})
        
        elapsed_us = (time.perf_counter() - t0) * 1e6
        return SearchResult(
            results=results,
            candidates=len(candidates),
            scanned=scanned,
            total_docs=len(self.doc_values),
            elapsed_us=elapsed_us,
            trigrams_used=trigrams,
        )

    def regex_search(self, pattern, col_name=None, flags=0):
        """
        Regex search using trigram index for candidate selection.
        
        Decomposes regex into literal trigrams, narrows candidates via index,
        then runs full regex on candidates only.
        """
        t0 = time.perf_counter()
        trigrams = extract_trigrams_from_regex(pattern)
        candidates = self._candidates(trigrams)
        
        if col_name:
            candidates = {c for c in candidates if c[0] == col_name}
        
        compiled = re.compile(pattern, flags | re.IGNORECASE)
        results = []
        scanned = 0
        for entry in candidates:
            val = self.doc_values.get(entry, "")
            scanned += 1
            if compiled.search(val):
                results.append({"collection": entry[0], "key": entry[1], "value": val})
        
        elapsed_us = (time.perf_counter() - t0) * 1e6
        return SearchResult(
            results=results,
            candidates=len(candidates),
            scanned=scanned,
            total_docs=len(self.doc_values),
            elapsed_us=elapsed_us,
            trigrams_used=trigrams,
        )

    def linear_search(self, query, col_name=None):
        """Brute-force linear scan (for benchmarking comparison)."""
        t0 = time.perf_counter()
        query_lower = query.lower()
        results = []
        scanned = 0
        for entry, val in self.doc_values.items():
            if col_name and entry[0] != col_name:
                continue
            scanned += 1
            if query_lower in val.lower():
                results.append({"collection": entry[0], "key": entry[1], "value": val})
        
        elapsed_us = (time.perf_counter() - t0) * 1e6
        return SearchResult(
            results=results,
            candidates=len(self.doc_values),
            scanned=scanned,
            total_docs=len(self.doc_values),
            elapsed_us=elapsed_us,
            trigrams_used=set(),
        )

    def linear_regex_search(self, pattern, col_name=None, flags=0):
        """Brute-force regex scan (for benchmarking comparison)."""
        t0 = time.perf_counter()
        compiled = re.compile(pattern, flags | re.IGNORECASE)
        results = []
        scanned = 0
        for entry, val in self.doc_values.items():
            if col_name and entry[0] != col_name:
                continue
            scanned += 1
            if compiled.search(val):
                results.append({"collection": entry[0], "key": entry[1], "value": val})
        
        elapsed_us = (time.perf_counter() - t0) * 1e6
        return SearchResult(
            results=results,
            candidates=len(self.doc_values),
            scanned=scanned,
            total_docs=len(self.doc_values),
            elapsed_us=elapsed_us,
            trigrams_used=set(),
        )


class SearchResult:
    """Wrapper for search results with stats."""

    def __init__(self, results, candidates, scanned, total_docs, elapsed_us, trigrams_used):
        self.results = results
        self.candidates = candidates
        self.scanned = scanned
        self.total_docs = total_docs
        self.elapsed_us = elapsed_us
        self.trigrams_used = trigrams_used
        self.selectivity = candidates / total_docs if total_docs else 0

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __repr__(self):
        return (
            f"SearchResult({len(self.results)} hits, "
            f"scanned {self.scanned}/{self.total_docs} docs "
            f"({self.selectivity:.1%} selectivity), "
            f"{self.elapsed_us:.0f}µs)"
        )

    def summary(self):
        return {
            "hits": len(self.results),
            "candidates": self.candidates,
            "scanned": self.scanned,
            "total_docs": self.total_docs,
            "selectivity": self.selectivity,
            "elapsed_us": self.elapsed_us,
            "trigrams": len(self.trigrams_used),
        }
