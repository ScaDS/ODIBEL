import math
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from itertools import combinations
from typing import Iterable, List, Tuple, Protocol, runtime_checkable

import numpy as np
import pandas as pd


# ============================================================
# 0. Core utilities / assumptions
# ============================================================

def get_id_columns(df_a: pd.DataFrame, df_b: pd.DataFrame):
    """
    Determine or validate the identifier columns for df_a and df_b.
    Returns (id_col_a, id_col_b).
    """
    def _infer_id(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return None
        preferred = {"id", "idx", "index", "_id", "record_id", "entity_id"}
        for col in df.columns:
            if col.lower() in preferred and df[col].is_unique:
                return col
        for col in df.columns:
            if df[col].is_unique:
                return col
        return None

    id_a = _infer_id(df_a)
    id_b = _infer_id(df_b)
    if id_a is None or id_b is None:
        raise ValueError("Could not infer unique identifier columns for both dataframes.")
    return id_a, id_b


def _normalize_attributes(df: pd.DataFrame, attributes: Iterable = None) -> List[str]:
    if attributes is None:
        return list(df.columns)
    return [col for col in attributes if col in df.columns]


def _safe_entropy(probs: np.ndarray) -> float:
    probs = probs[np.nonzero(probs)]
    if probs.size == 0:
        return 0.0
    return float(-np.sum(probs * np.log2(probs)))


def _quantiles(values: np.ndarray) -> dict:
    if values.size == 0:
        return {"q25": None, "q50": None, "q75": None}
    return {
        "q25": float(np.percentile(values, 25)),
        "q50": float(np.percentile(values, 50)),
        "q75": float(np.percentile(values, 75)),
    }


def _describe_numeric(values: np.ndarray) -> dict:
    if values.size == 0:
        return {
            "count": 0,
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
            "q25": None,
            "median": None,
            "q75": None,
        }
    return {
        "count": int(values.size),
        "mean": float(np.mean(values)),
        "std": float(np.std(values, ddof=1)) if values.size > 1 else 0.0,
        "min": float(np.min(values)),
        "max": float(np.max(values)),
        "q25": float(np.percentile(values, 25)),
        "median": float(np.percentile(values, 50)),
        "q75": float(np.percentile(values, 75)),
    }


def _string_similarity(a: str, b: str) -> float:
    a = "" if a is None else str(a)
    b = "" if b is None else str(b)
    return SequenceMatcher(None, a, b).ratio()


def _jaccard(a_tokens: set, b_tokens: set) -> float:
    if not a_tokens and not b_tokens:
        return 1.0
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / len(a_tokens | b_tokens)


# ============================================================
# 1. Attribute-level completeness & sparsity
# ============================================================

def compute_attribute_missingness(df: pd.DataFrame, attributes=None) -> pd.DataFrame:
    """
    Compute missingness statistics per attribute:
    - count_missing
    - pct_missing
    - count_non_missing
    Returns a DataFrame indexed by attribute.
    """
    cols = _normalize_attributes(df, attributes)
    if not cols:
        return pd.DataFrame(columns=["attribute", "count_missing", "pct_missing", "count_non_missing"])
    stats = []
    for col in cols:
        missing = df[col].isna().sum()
        total = len(df)
        stats.append(
            {
                "attribute": col,
                "count_missing": int(missing),
                "pct_missing": float(missing / total) if total else 0.0,
                "count_non_missing": int(total - missing),
            }
        )
    return pd.DataFrame(stats).set_index("attribute")


def compute_dataset_density(df: pd.DataFrame, attributes=None) -> float:
    """
    Compute average attribute completeness per record:
    - fraction of non-missing cells over all (rows x attributes).
    Returns a single float in [0, 1].
    """
    cols = _normalize_attributes(df, attributes)
    if df.empty or not cols:
        return 0.0
    non_missing = df[cols].notna().to_numpy().sum()
    total = len(df) * len(cols)
    return float(non_missing / total) if total else 0.0


def compare_attribute_missingness(df_a: pd.DataFrame, df_b: pd.DataFrame, attributes=None) -> pd.DataFrame:
    """
    Compare attribute-level missingness between df_a and df_b.
    Returns a DataFrame with columns:
    - attribute
    - pct_missing_a
    - pct_missing_b
    - diff_pct_missing (b - a)
    """
    cols = _normalize_attributes(df_a, attributes) if attributes is not None else list(set(df_a.columns) | set(df_b.columns))
    miss_a = compute_attribute_missingness(df_a, cols).rename(columns={"pct_missing": "pct_missing_a"})
    miss_b = compute_attribute_missingness(df_b, cols).rename(columns={"pct_missing": "pct_missing_b"})
    merged = miss_a.join(miss_b, how="outer", lsuffix="_a", rsuffix="_b")
    merged["pct_missing_a"] = merged["pct_missing_a"].fillna(0.0)
    merged["pct_missing_b"] = merged["pct_missing_b"].fillna(0.0)
    merged["diff_pct_missing"] = merged["pct_missing_b"] - merged["pct_missing_a"]
    merged = merged.reset_index().rename(columns={"index": "attribute"})
    return merged[["attribute", "pct_missing_a", "pct_missing_b", "diff_pct_missing"]]


def compute_missingness_in_matches(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attributes_a=None,
    attributes_b=None
) -> pd.DataFrame:
    """
    Compute missingness statistics for matched record pairs only.
    Returns a DataFrame with attribute-level missingness in the matched subset.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    if id_a not in matches.columns or id_b not in matches.columns:
        raise ValueError("matches DataFrame must contain the inferred id columns.")

    attrs_a = _normalize_attributes(df_a, attributes_a)
    attrs_b = _normalize_attributes(df_b, attributes_b)

    matched_a = matches[[id_a]].merge(df_a[[id_a] + attrs_a], on=id_a, how="left")
    matched_b = matches[[id_b]].merge(df_b[[id_b] + attrs_b], on=id_b, how="left")

    miss_a = compute_attribute_missingness(matched_a, attrs_a).add_suffix("_a")
    miss_b = compute_attribute_missingness(matched_b, attrs_b).add_suffix("_b")

    miss_a.index.name = "attribute"
    miss_b.index.name = "attribute"
    combined = miss_a.join(miss_b, how="outer")
    combined = combined.reset_index()
    return combined


# ============================================================
# 2. Attribute-level value distributions & discriminativeness
# ============================================================

def compute_attribute_value_frequencies(df: pd.DataFrame, attribute: str) -> pd.Series:
    """
    Compute frequency counts for values in a specific attribute.
    Returns a Series indexed by value with counts.
    """
    if attribute not in df.columns:
        raise KeyError(f"{attribute} not found in DataFrame.")
    return df[attribute].value_counts(dropna=False)


def compute_attribute_entropy(df: pd.DataFrame, attribute: str) -> float:
    """
    Compute entropy of an attribute's value distribution.
    Used as a proxy for discriminativeness.
    """
    counts = compute_attribute_value_frequencies(df, attribute)
    total = counts.sum()
    if total == 0:
        return 0.0
    probs = counts / total
    return _safe_entropy(probs.to_numpy())


def compute_attribute_uniqueness(df: pd.DataFrame, attribute: str) -> float:
    """
    Compute fraction of records where the attribute value is unique.
    Returns a float in [0, 1].
    """
    if len(df) == 0:
        return 0.0
    counts = df[attribute].value_counts(dropna=False)
    unique_mask = counts[counts == 1]
    return float(unique_mask.sum() / len(df))


def compute_attribute_most_common_ratio(df: pd.DataFrame, attribute: str) -> float:
    """
    Compute relative frequency of the most common value in an attribute.
    High values indicate low discriminativeness.
    """
    counts = compute_attribute_value_frequencies(df, attribute)
    total = counts.sum()
    if total == 0:
        return 0.0
    return float(counts.max() / total)


def summarize_attribute_discriminativeness(df: pd.DataFrame, attributes=None) -> pd.DataFrame:
    """
    Summarize discriminativeness metrics for multiple attributes.
    Returns DataFrame with columns:
    - attribute
    - entropy
    - uniqueness
    - most_common_ratio
    """
    cols = _normalize_attributes(df, attributes)
    rows = []
    for col in cols:
        rows.append(
            {
                "attribute": col,
                "entropy": compute_attribute_entropy(df, col),
                "uniqueness": compute_attribute_uniqueness(df, col),
                "most_common_ratio": compute_attribute_most_common_ratio(df, col),
            }
        )
    return pd.DataFrame(rows)


def compare_attribute_distributions(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    attribute: str,
    method: str = "js_divergence"
) -> float:
    """
    Compare value distributions of a given attribute between df_a and df_b.
    method options (to implement, e.g.):
    - 'js_divergence'
    - 'kl_divergence'
    - 'wasserstein'
    Returns a scalar divergence/distance score.
    """
    counts_a = compute_attribute_value_frequencies(df_a, attribute)
    counts_b = compute_attribute_value_frequencies(df_b, attribute)
    support = counts_a.index.union(counts_b.index)
    p = counts_a.reindex(support, fill_value=0).to_numpy(dtype=float)
    q = counts_b.reindex(support, fill_value=0).to_numpy(dtype=float)
    p = p / p.sum() if p.sum() else p
    q = q / q.sum() if q.sum() else q

    method = method.lower()
    if method == "js_divergence":
        if p.sum() == 0 or q.sum() == 0:
            return 0.0
        m = 0.5 * (p + q)
        eps = 1e-12
        p_safe = np.clip(p, eps, 1)
        q_safe = np.clip(q, eps, 1)
        m_safe = np.clip(m, eps, 1)
        kl_pm = np.sum(p_safe * np.log2(p_safe / m_safe))
        kl_qm = np.sum(q_safe * np.log2(q_safe / m_safe))
        return float(0.5 * (kl_pm + kl_qm))
    if method == "kl_divergence":
        eps = 1e-12
        p_safe = np.clip(p, eps, 1)
        q_safe = np.clip(q, eps, 1)
        return float(np.sum(p_safe * np.log2(p_safe / q_safe)))
    if method == "wasserstein":
        # Fallback simple approximation using cumulative differences
        cdf_p = np.cumsum(p)
        cdf_q = np.cumsum(q)
        return float(np.mean(np.abs(cdf_p - cdf_q)))
    raise ValueError(f"Unknown method {method}")


# ============================================================
# 3. Attribute type, formatting, and granularity
# ============================================================

def infer_attribute_type(df: pd.DataFrame, attribute: str) -> str:
    """
    Infer an attribute's semantic type (heuristic), e.g.:
    - 'string'
    - 'numeric'
    - 'date'
    - 'categorical'
    - 'id_like'
    Returns a string label.
    """
    if attribute not in df.columns or df.empty:
        return "unknown"
    series = df[attribute].dropna()
    if series.empty:
        return "unknown"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_bool_dtype(series):
        return "categorical"

    # Treat as object/string
    values = series.astype(str)
    unique_ratio = values.nunique(dropna=False) / len(series)
    avg_len = values.str.len().mean()
    all_numeric_like = values.str.fullmatch(r"[0-9\-]+").mean() > 0.8

    if unique_ratio > 0.9 and avg_len > 4 and all_numeric_like:
        return "id_like"
    if unique_ratio < 0.2:
        return "categorical"
    return "string"


def infer_attribute_types(df: pd.DataFrame, attributes=None) -> pd.Series:
    """
    Infer types for multiple attributes.
    Returns a Series indexed by attribute with inferred types.
    """
    cols = _normalize_attributes(df, attributes)
    return pd.Series({col: infer_attribute_type(df, col) for col in cols})


def compute_attribute_length_stats(df: pd.DataFrame, attribute: str) -> dict:
    """
    Compute statistics for string length of a given attribute:
    e.g. mean, std, min, max, quartiles.
    Returns a dictionary of statistics.
    """
    if attribute not in df.columns:
        raise KeyError(f"{attribute} not found.")
    lengths = df[attribute].dropna().astype(str).str.len().to_numpy()
    stats = _describe_numeric(lengths)
    return stats


def compute_attribute_token_stats(df: pd.DataFrame, attribute: str, tokenizer=str.split) -> dict:
    """
    Compute token-level statistics for a string attribute:
    e.g. mean tokens per value, std, max.
    Returns a dictionary of statistics.
    """
    if attribute not in df.columns:
        raise KeyError(f"{attribute} not found.")
    token_counts = []
    for val in df[attribute].dropna():
        tokens = tokenizer(str(val))
        token_counts.append(len(tokens))
    token_counts = np.array(token_counts)
    return _describe_numeric(token_counts)


def compute_attribute_format_variability(df: pd.DataFrame, attribute: str) -> float:
    """
    Estimate how many distinct 'formats' an attribute appears in.
    For example, via simple regex patterns or character-class patterns.
    Returns a scalar score (higher = more variability).
    """
    if attribute not in df.columns or df.empty:
        return 0.0

    def to_pattern(val: str) -> str:
        mapping = []
        for ch in val:
            if ch.isdigit():
                mapping.append("9")
            elif ch.isalpha():
                mapping.append("A" if ch.isupper() else "a")
            elif ch.isspace():
                mapping.append(" ")
            else:
                mapping.append("#")
        return "".join(mapping)

    patterns = df[attribute].dropna().astype(str).map(to_pattern)
    if patterns.empty:
        return 0.0
    return float(patterns.nunique() / len(patterns))


def compare_attribute_granularity(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    attribute_a: str,
    attribute_b: str
) -> dict:
    """
    Compare granularity of two corresponding attributes from df_a and df_b.
    Uses metrics like token counts, length distributions, etc.
    Returns a dictionary with summary measures.
    """
    len_a = compute_attribute_length_stats(df_a, attribute_a)
    len_b = compute_attribute_length_stats(df_b, attribute_b)
    tok_a = compute_attribute_token_stats(df_a, attribute_a)
    tok_b = compute_attribute_token_stats(df_b, attribute_b)
    return {
        "avg_length_a": len_a["mean"],
        "avg_length_b": len_b["mean"],
        "length_diff": None if len_a["mean"] is None or len_b["mean"] is None else abs(len_a["mean"] - len_b["mean"]),
        "avg_tokens_a": tok_a["mean"],
        "avg_tokens_b": tok_b["mean"],
        "token_diff": None if tok_a["mean"] is None or tok_b["mean"] is None else abs(tok_a["mean"] - tok_b["mean"]),
    }


def compare_attribute_type_alignment(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    attribute_pairs: list
) -> pd.DataFrame:
    """
    Given a list of (attribute_a, attribute_b) pairs, compute type alignment:
    - inferred_type_a
    - inferred_type_b
    - type_match (bool)
    Returns a DataFrame with one row per attribute pair.
    """
    rows = []
    for attr_a, attr_b in attribute_pairs:
        type_a = infer_attribute_type(df_a, attr_a)
        type_b = infer_attribute_type(df_b, attr_b)
        rows.append({"attribute_a": attr_a, "attribute_b": attr_b, "inferred_type_a": type_a, "inferred_type_b": type_b, "type_match": type_a == type_b})
    return pd.DataFrame(rows)


# ============================================================
# 4. Schema-level / dataset-pair characteristics
# ============================================================

def compute_schema_overlap(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
    """
    Compute overlap between the schemas of df_a and df_b.
    Returns a DataFrame with:
    - attribute_a
    - attribute_b (if matched by name or heuristic)
    - match_type (e.g., 'exact_name', 'fuzzy_name', 'unmatched')
    """
    rows = []
    lower_b_map = {col.lower(): col for col in df_b.columns}
    for col in df_a.columns:
        if col in df_b.columns:
            rows.append({"attribute_a": col, "attribute_b": col, "match_type": "exact_name"})
        elif col.lower() in lower_b_map:
            rows.append({"attribute_a": col, "attribute_b": lower_b_map[col.lower()], "match_type": "case_insensitive_name"})
        else:
            rows.append({"attribute_a": col, "attribute_b": None, "match_type": "unmatched"})
    return pd.DataFrame(rows)


def compute_attribute_set_overlap(df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
    """
    Compute simple schema overlap statistics:
    - num_attributes_a
    - num_attributes_b
    - num_exact_name_overlap
    - jaccard_overlap
    Returns a dictionary of metrics.
    """
    set_a = set(df_a.columns)
    set_b = set(df_b.columns)
    exact = len(set_a & set_b)
    jaccard = exact / len(set_a | set_b) if (set_a | set_b) else 0.0
    return {
        "num_attributes_a": len(set_a),
        "num_attributes_b": len(set_b),
        "num_exact_name_overlap": exact,
        "jaccard_overlap": float(jaccard),
    }


def compute_schema_richness(df: pd.DataFrame) -> dict:
    """
    Compute schema richness metrics for a dataset:
    - num_attributes
    - avg_entropy_across_attributes
    - avg_missingness
    Returns a dictionary.
    """
    if df.empty:
        return {"num_attributes": 0, "avg_entropy_across_attributes": 0.0, "avg_missingness": 0.0}
    entropies = []
    for col in df.columns:
        entropies.append(compute_attribute_entropy(df, col))
    miss = compute_attribute_missingness(df)["pct_missing"].mean() if not compute_attribute_missingness(df).empty else 0.0
    return {
        "num_attributes": len(df.columns),
        "avg_entropy_across_attributes": float(np.mean(entropies)) if entropies else 0.0,
        "avg_missingness": float(miss) if miss is not None else 0.0,
    }


def compare_schema_richness(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
    """
    Compare schema richness metrics between df_a and df_b.
    Returns a DataFrame with metrics per dataset.
    """
    metrics_a = compute_schema_richness(df_a)
    metrics_b = compute_schema_richness(df_b)
    return pd.DataFrame(
        [
            {"dataset": "a", **metrics_a},
            {"dataset": "b", **metrics_b},
        ]
    )


# ============================================================
# 5. Record-level duplication & redundancy
# ============================================================

def compute_exact_duplicate_rate(df: pd.DataFrame, subset=None) -> float:
    """
    Compute fraction of records that are exact duplicates (based on subset of attributes or full row).
    Returns a float in [0, 1].
    """
    if df.empty:
        return 0.0
    dup_mask = df.duplicated(subset=subset, keep=False)
    return float(dup_mask.sum() / len(df))


def find_exact_duplicate_groups(df: pd.DataFrame, subset=None) -> pd.Series:
    """
    Identify groups of exact duplicate records.
    Returns a Series mapping record index to a duplicate group id.
    """
    if df.empty:
        return pd.Series(dtype=float)
    group_cols = subset if subset is not None else list(df.columns)
    grouped = df.groupby(group_cols, dropna=False)
    group_id = {}
    current_id = 0
    for _, idx in grouped.groups.items():
        if len(idx) > 1:
            current_id += 1
            for i in idx:
                group_id[i] = current_id
    return pd.Series(group_id, name="duplicate_group")


def compute_near_duplicate_clusters(
    df: pd.DataFrame,
    attributes: list,
    similarity_threshold: float
):
    """
    Identify clusters of near-duplicate records based on similarity over given attributes.
    Returns a cluster structure (to define) or cluster labels per index.
    """
    if df.empty:
        return pd.Series(dtype=float)
    texts = df[attributes].fillna("").astype(str).agg(" | ".join, axis=1).tolist()
    n = len(texts)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[ry] = rx

    for i, j in combinations(range(n), 2):
        if _string_similarity(texts[i], texts[j]) >= similarity_threshold:
            union(i, j)

    labels = {i: find(i) for i in range(n)}
    return pd.Series(labels, index=df.index, name="near_duplicate_cluster")


def summarize_duplication(df: pd.DataFrame, attributes=None) -> dict:
    """
    Summarize duplication characteristics:
    - exact_duplicate_rate
    - est_near_duplicate_rate (if implemented)
    Returns a dictionary.
    """
    attrs = attributes if attributes is not None else list(df.columns)
    exact_rate = compute_exact_duplicate_rate(df, subset=attrs)
    near_clusters = compute_near_duplicate_clusters(df, attrs, similarity_threshold=0.9)
    est_near_rate = 0.0
    if not near_clusters.empty:
        counts = Counter(near_clusters.values())
        total_in_clusters = sum(count for count in counts.values() if count > 1)
        est_near_rate = total_in_clusters / len(df)
    return {
        "exact_duplicate_rate": exact_rate,
        "est_near_duplicate_rate": est_near_rate,
    }


# ============================================================
# 6. Ground-truth pair-level similarity characteristics
# ============================================================

def compute_pairwise_string_similarity_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attribute_a: str,
    attribute_b: str,
    similarity_function_name: str = "levenshtein"
) -> dict:
    """
    Compute summary statistics of string similarity for a given attribute pair over ground-truth matches.
    Returns a dictionary of metrics: mean, std, percentiles, etc.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    if id_a not in matches or id_b not in matches:
        raise ValueError("matches DataFrame must contain the inferred id columns.")

    ser_a = matches[[id_a]].merge(df_a[[id_a, attribute_a]], on=id_a, how="left")[attribute_a]
    ser_b = matches[[id_b]].merge(df_b[[id_b, attribute_b]], on=id_b, how="left")[attribute_b]

    sims = []
    for a_val, b_val in zip(ser_a, ser_b):
        sims.append(_string_similarity(a_val, b_val))
    sims = np.array(sims)
    stats = _describe_numeric(sims)
    return stats


def compute_pairwise_numeric_difference_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attribute_a: str,
    attribute_b: str
) -> dict:
    """
    Compute statistics of absolute numeric differences for matched pairs.
    Returns a dictionary: mean_diff, median_diff, etc.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    ser_a = matches[[id_a]].merge(df_a[[id_a, attribute_a]], on=id_a, how="left")[attribute_a]
    ser_b = matches[[id_b]].merge(df_b[[id_b, attribute_b]], on=id_b, how="left")[attribute_b]
    diffs = (ser_a.astype(float) - ser_b.astype(float)).abs().dropna().to_numpy()
    stats = _describe_numeric(diffs)
    return {f"{k}_diff": v for k, v in stats.items()}


def compute_pairwise_date_difference_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attribute_a: str,
    attribute_b: str,
    unit: str = "days"
) -> dict:
    """
    Compute statistics of temporal differences for date attributes in matched pairs.
    Returns a dictionary of stats in the specified unit.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    ser_a = pd.to_datetime(matches[[id_a]].merge(df_a[[id_a, attribute_a]], on=id_a, how="left")[attribute_a], errors="coerce")
    ser_b = pd.to_datetime(matches[[id_b]].merge(df_b[[id_b, attribute_b]], on=id_b, how="left")[attribute_b], errors="coerce")
    deltas = (ser_a - ser_b).dt
    if unit == "days":
        vals = deltas.days
    elif unit == "seconds":
        vals = deltas.total_seconds()
    else:
        # default to days if unknown
        vals = deltas.days
    values = pd.Series(vals).dropna().abs().to_numpy()
    stats = _describe_numeric(values)
    return {f"{k}_diff": v for k, v in stats.items()}


def compute_pairwise_jaccard_similarity_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attribute_a: str,
    attribute_b: str,
    tokenizer=str.split
) -> dict:
    """
    Compute Jaccard similarity statistics over token sets of matched pairs.
    Returns a dictionary of summary metrics.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    ser_a = matches[[id_a]].merge(df_a[[id_a, attribute_a]], on=id_a, how="left")[attribute_a]
    ser_b = matches[[id_b]].merge(df_b[[id_b, attribute_b]], on=id_b, how="left")[attribute_b]

    sims = []
    for a_val, b_val in zip(ser_a, ser_b):
        a_tokens = set(tokenizer(str(a_val))) if pd.notna(a_val) else set()
        b_tokens = set(tokenizer(str(b_val))) if pd.notna(b_val) else set()
        sims.append(_jaccard(a_tokens, b_tokens))
    sims = np.array(sims)
    stats = _describe_numeric(sims)
    return stats


def summarize_pairwise_similarity_for_attributes(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    attribute_pairs: list
) -> pd.DataFrame:
    """
    For a list of (attribute_a, attribute_b) pairs, compute high-level similarity
    statistics on the ground-truth matched pairs.
    Returns a DataFrame with one row per attribute pair and aggregated metrics.
    """
    rows = []
    for attr_a, attr_b in attribute_pairs:
        stats = compute_pairwise_string_similarity_stats(df_a, df_b, matches, attr_a, attr_b)
        rows.append({"attribute_a": attr_a, "attribute_b": attr_b, **stats})
    return pd.DataFrame(rows)


# ============================================================
# 7. Class imbalance and candidate space characteristics
# ============================================================

def compute_candidate_pair_space_size(df_a: pd.DataFrame, df_b: pd.DataFrame) -> int:
    """
    Compute the total number of possible candidate pairs: |A| * |B|.
    """
    return int(len(df_a) * len(df_b))


def compute_class_imbalance_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame
) -> dict:
    """
    Compute class imbalance characteristics for the matching task:
    - num_records_a
    - num_records_b
    - num_matches
    - candidate_space_size
    - match_ratio (matches / candidate_space_size)
    Returns a dictionary of metrics.
    """
    candidate_space = compute_candidate_pair_space_size(df_a, df_b)
    num_matches = len(matches)
    match_ratio = num_matches / candidate_space if candidate_space else 0.0
    return {
        "num_records_a": len(df_a),
        "num_records_b": len(df_b),
        "num_matches": num_matches,
        "candidate_space_size": candidate_space,
        "match_ratio": match_ratio,
    }


def compute_match_degree_distribution(matches: pd.DataFrame) -> dict:
    """
    Compute how many matches each record participates in:
    - degree distribution for A
    - degree distribution for B
    Returns a dictionary with summary stats for both sides.
    """
    if matches.empty:
        return {"degree_a": {}, "degree_b": {}}
    cols = list(matches.columns[:2])
    deg_a = matches[cols[0]].value_counts().to_dict()
    deg_b = matches[cols[1]].value_counts().to_dict()
    return {"degree_a": deg_a, "degree_b": deg_b}


# ============================================================
# 8. Temporal characteristics (if date attributes exist)
# ============================================================

def compute_temporal_coverage(df: pd.DataFrame, date_attribute: str) -> dict:
    """
    Compute the temporal coverage of a dataset:
    - min_date
    - max_date
    - span (in days)
    Returns a dictionary.
    """
    dates = pd.to_datetime(df[date_attribute], errors="coerce").dropna()
    if dates.empty:
        return {"min_date": None, "max_date": None, "span": None}
    min_d = dates.min()
    max_d = dates.max()
    span = (max_d - min_d).days
    return {"min_date": min_d, "max_date": max_d, "span": span}


def compute_temporal_overlap(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    date_attribute_a: str,
    date_attribute_b: str
) -> dict:
    """
    Compute temporal overlap between df_a and df_b:
    - overlap_span
    - overlap_ratio_a
    - overlap_ratio_b
    Returns a dictionary of metrics.
    """
    cov_a = compute_temporal_coverage(df_a, date_attribute_a)
    cov_b = compute_temporal_coverage(df_b, date_attribute_b)
    if cov_a["min_date"] is None or cov_b["min_date"] is None:
        return {"overlap_span": None, "overlap_ratio_a": None, "overlap_ratio_b": None}
    start = max(cov_a["min_date"], cov_b["min_date"])
    end = min(cov_a["max_date"], cov_b["max_date"])
    if end < start:
        return {"overlap_span": 0, "overlap_ratio_a": 0.0, "overlap_ratio_b": 0.0}
    overlap_span = (end - start).days
    ratio_a = overlap_span / cov_a["span"] if cov_a["span"] else 0.0
    ratio_b = overlap_span / cov_b["span"] if cov_b["span"] else 0.0
    return {"overlap_span": overlap_span, "overlap_ratio_a": ratio_a, "overlap_ratio_b": ratio_b}


def compute_temporal_drift_in_matches(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    date_attribute_a: str,
    date_attribute_b: str
) -> dict:
    """
    For ground-truth matches, compute distribution of date differences.
    Characterizes temporal drift between sources.
    Returns a dictionary of summary statistics.
    """
    return compute_pairwise_date_difference_stats(df_a, df_b, matches, date_attribute_a, date_attribute_b, unit="days")


# ============================================================
# 9. Blocking-related characteristics
# ============================================================

def simulate_blocking_pairs(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    blocking_key_a: str,
    blocking_key_b: str
) -> pd.DataFrame:
    """
    Simulate blocking by joining df_a and df_b on given blocking keys.
    Returns a DataFrame of candidate pairs with indices/id_a, id_b.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    paired = df_a[[id_a, blocking_key_a]].merge(df_b[[id_b, blocking_key_b]], left_on=blocking_key_a, right_on=blocking_key_b, how="inner", suffixes=("_a", "_b"))
    return paired[[id_a, id_b]]


def evaluate_blocking_key_quality(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    blocking_key_a: str,
    blocking_key_b: str
) -> dict:
    """
    Evaluate a blocking key in terms of:
    - blocking_recall: fraction of true matches that fall into at least one block
    - avg_block_size
    - num_blocks
    - reduction_ratio (1 - |candidates| / |A|*|B|)
    Returns a dictionary of metrics.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    candidates = simulate_blocking_pairs(df_a, df_b, blocking_key_a, blocking_key_b)
    candidate_space = compute_candidate_pair_space_size(df_a, df_b)
    reduction_ratio = 1 - (len(candidates) / candidate_space) if candidate_space else 0.0

    matches_set = set(zip(matches[id_a], matches[id_b]))
    candidate_set = set(zip(candidates[id_a], candidates[id_b]))
    hits = len(matches_set & candidate_set)
    blocking_recall = hits / len(matches_set) if matches_set else 0.0

    block_sizes = candidates.groupby(blocking_key_a if blocking_key_a in candidates else id_a).size() if not candidates.empty else pd.Series(dtype=int)
    avg_block_size = block_sizes.mean() if not block_sizes.empty else 0.0
    num_blocks = block_sizes.size

    return {
        "blocking_recall": blocking_recall,
        "avg_block_size": float(avg_block_size),
        "num_blocks": int(num_blocks),
        "reduction_ratio": reduction_ratio,
    }


def evaluate_multi_key_blocking_quality(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    blocking_key_pairs: list
) -> pd.DataFrame:
    """
    Evaluate multiple blocking key configurations.
    blocking_key_pairs is a list of (blocking_key_a, blocking_key_b).
    Returns a DataFrame with one row per configuration and metrics.
    """
    rows = []
    for key_a, key_b in blocking_key_pairs:
        metrics = evaluate_blocking_key_quality(df_a, df_b, matches, key_a, key_b)
        rows.append({"blocking_key_a": key_a, "blocking_key_b": key_b, **metrics})
    return pd.DataFrame(rows)


# ============================================================
# 10. Noise and outlier characteristics
# ============================================================

def estimate_typo_rate(
    df: pd.DataFrame,
    attribute: str,
    min_freq_for_vocab: int = 5
) -> float:
    """
    Estimate a simple 'typo rate' for a string attribute, using a vocabulary of frequent tokens
    and edit-distance-based heuristics.
    Returns a scalar in [0, 1] indicating estimated noisy/typo fraction.
    """
    if attribute not in df.columns or df.empty:
        return 0.0
    tokens = []
    for val in df[attribute].dropna().astype(str):
        tokens.extend(val.split())
    if not tokens:
        return 0.0
    freq = Counter(tokens)
    vocab = {tok for tok, cnt in freq.items() if cnt >= min_freq_for_vocab}
    if not vocab:
        return 0.0
    typo_flags = 0
    total = 0
    for tok, cnt in freq.items():
        total += cnt
        if tok in vocab:
            continue
        sim = max((_string_similarity(tok, v) for v in vocab), default=0.0)
        if sim > 0.8:
            typo_flags += cnt
    return typo_flags / total if total else 0.0


def detect_value_outliers(
    df: pd.DataFrame,
    attribute: str,
    method: str = "iqr"
) -> pd.Series:
    """
    Detect potential outliers for a numeric attribute.
    Returns a boolean Series indexed by records indicating outliers.
    """
    if attribute not in df.columns:
        raise KeyError(f"{attribute} not found.")
    series = pd.to_numeric(df[attribute], errors="coerce")
    if method == "iqr":
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        return (series < lower) | (series > upper)
    elif method == "zscore":
        mean = series.mean()
        std = series.std(ddof=1)
        if std == 0 or math.isnan(std):
            return pd.Series([False] * len(series), index=df.index)
        z = (series - mean).abs() / std
        return z > 3
    else:
        raise ValueError(f"Unknown method {method}")


def summarize_noise_indicators(
    df: pd.DataFrame,
    attributes: list
) -> pd.DataFrame:
    """
    Aggregate various noise indicators (e.g., typo_rate, outlier_fraction)
    across multiple attributes.
    Returns a DataFrame with one row per attribute.
    """
    rows = []
    for attr in attributes:
        if pd.api.types.is_numeric_dtype(df[attr]):
            outliers = detect_value_outliers(df, attr)
            rows.append({"attribute": attr, "outlier_fraction": float(outliers.mean()), "typo_rate": None})
        else:
            rows.append({"attribute": attr, "outlier_fraction": None, "typo_rate": float(estimate_typo_rate(df, attr))})
    return pd.DataFrame(rows)


# ============================================================
# 11. Name / text-specific characteristics (optional, but common)
# ============================================================

def compute_name_token_stats(
    df: pd.DataFrame,
    name_attribute: str
) -> dict:
    """
    Compute statistics specific to name-like attributes:
    - avg_tokens
    - fraction_single_token
    - fraction_threeplus_tokens
    Returns a dictionary.
    """
    if name_attribute not in df.columns:
        raise KeyError(f"{name_attribute} not found.")
    tokens = df[name_attribute].dropna().astype(str).map(lambda s: s.split())
    counts = tokens.map(len)
    if counts.empty:
        return {"avg_tokens": 0.0, "fraction_single_token": 0.0, "fraction_threeplus_tokens": 0.0}
    return {
        "avg_tokens": float(counts.mean()),
        "fraction_single_token": float((counts == 1).mean()),
        "fraction_threeplus_tokens": float((counts >= 3).mean()),
    }


def compute_name_order_variation_in_matches(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    name_attr_a: str,
    name_attr_b: str
) -> dict:
    """
    For matched pairs, estimate how often token orders differ between name_attr_a and name_attr_b.
    Returns a dictionary of summary metrics.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    ser_a = matches[[id_a]].merge(df_a[[id_a, name_attr_a]], on=id_a, how="left")[name_attr_a].fillna("")
    ser_b = matches[[id_b]].merge(df_b[[id_b, name_attr_b]], on=id_b, how="left")[name_attr_b].fillna("")
    different_order = 0
    total = 0
    for a_val, b_val in zip(ser_a, ser_b):
        tokens_a = a_val.split()
        tokens_b = b_val.split()
        if not tokens_a and not tokens_b:
            continue
        total += 1
        if Counter(tokens_a) == Counter(tokens_b) and tokens_a != tokens_b:
            different_order += 1
    ratio = different_order / total if total else 0.0
    return {"fraction_order_variation": ratio, "total_pairs_evaluated": total}


def estimate_alias_or_nickname_usage(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    name_attr_a: str,
    name_attr_b: str
) -> dict:
    """
    Heuristically estimate how often first-name-like tokens differ in non-typo ways
    (indicating nicknames/aliases) across matched pairs.
    Returns a dictionary.
    """
    id_a, id_b = get_id_columns(df_a, df_b)
    ser_a = matches[[id_a]].merge(df_a[[id_a, name_attr_a]], on=id_a, how="left")[name_attr_a].fillna("")
    ser_b = matches[[id_b]].merge(df_b[[id_b, name_attr_b]], on=id_b, how="left")[name_attr_b].fillna("")
    alias_count = 0
    total = 0
    for a_val, b_val in zip(ser_a, ser_b):
        first_a = a_val.split()[0] if a_val else ""
        first_b = b_val.split()[0] if b_val else ""
        if not first_a or not first_b:
            continue
        total += 1
        sim = _string_similarity(first_a.lower(), first_b.lower())
        if first_a.lower() != first_b.lower() and sim < 0.8:
            alias_count += 1
    return {"alias_or_nickname_fraction": alias_count / total if total else 0.0, "total_pairs_evaluated": total}


# ============================================================
# 12. Higher-level dataset “difficulty” aggregation
# ============================================================

def compute_dataset_difficulty_features(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matches: pd.DataFrame,
    config: dict = None
) -> pd.Series:
    """
    Compute a vector of 'difficulty features' for a benchmark:
    e.g.,
    - avg_missingness
    - avg_entropy
    - duplication_rate
    - match_ratio
    - avg_similarity_on_key_attributes
    - blocking_recall_using_default_keys
    Returns a Series of named features (ready to correlate with ER performance).
    """
    config = config or {}
    attrs = config.get("attributes", list(set(df_a.columns) & set(df_b.columns)))
    blocking_keys = config.get("blocking_keys", [])

    miss_a = compute_attribute_missingness(df_a, attrs)
    miss_b = compute_attribute_missingness(df_b, attrs)
    avg_missingness = float(pd.concat([miss_a["pct_missing"], miss_b["pct_missing"]]).mean()) if not miss_a.empty else 0.0

    entropies = []
    for col in attrs:
        if col in df_a.columns:
            entropies.append(compute_attribute_entropy(df_a, col))
        if col in df_b.columns:
            entropies.append(compute_attribute_entropy(df_b, col))
    avg_entropy = float(np.mean(entropies)) if entropies else 0.0

    duplication_rate = compute_exact_duplicate_rate(df_a) if not df_a.empty else 0.0
    imbalance = compute_class_imbalance_stats(df_a, df_b, matches)
    match_ratio = imbalance["match_ratio"]

    # Optional blocking recall using first blocking key pair if provided
    if blocking_keys:
        blk_metrics = evaluate_blocking_key_quality(df_a, df_b, matches, blocking_keys[0][0], blocking_keys[0][1])
        blocking_recall = blk_metrics["blocking_recall"]
    else:
        blocking_recall = None

    features = {
        "avg_missingness": avg_missingness,
        "avg_entropy": avg_entropy,
        "duplication_rate": duplication_rate,
        "match_ratio": match_ratio,
        "blocking_recall": blocking_recall,
    }
    return pd.Series(features)


def compute_dataset_difficulty_score(
    difficulty_features: pd.Series,
    weights: dict = None
) -> float:
    """
    Aggregate difficulty features into a single scalar difficulty score.
    Returns a float (higher = harder, or vice versa depending on design).
    """
    weights = weights or {}
    values = []
    for key, val in difficulty_features.items():
        if val is None:
            continue
        weight = weights.get(key, 1.0)
        values.append(weight * val)
    if not values:
        return 0.0
    return float(np.mean(values))


# ============================================================
# Backend interface and pandas implementation
# ============================================================

@runtime_checkable
class EntityResolutionEvalBackend(Protocol):
    """Backend-agnostic interface for entity-resolution evaluation utilities."""

    def compute_attribute_missingness(self, df: pd.DataFrame, attributes=None) -> pd.DataFrame: ...
    def compute_dataset_density(self, df: pd.DataFrame, attributes=None) -> float: ...
    def compare_attribute_missingness(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attributes=None) -> pd.DataFrame: ...
    def compute_missingness_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attributes_a=None, attributes_b=None) -> pd.DataFrame: ...

    def compute_attribute_value_frequencies(self, df: pd.DataFrame, attribute: str) -> pd.Series: ...
    def compute_attribute_entropy(self, df: pd.DataFrame, attribute: str) -> float: ...
    def compute_attribute_uniqueness(self, df: pd.DataFrame, attribute: str) -> float: ...
    def compute_attribute_most_common_ratio(self, df: pd.DataFrame, attribute: str) -> float: ...
    def summarize_attribute_discriminativeness(self, df: pd.DataFrame, attributes=None) -> pd.DataFrame: ...
    def compare_attribute_distributions(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute: str, method: str = "js_divergence") -> float: ...

    def infer_attribute_type(self, df: pd.DataFrame, attribute: str) -> str: ...
    def infer_attribute_types(self, df: pd.DataFrame, attributes=None) -> pd.Series: ...
    def compute_attribute_length_stats(self, df: pd.DataFrame, attribute: str) -> dict: ...
    def compute_attribute_token_stats(self, df: pd.DataFrame, attribute: str, tokenizer=str.split) -> dict: ...
    def compute_attribute_format_variability(self, df: pd.DataFrame, attribute: str) -> float: ...
    def compare_attribute_granularity(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute_a: str, attribute_b: str) -> dict: ...
    def compare_attribute_type_alignment(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute_pairs: list) -> pd.DataFrame: ...

    def compute_schema_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame: ...
    def compute_attribute_set_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict: ...
    def compute_schema_richness(self, df: pd.DataFrame) -> dict: ...
    def compare_schema_richness(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame: ...

    def compute_exact_duplicate_rate(self, df: pd.DataFrame, subset=None) -> float: ...
    def find_exact_duplicate_groups(self, df: pd.DataFrame, subset=None) -> pd.Series: ...
    def compute_near_duplicate_clusters(self, df: pd.DataFrame, attributes: list, similarity_threshold: float): ...
    def summarize_duplication(self, df: pd.DataFrame, attributes=None) -> dict: ...

    def compute_pairwise_string_similarity_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, similarity_function_name: str = "levenshtein") -> dict: ...
    def compute_pairwise_numeric_difference_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str) -> dict: ...
    def compute_pairwise_date_difference_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, unit: str = "days") -> dict: ...
    def compute_pairwise_jaccard_similarity_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, tokenizer=str.split) -> dict: ...
    def summarize_pairwise_similarity_for_attributes(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_pairs: list) -> pd.DataFrame: ...

    def compute_candidate_pair_space_size(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> int: ...
    def compute_class_imbalance_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame) -> dict: ...
    def compute_match_degree_distribution(self, matches: pd.DataFrame) -> dict: ...

    def compute_temporal_coverage(self, df: pd.DataFrame, date_attribute: str) -> dict: ...
    def compute_temporal_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame, date_attribute_a: str, date_attribute_b: str) -> dict: ...
    def compute_temporal_drift_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, date_attribute_a: str, date_attribute_b: str) -> dict: ...

    def simulate_blocking_pairs(self, df_a: pd.DataFrame, df_b: pd.DataFrame, blocking_key_a: str, blocking_key_b: str) -> pd.DataFrame: ...
    def evaluate_blocking_key_quality(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, blocking_key_a: str, blocking_key_b: str) -> dict: ...
    def evaluate_multi_key_blocking_quality(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, blocking_key_pairs: list) -> pd.DataFrame: ...

    def estimate_typo_rate(self, df: pd.DataFrame, attribute: str, min_freq_for_vocab: int = 5) -> float: ...
    def detect_value_outliers(self, df: pd.DataFrame, attribute: str, method: str = "iqr") -> pd.Series: ...
    def summarize_noise_indicators(self, df: pd.DataFrame, attributes: list) -> pd.DataFrame: ...

    def compute_name_token_stats(self, df: pd.DataFrame, name_attribute: str) -> dict: ...
    def compute_name_order_variation_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, name_attr_a: str, name_attr_b: str) -> dict: ...
    def estimate_alias_or_nickname_usage(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, name_attr_a: str, name_attr_b: str) -> dict: ...

    def compute_dataset_difficulty_features(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, config: dict = None) -> pd.Series: ...
    def compute_dataset_difficulty_score(self, difficulty_features: pd.Series, weights: dict = None) -> float: ...


class PandasEntityResolutionEvalBackend:
    """Pandas backend implementation of the entity-resolution evaluation interface."""

    def compute_attribute_missingness(self, df: pd.DataFrame, attributes=None) -> pd.DataFrame:
        return compute_attribute_missingness(df, attributes)

    def compute_dataset_density(self, df: pd.DataFrame, attributes=None) -> float:
        return compute_dataset_density(df, attributes)

    def compare_attribute_missingness(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attributes=None) -> pd.DataFrame:
        return compare_attribute_missingness(df_a, df_b, attributes)

    def compute_missingness_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attributes_a=None, attributes_b=None) -> pd.DataFrame:
        return compute_missingness_in_matches(df_a, df_b, matches, attributes_a, attributes_b)

    def compute_attribute_value_frequencies(self, df: pd.DataFrame, attribute: str) -> pd.Series:
        return compute_attribute_value_frequencies(df, attribute)

    def compute_attribute_entropy(self, df: pd.DataFrame, attribute: str) -> float:
        return compute_attribute_entropy(df, attribute)

    def compute_attribute_uniqueness(self, df: pd.DataFrame, attribute: str) -> float:
        return compute_attribute_uniqueness(df, attribute)

    def compute_attribute_most_common_ratio(self, df: pd.DataFrame, attribute: str) -> float:
        return compute_attribute_most_common_ratio(df, attribute)

    def summarize_attribute_discriminativeness(self, df: pd.DataFrame, attributes=None) -> pd.DataFrame:
        return summarize_attribute_discriminativeness(df, attributes)

    def compare_attribute_distributions(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute: str, method: str = "js_divergence") -> float:
        return compare_attribute_distributions(df_a, df_b, attribute, method)

    def infer_attribute_type(self, df: pd.DataFrame, attribute: str) -> str:
        return infer_attribute_type(df, attribute)

    def infer_attribute_types(self, df: pd.DataFrame, attributes=None) -> pd.Series:
        return infer_attribute_types(df, attributes)

    def compute_attribute_length_stats(self, df: pd.DataFrame, attribute: str) -> dict:
        return compute_attribute_length_stats(df, attribute)

    def compute_attribute_token_stats(self, df: pd.DataFrame, attribute: str, tokenizer=str.split) -> dict:
        return compute_attribute_token_stats(df, attribute, tokenizer)

    def compute_attribute_format_variability(self, df: pd.DataFrame, attribute: str) -> float:
        return compute_attribute_format_variability(df, attribute)

    def compare_attribute_granularity(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute_a: str, attribute_b: str) -> dict:
        return compare_attribute_granularity(df_a, df_b, attribute_a, attribute_b)

    def compare_attribute_type_alignment(self, df_a: pd.DataFrame, df_b: pd.DataFrame, attribute_pairs: list) -> pd.DataFrame:
        return compare_attribute_type_alignment(df_a, df_b, attribute_pairs)

    def compute_schema_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        return compute_schema_overlap(df_a, df_b)

    def compute_attribute_set_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
        return compute_attribute_set_overlap(df_a, df_b)

    def compute_schema_richness(self, df: pd.DataFrame) -> dict:
        return compute_schema_richness(df)

    def compare_schema_richness(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        return compare_schema_richness(df_a, df_b)

    def compute_exact_duplicate_rate(self, df: pd.DataFrame, subset=None) -> float:
        return compute_exact_duplicate_rate(df, subset)

    def find_exact_duplicate_groups(self, df: pd.DataFrame, subset=None) -> pd.Series:
        return find_exact_duplicate_groups(df, subset)

    def compute_near_duplicate_clusters(self, df: pd.DataFrame, attributes: list, similarity_threshold: float):
        return compute_near_duplicate_clusters(df, attributes, similarity_threshold)

    def summarize_duplication(self, df: pd.DataFrame, attributes=None) -> dict:
        return summarize_duplication(df, attributes)

    def compute_pairwise_string_similarity_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, similarity_function_name: str = "levenshtein") -> dict:
        return compute_pairwise_string_similarity_stats(df_a, df_b, matches, attribute_a, attribute_b, similarity_function_name)

    def compute_pairwise_numeric_difference_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str) -> dict:
        return compute_pairwise_numeric_difference_stats(df_a, df_b, matches, attribute_a, attribute_b)

    def compute_pairwise_date_difference_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, unit: str = "days") -> dict:
        return compute_pairwise_date_difference_stats(df_a, df_b, matches, attribute_a, attribute_b, unit)

    def compute_pairwise_jaccard_similarity_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_a: str, attribute_b: str, tokenizer=str.split) -> dict:
        return compute_pairwise_jaccard_similarity_stats(df_a, df_b, matches, attribute_a, attribute_b, tokenizer)

    def summarize_pairwise_similarity_for_attributes(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, attribute_pairs: list) -> pd.DataFrame:
        return summarize_pairwise_similarity_for_attributes(df_a, df_b, matches, attribute_pairs)

    def compute_candidate_pair_space_size(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> int:
        return compute_candidate_pair_space_size(df_a, df_b)

    def compute_class_imbalance_stats(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame) -> dict:
        return compute_class_imbalance_stats(df_a, df_b, matches)

    def compute_match_degree_distribution(self, matches: pd.DataFrame) -> dict:
        return compute_match_degree_distribution(matches)

    def compute_temporal_coverage(self, df: pd.DataFrame, date_attribute: str) -> dict:
        return compute_temporal_coverage(df, date_attribute)

    def compute_temporal_overlap(self, df_a: pd.DataFrame, df_b: pd.DataFrame, date_attribute_a: str, date_attribute_b: str) -> dict:
        return compute_temporal_overlap(df_a, df_b, date_attribute_a, date_attribute_b)

    def compute_temporal_drift_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, date_attribute_a: str, date_attribute_b: str) -> dict:
        return compute_temporal_drift_in_matches(df_a, df_b, matches, date_attribute_a, date_attribute_b)

    def simulate_blocking_pairs(self, df_a: pd.DataFrame, df_b: pd.DataFrame, blocking_key_a: str, blocking_key_b: str) -> pd.DataFrame:
        return simulate_blocking_pairs(df_a, df_b, blocking_key_a, blocking_key_b)

    def evaluate_blocking_key_quality(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, blocking_key_a: str, blocking_key_b: str) -> dict:
        return evaluate_blocking_key_quality(df_a, df_b, matches, blocking_key_a, blocking_key_b)

    def evaluate_multi_key_blocking_quality(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, blocking_key_pairs: list) -> pd.DataFrame:
        return evaluate_multi_key_blocking_quality(df_a, df_b, matches, blocking_key_pairs)

    def estimate_typo_rate(self, df: pd.DataFrame, attribute: str, min_freq_for_vocab: int = 5) -> float:
        return estimate_typo_rate(df, attribute, min_freq_for_vocab)

    def detect_value_outliers(self, df: pd.DataFrame, attribute: str, method: str = "iqr") -> pd.Series:
        return detect_value_outliers(df, attribute, method)

    def summarize_noise_indicators(self, df: pd.DataFrame, attributes: list) -> pd.DataFrame:
        return summarize_noise_indicators(df, attributes)

    def compute_name_token_stats(self, df: pd.DataFrame, name_attribute: str) -> dict:
        return compute_name_token_stats(df, name_attribute)

    def compute_name_order_variation_in_matches(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, name_attr_a: str, name_attr_b: str) -> dict:
        return compute_name_order_variation_in_matches(df_a, df_b, matches, name_attr_a, name_attr_b)

    def estimate_alias_or_nickname_usage(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, name_attr_a: str, name_attr_b: str) -> dict:
        return estimate_alias_or_nickname_usage(df_a, df_b, matches, name_attr_a, name_attr_b)

    def compute_dataset_difficulty_features(self, df_a: pd.DataFrame, df_b: pd.DataFrame, matches: pd.DataFrame, config: dict = None) -> pd.Series:
        return compute_dataset_difficulty_features(df_a, df_b, matches, config)

    def compute_dataset_difficulty_score(self, difficulty_features: pd.Series, weights: dict = None) -> float:
        return compute_dataset_difficulty_score(difficulty_features, weights)


# Default backend instance for convenience
pandas_backend = PandasEntityResolutionEvalBackend()
