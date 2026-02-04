"""Duplicate detection and record matching."""

from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd
from rapidfuzz import fuzz


@dataclass
class DuplicateCluster:
    """A cluster of potentially duplicate records."""

    cluster_id: int
    record_indices: list[int]
    confidence: float  # Average pairwise similarity
    field_similarities: dict[str, float]  # Per-field similarity scores


@dataclass
class MatchConfig:
    """Configuration for record matching."""

    # Fields to use for matching with their weights
    match_fields: dict[str, float] = field(default_factory=lambda: {
        "email": 1.0,
        "phone": 0.8,
        "first_name": 0.5,
        "last_name": 0.6,
        "company": 0.4,
    })

    # Threshold for considering records as duplicates
    duplicate_threshold: float = 0.8

    # Blocking fields - records must match on at least one to be compared
    blocking_fields: list[str] = field(default_factory=lambda: [
        "email", "phone", "last_name"
    ])


class RecordMatcher:
    """
    Detects duplicate records using fuzzy matching and blocking.

    Uses a blocking strategy to efficiently compare records,
    then applies weighted fuzzy matching across configured fields.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        config: MatchConfig | None = None,
    ) -> None:
        """
        Initialize the record matcher.

        Args:
            dataframe: DataFrame containing records to match.
            config: Matching configuration.
        """
        self.df = dataframe
        self.config = config or MatchConfig()
        self._clusters: list[DuplicateCluster] | None = None

    def find_duplicates(self) -> list[DuplicateCluster]:
        """
        Find all duplicate clusters in the dataset.

        Returns:
            List of DuplicateCluster objects.
        """
        if self._clusters is not None:
            return self._clusters

        # Get candidate pairs using blocking
        candidate_pairs = self._get_candidate_pairs()

        # Score each pair
        pair_scores: dict[tuple[int, int], dict[str, Any]] = {}
        for i, j in candidate_pairs:
            score, field_scores = self._score_pair(i, j)
            if score >= self.config.duplicate_threshold:
                pair_scores[(i, j)] = {
                    "score": score,
                    "field_scores": field_scores,
                }

        # Cluster connected pairs
        self._clusters = self._cluster_pairs(pair_scores)
        return self._clusters

    def _get_candidate_pairs(self) -> set[tuple[int, int]]:
        """
        Get candidate record pairs using blocking.

        Only records that share a blocking key will be compared,
        dramatically reducing the number of comparisons needed.

        Returns:
            Set of (index_i, index_j) pairs to compare.
        """
        candidates: set[tuple[int, int]] = set()

        for block_field in self.config.blocking_fields:
            if block_field not in self.df.columns:
                continue

            # Group by blocking field
            blocks = self._create_blocks(block_field)

            for indices in blocks.values():
                if len(indices) < 2:
                    continue
                # Add all pairs within the block
                for i in range(len(indices)):
                    for j in range(i + 1, len(indices)):
                        pair = (min(indices[i], indices[j]), max(indices[i], indices[j]))
                        candidates.add(pair)

        return candidates

    def _create_blocks(self, field: str) -> dict[str, list[int]]:
        """
        Create blocking groups for a field.

        Args:
            field: Field name to block on.

        Returns:
            Dictionary mapping block keys to record indices.
        """
        blocks: dict[str, list[int]] = {}

        for idx, value in enumerate(self.df[field]):
            if pd.isna(value) or str(value).strip() == "":
                continue

            # Create block key (normalized value)
            key = self._normalize_for_blocking(str(value), field)
            if key:
                if key not in blocks:
                    blocks[key] = []
                blocks[key].append(idx)

        return blocks

    def _normalize_for_blocking(self, value: str, field: str) -> str:
        """
        Normalize a value for blocking purposes.

        Args:
            value: Value to normalize.
            field: Field name for context-specific normalization.

        Returns:
            Normalized blocking key.
        """
        value = value.lower().strip()

        if field == "email":
            # Use email domain for blocking (less strict)
            if "@" in value:
                return value.split("@")[0][:4]  # First 4 chars of local part
        elif field == "phone":
            # Use last 4 digits
            digits = "".join(c for c in value if c.isdigit())
            return digits[-4:] if len(digits) >= 4 else digits
        elif field in ("last_name", "first_name"):
            # Use first 3 characters
            return value[:3] if len(value) >= 3 else value

        return value[:5] if len(value) >= 5 else value

    def _score_pair(
        self,
        idx_i: int,
        idx_j: int,
    ) -> tuple[float, dict[str, float]]:
        """
        Calculate similarity score between two records.

        Args:
            idx_i: Index of first record.
            idx_j: Index of second record.

        Returns:
            Tuple of (overall_score, field_scores).
        """
        field_scores: dict[str, float] = {}
        total_weight = 0.0
        weighted_score = 0.0

        for field, weight in self.config.match_fields.items():
            if field not in self.df.columns:
                continue

            val_i = str(self.df.iloc[idx_i][field]).strip()
            val_j = str(self.df.iloc[idx_j][field]).strip()

            # Skip if both are empty
            if not val_i and not val_j:
                continue

            # Calculate field similarity
            similarity = self._field_similarity(val_i, val_j, field)
            field_scores[field] = similarity

            weighted_score += similarity * weight
            total_weight += weight

        overall_score = weighted_score / total_weight if total_weight > 0 else 0.0
        return overall_score, field_scores

    def _field_similarity(
        self,
        val_i: str,
        val_j: str,
        field: str,
    ) -> float:
        """
        Calculate similarity between two field values.

        Args:
            val_i: First value.
            val_j: Second value.
            field: Field name for context-specific comparison.

        Returns:
            Similarity score between 0.0 and 1.0.
        """
        if not val_i or not val_j:
            return 0.0

        val_i = val_i.lower()
        val_j = val_j.lower()

        # Exact match
        if val_i == val_j:
            return 1.0

        # Field-specific comparison
        if field == "email":
            return 1.0 if val_i == val_j else 0.0  # Emails should match exactly
        elif field == "phone":
            # Compare digits only
            digits_i = "".join(c for c in val_i if c.isdigit())
            digits_j = "".join(c for c in val_j if c.isdigit())
            if digits_i == digits_j:
                return 1.0
            # Check if one contains the other (different formatting)
            if digits_i in digits_j or digits_j in digits_i:
                return 0.9
            return fuzz.ratio(digits_i, digits_j) / 100.0
        else:
            # General fuzzy matching
            return fuzz.token_sort_ratio(val_i, val_j) / 100.0

    def _cluster_pairs(
        self,
        pair_scores: dict[tuple[int, int], dict[str, Any]],
    ) -> list[DuplicateCluster]:
        """
        Cluster connected duplicate pairs using union-find.

        Args:
            pair_scores: Dictionary of scored pairs.

        Returns:
            List of DuplicateCluster objects.
        """
        if not pair_scores:
            return []

        # Union-find for clustering
        parent: dict[int, int] = {}

        def find(x: int) -> int:
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: int, y: int) -> None:
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Union all duplicate pairs
        for i, j in pair_scores.keys():
            union(i, j)

        # Group by cluster root
        clusters_dict: dict[int, list[int]] = {}
        for idx in parent.keys():
            root = find(idx)
            if root not in clusters_dict:
                clusters_dict[root] = []
            clusters_dict[root].append(idx)

        # Build cluster objects
        clusters = []
        for cluster_id, (root, indices) in enumerate(clusters_dict.items()):
            if len(indices) < 2:
                continue

            # Calculate average confidence and field scores
            total_score = 0.0
            total_pairs = 0
            aggregated_field_scores: dict[str, list[float]] = {}

            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    pair = (min(indices[i], indices[j]), max(indices[i], indices[j]))
                    if pair in pair_scores:
                        total_score += pair_scores[pair]["score"]
                        total_pairs += 1
                        for field, score in pair_scores[pair]["field_scores"].items():
                            if field not in aggregated_field_scores:
                                aggregated_field_scores[field] = []
                            aggregated_field_scores[field].append(score)

            avg_confidence = total_score / total_pairs if total_pairs > 0 else 0.0
            avg_field_scores = {
                field: sum(scores) / len(scores)
                for field, scores in aggregated_field_scores.items()
            }

            clusters.append(DuplicateCluster(
                cluster_id=cluster_id,
                record_indices=sorted(indices),
                confidence=avg_confidence,
                field_similarities=avg_field_scores,
            ))

        # Sort by confidence descending
        clusters.sort(key=lambda c: c.confidence, reverse=True)
        return clusters

    def get_duplicate_summary(self) -> pd.DataFrame:
        """
        Get a summary of duplicate clusters.

        Returns:
            DataFrame with duplicate cluster information.
        """
        clusters = self.find_duplicates()

        rows = []
        for cluster in clusters:
            rows.append({
                "cluster_id": cluster.cluster_id,
                "record_count": len(cluster.record_indices),
                "confidence": f"{cluster.confidence:.1%}",
                "indices": cluster.record_indices,
            })

        return pd.DataFrame(rows)

    def get_cluster_records(self, cluster_id: int) -> pd.DataFrame:
        """
        Get all records in a specific cluster.

        Args:
            cluster_id: ID of the cluster.

        Returns:
            DataFrame with records in the cluster.
        """
        clusters = self.find_duplicates()

        for cluster in clusters:
            if cluster.cluster_id == cluster_id:
                return self.df.iloc[cluster.record_indices].copy()

        return pd.DataFrame()
