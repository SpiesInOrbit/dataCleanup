"""Merge resolution for duplicate records."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

import pandas as pd


class MergeStrategy(Enum):
    """Strategies for resolving field conflicts during merge."""

    KEEP_FIRST = "keep_first"  # Keep value from first record
    KEEP_LAST = "keep_last"  # Keep value from last record
    KEEP_LONGEST = "keep_longest"  # Keep longest non-empty value
    KEEP_MOST_COMPLETE = "keep_most_complete"  # Keep record with most fields filled
    CONCATENATE = "concatenate"  # Concatenate unique values
    MANUAL = "manual"  # Require manual resolution


@dataclass
class MergeDecision:
    """Record of a merge decision for audit purposes."""

    field: str
    chosen_value: str
    source_index: int
    strategy: MergeStrategy
    alternatives: list[tuple[int, str]]  # Other values that were not chosen


@dataclass
class MergeResult:
    """Result of merging duplicate records."""

    merged_record: dict[str, Any]
    source_indices: list[int]
    decisions: list[MergeDecision]
    confidence: float


class MergeResolver:
    """
    Resolves duplicate records into a single merged record.

    Supports multiple merge strategies and tracks decisions
    for audit purposes.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        default_strategy: MergeStrategy = MergeStrategy.KEEP_MOST_COMPLETE,
        field_strategies: dict[str, MergeStrategy] | None = None,
    ) -> None:
        """
        Initialize the merge resolver.

        Args:
            dataframe: DataFrame containing records.
            default_strategy: Default strategy for resolving conflicts.
            field_strategies: Per-field strategy overrides.
        """
        self.df = dataframe
        self.default_strategy = default_strategy
        self.field_strategies = field_strategies or {}

    def merge_records(self, indices: list[int]) -> MergeResult:
        """
        Merge multiple records into one.

        Args:
            indices: List of record indices to merge.

        Returns:
            MergeResult with merged record and decisions.
        """
        if not indices:
            return MergeResult(
                merged_record={},
                source_indices=[],
                decisions=[],
                confidence=0.0,
            )

        if len(indices) == 1:
            record = self.df.iloc[indices[0]].to_dict()
            return MergeResult(
                merged_record=record,
                source_indices=indices,
                decisions=[],
                confidence=1.0,
            )

        records = self.df.iloc[indices]
        merged: dict[str, Any] = {}
        decisions: list[MergeDecision] = []

        for column in self.df.columns:
            strategy = self.field_strategies.get(column, self.default_strategy)
            decision = self._resolve_field(column, records, indices, strategy)
            merged[column] = decision.chosen_value
            decisions.append(decision)

        # Calculate confidence based on agreement
        agreement_scores = []
        for decision in decisions:
            if decision.alternatives:
                # Lower confidence if there were alternatives
                agreement_scores.append(0.5)
            else:
                agreement_scores.append(1.0)

        confidence = sum(agreement_scores) / len(agreement_scores) if agreement_scores else 1.0

        return MergeResult(
            merged_record=merged,
            source_indices=indices,
            decisions=decisions,
            confidence=confidence,
        )

    def _resolve_field(
        self,
        field: str,
        records: pd.DataFrame,
        indices: list[int],
        strategy: MergeStrategy,
    ) -> MergeDecision:
        """
        Resolve a single field across multiple records.

        Args:
            field: Field name.
            records: DataFrame slice with records to merge.
            indices: Original indices of records.
            strategy: Strategy to use for resolution.

        Returns:
            MergeDecision with chosen value and alternatives.
        """
        values = [(idx, str(records.loc[idx, field]).strip() if pd.notna(records.loc[idx, field]) else "")
                  for idx in records.index]

        # Filter to non-empty values
        non_empty = [(idx, val) for idx, val in values if val]

        if not non_empty:
            return MergeDecision(
                field=field,
                chosen_value="",
                source_index=indices[0],
                strategy=strategy,
                alternatives=[],
            )

        if len(non_empty) == 1:
            return MergeDecision(
                field=field,
                chosen_value=non_empty[0][1],
                source_index=non_empty[0][0],
                strategy=strategy,
                alternatives=[],
            )

        # Apply strategy
        chosen_idx, chosen_value = self._apply_strategy(non_empty, strategy, records)

        alternatives = [(idx, val) for idx, val in non_empty
                       if idx != chosen_idx and val != chosen_value]

        return MergeDecision(
            field=field,
            chosen_value=chosen_value,
            source_index=chosen_idx,
            strategy=strategy,
            alternatives=alternatives,
        )

    def _apply_strategy(
        self,
        values: list[tuple[int, str]],
        strategy: MergeStrategy,
        records: pd.DataFrame,
    ) -> tuple[int, str]:
        """
        Apply merge strategy to select a value.

        Args:
            values: List of (index, value) tuples.
            strategy: Strategy to apply.
            records: Full records for context.

        Returns:
            Tuple of (chosen_index, chosen_value).
        """
        if strategy == MergeStrategy.KEEP_FIRST:
            return values[0]

        elif strategy == MergeStrategy.KEEP_LAST:
            return values[-1]

        elif strategy == MergeStrategy.KEEP_LONGEST:
            return max(values, key=lambda x: len(x[1]))

        elif strategy == MergeStrategy.KEEP_MOST_COMPLETE:
            # Find record with most filled fields
            completeness = {}
            for idx, _ in values:
                filled = sum(1 for col in records.columns
                           if pd.notna(records.loc[idx, col])
                           and str(records.loc[idx, col]).strip())
                completeness[idx] = filled

            best_idx = max(completeness, key=completeness.get)  # type: ignore[arg-type]
            best_value = next(val for idx, val in values if idx == best_idx)
            return (best_idx, best_value)

        elif strategy == MergeStrategy.CONCATENATE:
            # Concatenate unique values
            unique_values = list(dict.fromkeys(val for _, val in values))
            combined = "; ".join(unique_values)
            return (values[0][0], combined)

        else:  # MANUAL or unknown - default to first
            return values[0]

    def merge_cluster(
        self,
        indices: list[int],
        preview: bool = False,
    ) -> MergeResult | pd.DataFrame:
        """
        Merge a cluster of duplicates.

        Args:
            indices: Record indices in the cluster.
            preview: If True, return comparison DataFrame instead of merging.

        Returns:
            MergeResult or preview DataFrame.
        """
        if preview:
            # Return side-by-side comparison
            comparison = self.df.iloc[indices].T
            comparison.columns = [f"Record {i+1}" for i in range(len(indices))]
            return comparison

        return self.merge_records(indices)

    def bulk_merge(
        self,
        clusters: list[list[int]],
    ) -> tuple[pd.DataFrame, list[MergeResult]]:
        """
        Merge multiple clusters and return unified DataFrame.

        Args:
            clusters: List of index lists, one per cluster.

        Returns:
            Tuple of (merged DataFrame, list of MergeResults).
        """
        results = []
        merged_records = []
        merged_indices: set[int] = set()

        for cluster in clusters:
            result = self.merge_records(cluster)
            results.append(result)
            merged_records.append(result.merged_record)
            merged_indices.update(cluster)

        # Include non-duplicate records
        all_indices = set(range(len(self.df)))
        singleton_indices = all_indices - merged_indices

        for idx in sorted(singleton_indices):
            merged_records.append(self.df.iloc[idx].to_dict())

        merged_df = pd.DataFrame(merged_records)
        return merged_df, results
