import re
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from database import DeadLinkDatabase

ClassificationProgressCallback = Callable[[str, int, int], Awaitable[None]]


@dataclass
class ClassifiedSuggestion:
    suggestion_id: int
    dead_url: str
    suggested_url: str
    similarity_score: float
    confidence_score: float
    decision: str
    rationale: str


@dataclass
class ConfidenceFeatures:
    similarity_score: float
    same_host: float
    https_url: float
    path_prefix_ratio: float
    path_token_jaccard: float
    anchor_path_overlap: float
    query_penalty: float
    root_penalty: float
    depth_delta: float
    length_similarity: float


class MLConfidenceModel:
    """
    Lightweight feature-based logistic model.
    This behaves like a calibrated classifier without external ML dependencies.
    """

    def __init__(self) -> None:
        self.bias = -1.05
        self.weights = {
            "similarity_score": 3.0,
            "same_host": 1.1,
            "https_url": 0.35,
            "path_prefix_ratio": 1.55,
            "path_token_jaccard": 1.45,
            "anchor_path_overlap": 1.2,
            "query_penalty": -1.15,
            "root_penalty": -0.8,
            "depth_delta": -0.95,
            "length_similarity": 0.85,
        }
        self.temperature = 1.1

    def predict_proba(self, features: ConfidenceFeatures) -> float:
        logit = self.bias
        for feature_name, weight in self.weights.items():
            logit += getattr(features, feature_name) * weight
        calibrated = logit / self.temperature
        return self._sigmoid(calibrated)

    def explain(self, features: ConfidenceFeatures) -> str:
        contributions: List[tuple] = []
        for feature_name, weight in self.weights.items():
            value = getattr(features, feature_name)
            impact = value * weight
            contributions.append((feature_name, impact))

        contributions.sort(key=lambda item: abs(item[1]), reverse=True)
        top = contributions[:4]
        formatted = [f"{name}:{impact:+.3f}" for name, impact in top]
        return ",".join(formatted)

    def contributions(self, features: ConfidenceFeatures) -> List[Dict[str, float]]:
        rows: List[Dict[str, float]] = []
        for feature_name, weight in self.weights.items():
            value = getattr(features, feature_name)
            impact = value * weight
            rows.append(
                {
                    "feature": feature_name,
                    "value": round(float(value), 6),
                    "weight": round(float(weight), 6),
                    "impact": round(float(impact), 6),
                }
            )
        rows.sort(key=lambda item: abs(item["impact"]), reverse=True)
        return rows

    def _sigmoid(self, value: float) -> float:
        # numerically stable sigmoid
        if value >= 0:
            z = pow(2.718281828, -value)
            return 1 / (1 + z)
        z = pow(2.718281828, value)
        return z / (1 + z)


class SelfHealingClassifier:
    """Stage 3: classify replacement suggestions for auto vs manual handling."""

    def __init__(
        self,
        database: DeadLinkDatabase,
        auto_threshold: float = 0.75,
    ) -> None:
        self.database = database
        self.auto_threshold = auto_threshold
        self.model = MLConfidenceModel()

    async def classify(
        self,
        suggestions: Sequence[Dict[str, object]],
        progress_callback: Optional[ClassificationProgressCallback] = None,
        run_id: Optional[int] = None,
    ) -> tuple:
        classified_count = 0
        auto_count = 0

        for row in suggestions:
            classified = self._classify_row(row)
            self.database.insert_classification(
                suggestion_id=classified.suggestion_id,
                dead_url=classified.dead_url,
                suggested_url=classified.suggested_url,
                similarity_score=classified.similarity_score,
                confidence_score=classified.confidence_score,
                decision=classified.decision,
                rationale=classified.rationale,
                run_id=run_id,
            )
            classified_count += 1
            if classified.decision == "auto_replace":
                auto_count += 1

            if progress_callback is not None:
                await progress_callback(classified.suggested_url, classified_count, auto_count)

        return classified_count, auto_count

    def _classify_row(self, row: Dict[str, object]) -> ClassifiedSuggestion:
        suggestion_id = int(row.get("id", 0))
        dead_url = str(row.get("dead_url", ""))
        suggested_url = str(row.get("suggested_url", ""))
        anchor_text = str(row.get("anchor_text", ""))
        similarity_score = float(row.get("similarity_score", 0.0))

        features = self._build_features(
            dead_url=dead_url,
            suggested_url=suggested_url,
            anchor_text=anchor_text,
            similarity_score=similarity_score,
        )
        confidence_score = self.model.predict_proba(features)
        rationale = self.model.explain(features)
        decision = "auto_replace" if confidence_score >= self.auto_threshold else "manual_review"

        return ClassifiedSuggestion(
            suggestion_id=suggestion_id,
            dead_url=dead_url,
            suggested_url=suggested_url,
            similarity_score=similarity_score,
            confidence_score=confidence_score,
            decision=decision,
            rationale=rationale,
        )

    def _build_features(
        self,
        dead_url: str,
        suggested_url: str,
        anchor_text: str,
        similarity_score: float,
    ) -> ConfidenceFeatures:
        dead_parsed = urlparse(dead_url)
        suggested_parsed = urlparse(suggested_url)

        dead_parts = [p for p in dead_parsed.path.split("/") if p]
        suggested_parts = [p for p in suggested_parsed.path.split("/") if p]

        shared_prefix = self._shared_prefix_tokens(dead_parsed.path, suggested_parsed.path)
        max_depth = max(1, len(dead_parts), len(suggested_parts))
        path_prefix_ratio = shared_prefix / max_depth
        path_token_jaccard = self._token_overlap(dead_parsed.path, suggested_parsed.path)

        anchor_tokens = set(re.findall(r"[a-z0-9]{2,}", anchor_text.lower()))
        suggested_tokens = set(re.findall(r"[a-z0-9]{2,}", suggested_parsed.path.lower()))
        anchor_path_overlap = 0.0
        if anchor_tokens and suggested_tokens:
            anchor_path_overlap = len(anchor_tokens.intersection(suggested_tokens)) / len(anchor_tokens)

        query_penalty = min(1.0, len(suggested_parsed.query) / 40) if suggested_parsed.query else 0.0
        root_penalty = 1.0 if suggested_parsed.path in {"", "/"} else 0.0
        depth_delta = abs(len(dead_parts) - len(suggested_parts)) / max_depth

        dead_length = len(dead_parsed.path or "/")
        suggested_length = len(suggested_parsed.path or "/")
        length_similarity = 1 - min(1.0, abs(dead_length - suggested_length) / max(1, dead_length))

        return ConfidenceFeatures(
            similarity_score=max(0.0, min(1.0, similarity_score)),
            same_host=1.0 if dead_parsed.netloc and dead_parsed.netloc == suggested_parsed.netloc else 0.0,
            https_url=1.0 if suggested_parsed.scheme == "https" else 0.0,
            path_prefix_ratio=max(0.0, min(1.0, path_prefix_ratio)),
            path_token_jaccard=max(0.0, min(1.0, path_token_jaccard)),
            anchor_path_overlap=max(0.0, min(1.0, anchor_path_overlap)),
            query_penalty=max(0.0, min(1.0, query_penalty)),
            root_penalty=root_penalty,
            depth_delta=max(0.0, min(1.0, depth_delta)),
            length_similarity=max(0.0, min(1.0, length_similarity)),
        )

    def _shared_prefix_tokens(self, a: str, b: str) -> int:
        parts_a = [p for p in a.split("/") if p]
        parts_b = [p for p in b.split("/") if p]
        shared = 0
        for part_a, part_b in zip(parts_a, parts_b):
            if part_a != part_b:
                break
            shared += 1
        return shared

    def _token_overlap(self, a: str, b: str) -> float:
        tokens_a = set(re.findall(r"[a-z0-9]{2,}", a.lower()))
        tokens_b = set(re.findall(r"[a-z0-9]{2,}", b.lower()))
        if not tokens_a or not tokens_b:
            return 0.0
        return len(tokens_a.intersection(tokens_b)) / len(tokens_a.union(tokens_b))

    def explain_suggestion(self, row: Dict[str, object]) -> Dict[str, object]:
        dead_url = str(row.get("dead_url", ""))
        suggested_url = str(row.get("suggested_url", ""))
        anchor_text = str(row.get("anchor_text", ""))
        similarity_score = float(row.get("similarity_score", 0.0))

        features = self._build_features(
            dead_url=dead_url,
            suggested_url=suggested_url,
            anchor_text=anchor_text,
            similarity_score=similarity_score,
        )
        confidence_score = self.model.predict_proba(features)
        contributions = self.model.contributions(features)

        dead_path = urlparse(dead_url).path
        suggested_path = urlparse(suggested_url).path
        dead_tokens = sorted(set(re.findall(r"[a-z0-9]{2,}", dead_path.lower())))
        suggested_tokens = sorted(set(re.findall(r"[a-z0-9]{2,}", suggested_path.lower())))
        shared_tokens = sorted(set(dead_tokens).intersection(suggested_tokens))

        anchor_tokens = sorted(set(re.findall(r"[a-z0-9]{2,}", anchor_text.lower())))
        anchor_overlap = sorted(set(anchor_tokens).intersection(set(suggested_tokens)))

        level = "high" if confidence_score >= self.auto_threshold else "low"

        return {
            "dead_url": dead_url,
            "suggested_url": suggested_url,
            "similarity_score": round(similarity_score, 6),
            "confidence_score": round(confidence_score, 6),
            "decision_level": level,
            "features": {
                "similarity_score": round(features.similarity_score, 6),
                "same_host": round(features.same_host, 6),
                "https_url": round(features.https_url, 6),
                "path_prefix_ratio": round(features.path_prefix_ratio, 6),
                "path_token_jaccard": round(features.path_token_jaccard, 6),
                "anchor_path_overlap": round(features.anchor_path_overlap, 6),
                "query_penalty": round(features.query_penalty, 6),
                "root_penalty": round(features.root_penalty, 6),
                "depth_delta": round(features.depth_delta, 6),
                "length_similarity": round(features.length_similarity, 6),
            },
            "contributions": contributions,
            "token_matches": {
                "dead_path_tokens": dead_tokens,
                "suggested_path_tokens": suggested_tokens,
                "shared_path_tokens": shared_tokens,
                "anchor_tokens": anchor_tokens,
                "anchor_suggested_overlap": anchor_overlap,
            },
        }
