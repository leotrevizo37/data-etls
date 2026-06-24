from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import re
import unicodedata
from typing import Any, Optional, Sequence


@dataclass(frozen=True)
class MantizTextPrediction:
    label: str
    confidence: float
    model_version: Optional[str]


@dataclass(frozen=True)
class MantizTextTrainingResult:
    model: Any
    accuracy: float
    macro_f1: float
    training_rows: int


def normalize_mantiz_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(text))
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-zA-Z0-9 ]", " ", ascii_text)
    return re.sub(r"\s+", " ", cleaned.lower()).strip()


def _load_sklearn():
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import SGDClassifier
        from sklearn.metrics import accuracy_score, f1_score
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import Pipeline
    except Exception as exc:
        raise RuntimeError("scikit-learn is required for Mantiz text training") from exc
    return TfidfVectorizer, SGDClassifier, accuracy_score, f1_score, train_test_split, Pipeline


def train_mantiz_text_model(
    samples: Sequence[tuple[str, str]],
    min_training_rows: int = 25,
    random_state: int = 17,
) -> Optional[MantizTextTrainingResult]:
    cleaned = [
        (normalize_mantiz_text(text), label)
        for text, label in samples
        if normalize_mantiz_text(text) and label
    ]
    if len(cleaned) < min_training_rows:
        return None

    labels = {label for _, label in cleaned}
    if len(labels) < 2:
        return None

    (
        TfidfVectorizer,
        SGDClassifier,
        accuracy_score,
        f1_score,
        train_test_split,
        Pipeline,
    ) = _load_sklearn()

    texts = [text for text, _ in cleaned]
    y = [label for _, label in cleaned]
    try:
        train_x, test_x, train_y, test_y = train_test_split(
            texts,
            y,
            test_size=0.30,
            random_state=random_state,
            stratify=y,
        )
    except ValueError:
        train_x, test_x, train_y, test_y = texts, texts, y, y

    model = Pipeline(
        [
            ("tfidf", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5))),
            (
                "classifier",
                SGDClassifier(
                    loss="log_loss",
                    random_state=random_state,
                    max_iter=1500,
                    tol=1e-4,
                    class_weight="balanced",
                ),
            ),
        ]
    )
    model.fit(train_x, train_y)
    predictions = model.predict(test_x)

    return MantizTextTrainingResult(
        model=model,
        accuracy=float(accuracy_score(test_y, predictions)),
        macro_f1=float(f1_score(test_y, predictions, average="macro")),
        training_rows=len(cleaned),
    )


def predict_mantiz_text(
    model: Any,
    text: Optional[str],
    model_version: Optional[str],
    threshold: float,
) -> Optional[MantizTextPrediction]:
    normalized = normalize_mantiz_text(text)
    if not normalized:
        return None

    probabilities = model.predict_proba([normalized])[0]
    classes = list(model.classes_)
    best_index = max(range(len(probabilities)), key=lambda index: probabilities[index])
    confidence = float(probabilities[best_index])
    if confidence < threshold:
        return None
    return MantizTextPrediction(str(classes[best_index]), confidence, model_version)


def dump_mantiz_model_bytes(model: Any) -> bytes:
    try:
        import joblib
    except Exception as exc:
        raise RuntimeError("joblib is required for Mantiz model persistence") from exc

    buffer = BytesIO()
    joblib.dump(model, buffer)
    return buffer.getvalue()


def load_mantiz_model_bytes(content: bytes) -> Any:
    try:
        import joblib
    except Exception as exc:
        raise RuntimeError("joblib is required for Mantiz model persistence") from exc

    return joblib.load(BytesIO(content))
