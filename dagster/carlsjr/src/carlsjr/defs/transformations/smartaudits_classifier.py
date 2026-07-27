from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import re
import unicodedata
from typing import Any, Iterable, Optional, Sequence


CATEGORY_COMPLIANCE = "CUMPLIMIENTO"
CATEGORY_IMAGE_NOT_LEGIBLE = "IMAGEN_NO_LEGIBLE"
CATEGORY_IMAGE_NOT_PROCESSABLE = "IMAGEN_NO_PROCESABLE"
CATEGORY_OUT_OF_RANGE = "FUERA_DE_RANGO"
CATEGORY_CLEANING_NON_COMPLIANCE = "INCUMPLIMIENTO_LIMPIEZA"
CATEGORY_GENERAL_NON_COMPLIANCE = "INCUMPLIMIENTO_GENERAL"
CATEGORY_UNCLASSIFIED = "SIN_CLASIFICAR"

RESULT_CATEGORY_ORDER = [
    CATEGORY_IMAGE_NOT_PROCESSABLE,
    CATEGORY_IMAGE_NOT_LEGIBLE,
    CATEGORY_OUT_OF_RANGE,
    CATEGORY_CLEANING_NON_COMPLIANCE,
    CATEGORY_GENERAL_NON_COMPLIANCE,
    CATEGORY_UNCLASSIFIED,
    CATEGORY_COMPLIANCE,
]

IMAGE_NOT_LEGIBLE_KEYWORDS = (
    "no es legible",
    "no es visible claramente",
    "no se distingue",
    "no se alcanza a ver",
    "borroso",
    "borrosa",
    "desenfoc",
)

IMAGE_NOT_PROCESSABLE_KEYWORDS = (
    "no se pudo completar",
    "no se puede detectar",
    "no se puede determinar",
    "no se pudo",
    "error al",
    "imagen no procesable",
    "no muestra un valor",
)

OUT_OF_RANGE_KEYWORDS = (
    "excede el rango",
    "fuera del rango",
    "fuera de rango",
    "por debajo del rango",
    "por encima del rango",
    "no esta dentro del rango",
)

CLEANING_KEYWORDS = (
    "suciedad",
    "sucio",
    "sucia",
    "mancha",
    "manchas",
    "basura",
    "derram",
    "limpieza",
    "desacomodo",
)

GENERAL_NON_COMPLIANCE_KEYWORDS = (
    "incumplimiento",
    "no corresponde",
    "no coincide",
    "incorrecto",
    "incorrecta",
    "evidencia incorrecta",
    "no cumple con la tarea",
)


@dataclass(frozen=True)
class Prediction:
    category: str
    confidence: float
    model_version: Optional[str]


@dataclass(frozen=True)
class TrainingResult:
    model: Any
    accuracy: float
    macro_f1: float
    training_rows: int


def normalize_comment(comment: Optional[str]) -> str:
    if comment is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(comment))
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text.lower()).strip()


def _contains_any(text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _matches_any_pattern(
    text: str,
    patterns: Sequence[str],
) -> bool:
    return any(
        re.search(pattern, text, flags=re.IGNORECASE) is not None
        for pattern in patterns
    )

def deterministic_result_category(
    comment: Optional[str],
    result_ai: Optional[int],
) -> Optional[str]:
    if result_ai == 1:
        return CATEGORY_COMPLIANCE

    normalized = normalize_comment(comment)
    if _contains_any(normalized, IMAGE_NOT_LEGIBLE_KEYWORDS):
        return CATEGORY_IMAGE_NOT_LEGIBLE
    if _contains_any(normalized, IMAGE_NOT_PROCESSABLE_KEYWORDS):
        return CATEGORY_IMAGE_NOT_PROCESSABLE
    if (
        _contains_any(normalized, OUT_OF_RANGE_KEYWORDS)
        or _matches_any_pattern(normalized, OUT_OF_RANGE_PATTERNS)
    ):
        return CATEGORY_OUT_OF_RANGE
    if _contains_any(normalized, CLEANING_KEYWORDS):
        return CATEGORY_CLEANING_NON_COMPLIANCE
    if _contains_any(normalized, GENERAL_NON_COMPLIANCE_KEYWORDS):
        return CATEGORY_GENERAL_NON_COMPLIANCE
    return None


def choose_mode_category(rows: Iterable[tuple[str, Optional[int]]]) -> str:
    counts: dict[str, int] = {}
    first_evidence: dict[str, int] = {}
    for category, evidence_photo_id in rows:
        counts[category] = counts.get(category, 0) + 1
        if evidence_photo_id is not None:
            current = first_evidence.get(category)
            first_evidence[category] = (
                evidence_photo_id
                if current is None
                else min(current, evidence_photo_id)
            )

    if not counts:
        return CATEGORY_UNCLASSIFIED

    priority = {category: index for index, category in enumerate(RESULT_CATEGORY_ORDER)}
    return sorted(
        counts,
        key=lambda category: (
            -counts[category],
            priority.get(category, len(priority)),
            first_evidence.get(category, 0),
        ),
    )[0]


def _load_sklearn():
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import SGDClassifier
        from sklearn.metrics import accuracy_score, f1_score
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import Pipeline
    except Exception as exc:
        raise RuntimeError("scikit-learn is required for SmartAudits training") from exc
    return TfidfVectorizer, SGDClassifier, accuracy_score, f1_score, train_test_split, Pipeline


def train_result_category_model(
    samples: Sequence[tuple[str, str]],
    min_training_rows: int = 50,
    random_state: int = 17,
) -> Optional[TrainingResult]:
    cleaned = [
        (normalize_comment(comment), category)
        for comment, category in samples
        if normalize_comment(comment) and category != CATEGORY_COMPLIANCE
    ]
    if len(cleaned) < min_training_rows:
        return None

    labels = {category for _, category in cleaned}
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

    texts = [comment for comment, _ in cleaned]
    y = [category for _, category in cleaned]
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

    return TrainingResult(
        model=model,
        accuracy=float(accuracy_score(test_y, predictions)),
        macro_f1=float(f1_score(test_y, predictions, average="macro")),
        training_rows=len(cleaned),
    )


def predict_result_category(
    model: Any,
    comment: Optional[str],
    model_version: Optional[str],
    threshold: float,
) -> Prediction:
    normalized = normalize_comment(comment)
    if not normalized:
        return Prediction(CATEGORY_UNCLASSIFIED, 0.0, model_version)

    probabilities = model.predict_proba([normalized])[0]
    classes = list(model.classes_)
    best_index = max(range(len(probabilities)), key=lambda index: probabilities[index])
    confidence = float(probabilities[best_index])
    if confidence < threshold:
        return Prediction(CATEGORY_UNCLASSIFIED, confidence, model_version)
    return Prediction(str(classes[best_index]), confidence, model_version)


def dump_model_bytes(model: Any) -> bytes:
    try:
        import joblib
    except Exception as exc:
        raise RuntimeError("joblib is required for SmartAudits model persistence") from exc

    buffer = BytesIO()
    joblib.dump(model, buffer)
    return buffer.getvalue()


def load_model_bytes(content: bytes) -> Any:
    try:
        import joblib
    except Exception as exc:
        raise RuntimeError("joblib is required for SmartAudits model persistence") from exc

    return joblib.load(BytesIO(content))
