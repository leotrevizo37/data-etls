from __future__ import annotations

from functools import reduce
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


LIFECYCLE_COLUMNS = [
    "OrganizationId",
    "ClientId",
    "BranchId",
    "BranchName",
    "TicketId",
    "Folio",
    "EquipmentId",
    "EquipmentName",
    "EquipmentCatalogName",
    "FailureTypeId",
    "FailureTypeName",
    "SupplierId",
    "SupplierName",
    "TicketStatus",
    "TicketPriority",
    "TicketType",
    "TicketSource",
    "CreatedAt",
    "ScheduledAt",
    "ResolvedAt",
    "ApprovedAt",
    "CancelledAt",
    "UpdatedAt",
    "OpenToScheduledHours",
    "ScheduledToResolvedHours",
    "ResolvedToApprovedHours",
    "OpenToResolvedHours",
    "OpenToApprovedHours",
    "OpenToCancelledHours",
    "CurrentAgeHours",
    "IsOpen",
    "IsScheduled",
    "IsResolved",
    "IsApproved",
    "IsCancelled",
    "IsTerminal",
    "HasEquipment",
    "HasFailureType",
    "HasPhoto",
    "HasResolutionPhoto",
    "SourceLastChangedAt",
]

INSIGHT_DETAIL_COLUMNS = [
    "OrganizationId",
    "TicketId",
    "Folio",
    "BranchId",
    "EquipmentId",
    "EquipmentName",
    "SourceType",
    "SourceId",
    "SourceCreatedAt",
    "SourceUpdatedAt",
    "RawTextExcerpt",
    "NormalizedText",
    "InsightType",
    "InsightPhraseHash",
    "ExtractedEquipmentText",
    "ExtractedFailurePhrase",
    "NormalizedFailurePhrase",
    "FailurePhraseHash",
    "FailureConceptHash",
    "CanonicalFailureCode",
    "FailureConceptStatus",
    "ExtractedSolutionPhrase",
    "NormalizedSolutionPhrase",
    "SolutionPhraseHash",
    "SolutionConceptHash",
    "CanonicalSolutionCode",
    "SolutionConceptStatus",
    "ExtractedSolutionPartText",
    "NormalizedSolutionPartText",
    "SolutionPartHash",
    "SolutionPartConceptHash",
    "CanonicalSolutionPartName",
    "SolutionPartConceptStatus",
    "ExtractedInsightSubject",
    "ExtractedProblemQualifier",
    "NormalizedProblemQualifier",
    "ExtractionMethod",
    "ExtractionConfidence",
    "IsNewConceptCandidate",
    "SourceLastChangedAt",
]

INSIGHT_COLUMNS = [
    "OrganizationId",
    "TicketId",
    "Folio",
    "BranchId",
    "EquipmentId",
    "EquipmentName",
    "FailureCode",
    "FailureSubject",
    "FailureProblem",
    "SolutionCode",
    "SolutionSubject",
    "SolutionPartName",
    "SparePartName",
    "SparePartUnitPrice",
    "SparePartCurrency",
    "InsightStatus",
    "ClassificationMethod",
    "ClassifierConfidence",
    "NeedsReview",
    "InsightsJson",
    "TextEvidenceJson",
    "SourceLastChangedAt",
]

CLASSIFIER_PREDICTION_COLUMNS = [
    "OrganizationId",
    "TicketId",
    "SourceType",
    "SourceId",
    "InsightType",
    "CanonicalCode",
    "CanonicalName",
    "ClassifierModelVersion",
    "ClassifierConfidence",
]

REVIEW_QUEUE_COLUMNS = [
    "InsightType",
    "ConceptHash",
    "CanonicalCode",
    "CanonicalName",
    "RepresentativeText",
    "FirstSeenAt",
    "LastSeenAt",
    "CandidateCount",
    "TicketCount",
    "LastTicketId",
    "SuggestedByMethod",
]


def normalize_text_column(column):
    translated = F.translate(
        F.lower(F.coalesce(column.cast("string"), F.lit(""))),
        "\u00e1\u00e0\u00e4\u00e2\u00e3\u00e9\u00e8\u00eb\u00ea\u00ed\u00ec\u00ef\u00ee\u00f3\u00f2\u00f6\u00f4\u00f5\u00fa\u00f9\u00fc\u00fb\u00f1\u00e7",
        "aaaaaeeeeiiiiooooouuuunc",
    )
    return F.trim(F.regexp_replace(F.regexp_replace(translated, "[^a-z0-9 ]", " "), "\\s+", " "))


def _empty_to_null(column):
    return F.when(F.length(F.trim(column)) > F.lit(0), F.trim(column))


def _hours_between(start_column, end_column):
    return (
        F.when(
            start_column.isNotNull() & end_column.isNotNull(),
            ((F.unix_timestamp(end_column) - F.unix_timestamp(start_column)) / F.lit(3600.0)),
        )
        .otherwise(F.lit(None).cast("double"))
        .cast("decimal(18,4)")
    )


def _ticket_context_df(
    tickets_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
) -> DataFrame:
    return (
        tickets_df.alias("t")
        .join(branches_df.alias("b"), F.col("t.branchId") == F.col("b.id"), "left")
        .join(clients_df.alias("c"), F.col("b.clientId") == F.col("c.id"), "left")
        .join(
            equipment_instances_df.alias("ei"),
            F.col("t.equipmentId") == F.col("ei.id"),
            "left",
        )
        .join(
            equipment_catalog_df.alias("ec"),
            F.col("ei.catalogId") == F.col("ec.id"),
            "left",
        )
        .join(failure_types_df.alias("ft"), F.col("t.failureId") == F.col("ft.id"), "left")
        .join(suppliers_df.alias("s"), F.col("t.supplierId") == F.col("s.id"), "left")
        .filter(F.col("c.organizationId") == F.lit(organization_id))
        .select(
            F.col("c.organizationId").cast("string").alias("OrganizationId"),
            F.col("c.id").cast("string").alias("ClientId"),
            F.col("b.id").cast("string").alias("BranchId"),
            F.col("b.name").cast("string").alias("BranchName"),
            F.col("t.id").cast("string").alias("TicketId"),
            F.col("t.folio").cast("string").alias("Folio"),
            F.col("t.equipmentId").cast("string").alias("EquipmentId"),
            F.coalesce(
                F.col("ei.location").cast("string"),
                F.col("ei.customAssetId").cast("string"),
                F.col("ei.serialNumber").cast("string"),
                F.col("ec.name").cast("string"),
            ).alias("EquipmentName"),
            F.col("ec.name").cast("string").alias("EquipmentCatalogName"),
            F.col("t.failureId").cast("string").alias("FailureTypeId"),
            F.col("ft.name").cast("string").alias("FailureTypeName"),
            F.col("t.supplierId").cast("string").alias("SupplierId"),
            F.col("s.name").cast("string").alias("SupplierName"),
            F.col("t.status").cast("string").alias("TicketStatus"),
            F.col("t.priority").cast("string").alias("TicketPriority"),
            F.col("t.type").cast("string").alias("TicketType"),
            F.col("t.source").cast("string").alias("TicketSource"),
            F.col("t.createdAt").cast("timestamp").alias("CreatedAt"),
            F.col("t.scheduledAt").cast("timestamp").alias("ScheduledAt"),
            F.col("t.resolvedAt").cast("timestamp").alias("ResolvedAt"),
            F.col("t.approvedAt").cast("timestamp").alias("ApprovedAt"),
            F.col("t.cancelledAt").cast("timestamp").alias("CancelledAt"),
            F.col("t.updatedAt").cast("timestamp").alias("UpdatedAt"),
            F.col("t.photo").cast("string").alias("Photo"),
            F.col("t.resolutionPhoto").cast("string").alias("ResolutionPhoto"),
            F.col("t.title").cast("string").alias("Title"),
            F.col("t.description").cast("string").alias("Description"),
            F.col("t.scheduleNotes").cast("string").alias("ScheduleNotes"),
            F.col("t.resolutionNotes").cast("string").alias("ResolutionNotes"),
            F.col("t.approvalNotes").cast("string").alias("ApprovalNotes"),
            F.col("t.cancellationNotes").cast("string").alias("CancellationNotes"),
        )
    )


def build_mantiz_ticket_lifecycle_df(
    tickets_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
    as_of_timestamp: Optional[str] = None,
) -> DataFrame:
    base = _ticket_context_df(
        tickets_df,
        branches_df,
        clients_df,
        equipment_instances_df,
        equipment_catalog_df,
        failure_types_df,
        suppliers_df,
        organization_id,
    )
    status = normalize_text_column(F.col("TicketStatus"))
    is_cancelled = F.col("CancelledAt").isNotNull() | (status == F.lit("cancelado"))
    is_approved = F.col("ApprovedAt").isNotNull() | (status == F.lit("aprobado"))
    is_terminal = is_cancelled | is_approved
    as_of = F.lit(as_of_timestamp).cast("timestamp") if as_of_timestamp else F.current_timestamp()

    return (
        base
        .withColumn("OpenToScheduledHours", _hours_between(F.col("CreatedAt"), F.col("ScheduledAt")))
        .withColumn("ScheduledToResolvedHours", _hours_between(F.col("ScheduledAt"), F.col("ResolvedAt")))
        .withColumn("ResolvedToApprovedHours", _hours_between(F.col("ResolvedAt"), F.col("ApprovedAt")))
        .withColumn("OpenToResolvedHours", _hours_between(F.col("CreatedAt"), F.col("ResolvedAt")))
        .withColumn("OpenToApprovedHours", _hours_between(F.col("CreatedAt"), F.col("ApprovedAt")))
        .withColumn("OpenToCancelledHours", _hours_between(F.col("CreatedAt"), F.col("CancelledAt")))
        .withColumn(
            "CurrentAgeHours",
            F.when(~is_terminal, _hours_between(F.col("CreatedAt"), as_of))
            .otherwise(F.lit(None).cast("decimal(18,4)")),
        )
        .withColumn("IsCancelled", is_cancelled.cast("boolean"))
        .withColumn("IsApproved", is_approved.cast("boolean"))
        .withColumn("IsTerminal", is_terminal.cast("boolean"))
        .withColumn(
            "IsResolved",
            (F.col("ResolvedAt").isNotNull() | (status == F.lit("resuelto"))).cast("boolean"),
        )
        .withColumn(
            "IsScheduled",
            (F.col("ScheduledAt").isNotNull() | (status == F.lit("agendado"))).cast("boolean"),
        )
        .withColumn("IsOpen", (~is_terminal).cast("boolean"))
        .withColumn("HasEquipment", F.col("EquipmentId").isNotNull().cast("boolean"))
        .withColumn("HasFailureType", F.col("FailureTypeId").isNotNull().cast("boolean"))
        .withColumn(
            "HasPhoto",
            (F.length(F.trim(F.coalesce(F.col("Photo"), F.lit("")))) > F.lit(0)).cast("boolean"),
        )
        .withColumn(
            "HasResolutionPhoto",
            (F.length(F.trim(F.coalesce(F.col("ResolutionPhoto"), F.lit("")))) > F.lit(0)).cast("boolean"),
        )
        .withColumn(
            "SourceLastChangedAt",
            F.coalesce(
                F.col("UpdatedAt"),
                F.col("CreatedAt"),
                F.lit("1900-01-01 00:00:00").cast("timestamp"),
            ),
        )
        .select(*LIFECYCLE_COLUMNS)
    )


def build_mantiz_changed_ticket_keys_df(
    tickets_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
) -> DataFrame:
    return (
        _ticket_context_df(
            tickets_df,
            branches_df,
            clients_df,
            equipment_instances_df,
            equipment_catalog_df,
            failure_types_df,
            suppliers_df,
            organization_id,
        )
        .select("OrganizationId", "TicketId")
        .dropDuplicates(["OrganizationId", "TicketId"])
    )


def _source_texts_df(context_df: DataFrame, ticket_comments_df: DataFrame) -> DataFrame:
    ticket_sources = [
        ("TICKET_TITLE", "Title"),
        ("TICKET_DESCRIPTION", "Description"),
        ("TICKET_SCHEDULE_NOTES", "ScheduleNotes"),
        ("TICKET_RESOLUTION_NOTES", "ResolutionNotes"),
        ("TICKET_APPROVAL_NOTES", "ApprovalNotes"),
        ("TICKET_CANCELLATION_NOTES", "CancellationNotes"),
    ]
    frames = [
        context_df.select(
            "OrganizationId",
            "TicketId",
            "Folio",
            "BranchId",
            "EquipmentId",
            "EquipmentName",
            F.lit(source_type).alias("SourceType"),
            F.col("TicketId").alias("SourceId"),
            F.col("CreatedAt").alias("SourceCreatedAt"),
            F.col("UpdatedAt").alias("SourceUpdatedAt"),
            F.col(column).cast("string").alias("RawText"),
        )
        for source_type, column in ticket_sources
    ]

    comments = (
        ticket_comments_df.alias("tc")
        .join(
            context_df.select(
                "OrganizationId",
                "TicketId",
                "Folio",
                "BranchId",
                "EquipmentId",
                "EquipmentName",
            ).alias("ctx"),
            F.col("tc.ticketId") == F.col("ctx.TicketId"),
            "inner",
        )
        .select(
            F.col("ctx.OrganizationId"),
            F.col("ctx.TicketId"),
            F.col("ctx.Folio"),
            F.col("ctx.BranchId"),
            F.col("ctx.EquipmentId"),
            F.col("ctx.EquipmentName"),
            F.lit("TICKET_COMMENT").alias("SourceType"),
            F.col("tc.id").cast("string").alias("SourceId"),
            F.col("tc.createdAt").cast("timestamp").alias("SourceCreatedAt"),
            F.col("tc.updatedAt").cast("timestamp").alias("SourceUpdatedAt"),
            F.col("tc.content").cast("string").alias("RawText"),
        )
    )
    frames.append(comments)

    return (
        reduce(lambda left, right: left.unionByName(right), frames)
        .withColumn("RawTextExcerpt", F.substring(F.col("RawText"), 1, 2000))
        .withColumn("NormalizedText", normalize_text_column(F.col("RawText")))
        .withColumn(
            "SourceLastChangedAt",
            F.coalesce(
                F.col("SourceUpdatedAt"),
                F.col("SourceCreatedAt"),
                F.lit("1900-01-01 00:00:00").cast("timestamp"),
            ),
        )
        .filter(F.col("NormalizedText") != F.lit(""))
    )


def _failure_code_column(text_column):
    return (
        F.when(
            text_column.rlike("\\b(no enfria|no esta enfriando|no congela|sin frio|temperatura alta|no baja temperatura)\\b"),
            F.lit("NO_ENFRIA"),
        )
        .when(
            text_column.rlike("\\b(no calienta|sin calor|no levanta temperatura|quemador no prende)\\b"),
            F.lit("NO_CALIENTA"),
        )
        .when(
            text_column.rlike("\\b(no prende|no enciende|no arranca|no funciona|apagado|sin energia)\\b"),
            F.lit("NO_ENCIENDE"),
        )
        .when(
            text_column.rlike("\\b(fuga de agua|fuga agua|tira agua|sale agua|escurrimiento|gotea|gotera)\\b"),
            F.lit("FUGA_AGUA"),
        )
        .when(text_column.rlike("\\b(fuga de gas|fuga gas|huele a gas)\\b"), F.lit("FUGA_GAS"))
        .when(text_column.rlike("\\b(fuga|tira liquido|pierde liquido)\\b"), F.lit("FUGA"))
        .when(text_column.rlike("\\b(ruido|zumbido|vibra|vibracion|rechina)\\b"), F.lit("RUIDO"))
        .when(text_column.rlike("\\b(floja|flojo|suelta|suelto|desajustada|desajustado)\\b"), F.lit("FLOJO"))
        .when(text_column.rlike("\\b(roto|quebrado|danado|mal estado|descompuesto)\\b"), F.lit("DANADO"))
        .when(text_column.rlike("\\b(atascado|atorado|bloqueado|trabado)\\b"), F.lit("ATASCADO"))
        .when(text_column.rlike("\\b(puerta no cierra|no cierra|no sella|empaque)\\b"), F.lit("PUERTA_NO_CIERRA"))
        .when(text_column.rlike("\\b(sensor|termostato|display|alarma)\\b"), F.lit("FALLA_SENSOR"))
    )


def _solution_code_column(text_column):
    return (
        F.when(
            text_column.rlike("\\b(se cambia|se cambio|cambiaron|cambio de|reemplazo|reemplaza|reemplazar|sustituye|sustituyo|se instala|instalacion de)\\b"),
            F.lit("CAMBIO_REFACCION"),
        )
        .when(text_column.rlike("\\b(torque|torqueo)\\b"), F.lit("TORQUE"))
        .when(
            text_column.rlike("\\b(se apreto|se aprieta|apretar|aprieta|se ajusto|se ajusta|ajuste|se acomodo|se acomoda|acomodo|acomoda|acomodar|reacomodo|reacomoda|reacomodar|se nivela|nivelacion)\\b"),
            F.lit("AJUSTE"),
        )
        .when(text_column.rlike("\\b(se calibro|calibracion|calibro)\\b"), F.lit("CALIBRACION"))
        .when(text_column.rlike("\\b(se limpio|limpieza|limpio|retira suciedad|desazolva)\\b"), F.lit("LIMPIEZA"))
        .when(text_column.rlike("\\b(cable|conector|tarjeta|contacto|voltaje|electrico|electrica)\\b"), F.lit("REPARACION_ELECTRICA"))
        .when(text_column.rlike("\\b(recarga gas|carga gas|refrigerante)\\b"), F.lit("RECARGA_REFRIGERANTE"))
        .when(text_column.rlike("\\b(se reparo|reparacion|reparo|soldadura|solda)\\b"), F.lit("REPARACION"))
    )


def _problem_qualifier_column(text_column):
    return (
        F.when(text_column.rlike("\\b(no enfria|no esta enfriando|no congela|sin frio)\\b"), F.lit("NO_ENFRIA"))
        .when(text_column.rlike("\\b(no calienta|sin calor|no levanta temperatura)\\b"), F.lit("NO_CALIENTA"))
        .when(text_column.rlike("\\b(no prende|no enciende|no arranca|no funciona)\\b"), F.lit("NO_ENCIENDE"))
        .when(text_column.rlike("\\b(floja|flojo|suelta|suelto|desajustada|desajustado)\\b"), F.lit("FLOJA"))
        .when(text_column.rlike("\\b(fuga|gotea|gotera|tira agua|sale agua)\\b"), F.lit("FUGA"))
        .when(text_column.rlike("\\b(ruido|zumbido|vibra|vibracion|rechina)\\b"), F.lit("RUIDO"))
        .when(text_column.rlike("\\b(roto|quebrado|danado|mal estado|descompuesto)\\b"), F.lit("DANADO"))
        .when(text_column.rlike("\\b(atascado|atorado|bloqueado|trabado)\\b"), F.lit("ATASCADO"))
    )


def _base_insight_columns(df: DataFrame) -> DataFrame:
    return df.select(
        "OrganizationId",
        "TicketId",
        "Folio",
        "BranchId",
        "EquipmentId",
        "EquipmentName",
        "SourceType",
        "SourceId",
        "SourceCreatedAt",
        "SourceUpdatedAt",
        "RawTextExcerpt",
        "NormalizedText",
        "InsightType",
        "InsightPhraseHash",
        "ExtractedEquipmentText",
        "ExtractedFailurePhrase",
        "NormalizedFailurePhrase",
        "FailurePhraseHash",
        "FailureConceptHash",
        "CanonicalFailureCode",
        "FailureConceptStatus",
        "ExtractedSolutionPhrase",
        "NormalizedSolutionPhrase",
        "SolutionPhraseHash",
        "SolutionConceptHash",
        "CanonicalSolutionCode",
        "SolutionConceptStatus",
        "ExtractedSolutionPartText",
        "NormalizedSolutionPartText",
        "SolutionPartHash",
        "SolutionPartConceptHash",
        "CanonicalSolutionPartName",
        "SolutionPartConceptStatus",
        "ExtractedInsightSubject",
        "ExtractedProblemQualifier",
        "NormalizedProblemQualifier",
        "ExtractionMethod",
        "ExtractionConfidence",
        "IsNewConceptCandidate",
        "SourceLastChangedAt",
    )


def _failure_insights_df(sources_df: DataFrame) -> DataFrame:
    code = _failure_code_column(F.col("NormalizedText"))
    equipment_from_text = _empty_to_null(
        F.regexp_extract(
            F.col("NormalizedText"),
            "([a-z0-9]+(?: [a-z0-9]+){0,3}) (?:no enfria|no congela|no calienta|no prende|no enciende|fuga|gotea|ruido|roto|danado|atascado|floja|flojo|suelta|suelto)",
            1,
        )
    )
    problem = _problem_qualifier_column(F.col("NormalizedText"))
    return _base_insight_columns(
        sources_df
        .withColumn("CanonicalFailureCode", code)
        .filter(F.col("CanonicalFailureCode").isNotNull())
        .withColumn("InsightType", F.lit("FALLA"))
        .withColumn(
            "NormalizedFailurePhrase",
            F.lower(F.regexp_replace(F.col("CanonicalFailureCode"), "_", " ")),
        )
        .withColumn("ExtractedFailurePhrase", F.col("NormalizedFailurePhrase"))
        .withColumn("FailurePhraseHash", F.sha2(F.col("NormalizedFailurePhrase"), 256))
        .withColumn("FailureConceptHash", F.sha2(F.col("CanonicalFailureCode"), 256))
        .withColumn("FailureConceptStatus", F.lit("CANDIDATE"))
        .withColumn("InsightPhraseHash", F.col("FailurePhraseHash"))
        .withColumn("ExtractedEquipmentText", F.coalesce(equipment_from_text, F.col("EquipmentName")))
        .withColumn("ExtractedInsightSubject", F.coalesce(equipment_from_text, F.col("EquipmentName")))
        .withColumn("ExtractedProblemQualifier", F.coalesce(problem, F.col("CanonicalFailureCode")))
        .withColumn(
            "NormalizedProblemQualifier",
            normalize_text_column(F.coalesce(problem, F.col("CanonicalFailureCode"))),
        )
        .withColumn("ExtractedSolutionPhrase", F.lit(None).cast("string"))
        .withColumn("NormalizedSolutionPhrase", F.lit(None).cast("string"))
        .withColumn("SolutionPhraseHash", F.lit(None).cast("string"))
        .withColumn("SolutionConceptHash", F.lit(None).cast("string"))
        .withColumn("CanonicalSolutionCode", F.lit(None).cast("string"))
        .withColumn("SolutionConceptStatus", F.lit(None).cast("string"))
        .withColumn("ExtractedSolutionPartText", F.lit(None).cast("string"))
        .withColumn("NormalizedSolutionPartText", F.lit(None).cast("string"))
        .withColumn("SolutionPartHash", F.lit(None).cast("string"))
        .withColumn("SolutionPartConceptHash", F.lit(None).cast("string"))
        .withColumn("CanonicalSolutionPartName", F.lit(None).cast("string"))
        .withColumn("SolutionPartConceptStatus", F.lit(None).cast("string"))
        .withColumn("ExtractionMethod", F.lit("RULE"))
        .withColumn("ExtractionConfidence", F.lit(0.85).cast("double"))
        .withColumn("IsNewConceptCandidate", F.lit(True))
    )


def _solution_seed_df(sources_df: DataFrame) -> DataFrame:
    action_pattern = "(?:se cambia|se cambio|cambiaron|cambio de|reemplazo(?: de)?|reemplaza|reemplazar|sustituye|sustituyo|se instala|instalacion de|se apreto|se aprieta|apretar|aprieta|se ajusto|se ajusta|se acomodo|se acomoda|acomodo|acomoda|acomodar|reacomodo|reacomoda|reacomodar|se nivela|se calibro|se limpio|desazolva|se reparo|recarga|carga)"
    part_text = _empty_to_null(
        F.regexp_extract(
            F.col("NormalizedText"),
            action_pattern + " (?:el |la |los |las |un |una )?([a-z0-9]+)",
            1,
        )
    )
    problem = _problem_qualifier_column(F.col("NormalizedText"))
    return (
        sources_df
        .withColumn("CanonicalSolutionCode", _solution_code_column(F.col("NormalizedText")))
        .filter(F.col("CanonicalSolutionCode").isNotNull())
        .withColumn(
            "NormalizedSolutionPhrase",
            F.lower(F.regexp_replace(F.col("CanonicalSolutionCode"), "_", " ")),
        )
        .withColumn("ExtractedSolutionPhrase", F.col("NormalizedSolutionPhrase"))
        .withColumn("SolutionPhraseHash", F.sha2(F.col("NormalizedSolutionPhrase"), 256))
        .withColumn("SolutionConceptHash", F.sha2(F.col("CanonicalSolutionCode"), 256))
        .withColumn("SolutionConceptStatus", F.lit("CANDIDATE"))
        .withColumn("NormalizedSolutionPartText", part_text)
        .withColumn("ExtractedSolutionPartText", part_text)
        .withColumn(
            "CanonicalSolutionPartName",
            F.when(part_text.isNotNull(), F.upper(part_text)),
        )
        .withColumn("SolutionPartHash", F.when(part_text.isNotNull(), F.sha2(part_text, 256)))
        .withColumn(
            "SolutionPartConceptHash",
            F.when(F.col("CanonicalSolutionPartName").isNotNull(), F.sha2(F.col("CanonicalSolutionPartName"), 256)),
        )
        .withColumn(
            "SolutionPartConceptStatus",
            F.when(F.col("CanonicalSolutionPartName").isNotNull(), F.lit("CANDIDATE")),
        )
        .withColumn("ExtractedInsightSubject", F.coalesce(part_text, F.col("EquipmentName")))
        .withColumn("ExtractedProblemQualifier", problem)
        .withColumn("NormalizedProblemQualifier", normalize_text_column(problem))
    )


def _solution_insights_df(solution_seed_df: DataFrame) -> DataFrame:
    return _base_insight_columns(
        solution_seed_df
        .withColumn("InsightType", F.lit("SOLUCION"))
        .withColumn("InsightPhraseHash", F.col("SolutionPhraseHash"))
        .withColumn("ExtractedEquipmentText", F.col("ExtractedInsightSubject"))
        .withColumn("ExtractedFailurePhrase", F.lit(None).cast("string"))
        .withColumn("NormalizedFailurePhrase", F.lit(None).cast("string"))
        .withColumn("FailurePhraseHash", F.lit(None).cast("string"))
        .withColumn("FailureConceptHash", F.lit(None).cast("string"))
        .withColumn("CanonicalFailureCode", F.lit(None).cast("string"))
        .withColumn("FailureConceptStatus", F.lit(None).cast("string"))
        .withColumn("ExtractedSolutionPartText", F.lit(None).cast("string"))
        .withColumn("NormalizedSolutionPartText", F.lit(None).cast("string"))
        .withColumn("SolutionPartHash", F.lit(None).cast("string"))
        .withColumn("SolutionPartConceptHash", F.lit(None).cast("string"))
        .withColumn("CanonicalSolutionPartName", F.lit(None).cast("string"))
        .withColumn("SolutionPartConceptStatus", F.lit(None).cast("string"))
        .withColumn("ExtractionMethod", F.lit("RULE"))
        .withColumn("ExtractionConfidence", F.lit(0.85).cast("double"))
        .withColumn("IsNewConceptCandidate", F.lit(True))
    )


def _solution_part_insights_df(solution_seed_df: DataFrame) -> DataFrame:
    return _base_insight_columns(
        solution_seed_df
        .filter(F.col("CanonicalSolutionPartName").isNotNull())
        .withColumn("InsightType", F.lit("REFACCION_SOLUCION"))
        .withColumn("InsightPhraseHash", F.col("SolutionPartHash"))
        .withColumn("ExtractedEquipmentText", F.col("ExtractedInsightSubject"))
        .withColumn("ExtractedFailurePhrase", F.lit(None).cast("string"))
        .withColumn("NormalizedFailurePhrase", F.lit(None).cast("string"))
        .withColumn("FailurePhraseHash", F.lit(None).cast("string"))
        .withColumn("FailureConceptHash", F.lit(None).cast("string"))
        .withColumn("CanonicalFailureCode", F.lit(None).cast("string"))
        .withColumn("FailureConceptStatus", F.lit(None).cast("string"))
        .withColumn("ExtractionMethod", F.lit("RULE"))
        .withColumn("ExtractionConfidence", F.lit(0.80).cast("double"))
        .withColumn("IsNewConceptCandidate", F.lit(True))
    )


def build_mantiz_source_texts_df(
    tickets_df: DataFrame,
    ticket_comments_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
) -> DataFrame:
    context_df = _ticket_context_df(
        tickets_df,
        branches_df,
        clients_df,
        equipment_instances_df,
        equipment_catalog_df,
        failure_types_df,
        suppliers_df,
        organization_id,
    )
    return _source_texts_df(context_df, ticket_comments_df)


def empty_mantiz_classifier_predictions_df(spark) -> DataFrame:
    schema = StructType(
        [
            StructField("OrganizationId", StringType(), True),
            StructField("TicketId", StringType(), True),
            StructField("SourceType", StringType(), True),
            StructField("SourceId", StringType(), True),
            StructField("InsightType", StringType(), True),
            StructField("CanonicalCode", StringType(), True),
            StructField("CanonicalName", StringType(), True),
            StructField("ClassifierModelVersion", StringType(), True),
            StructField("ClassifierConfidence", DoubleType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


def _classifier_prediction_insights_df(sources_df: DataFrame, predictions_df: DataFrame) -> DataFrame:
    predictions = (
        predictions_df
        .select(*CLASSIFIER_PREDICTION_COLUMNS)
        .filter(F.col("CanonicalCode").isNotNull())
        .filter(F.col("CanonicalCode") != F.lit(""))
    )
    joined = (
        sources_df.alias("src")
        .join(
            predictions.alias("pred"),
            (
                (F.col("src.OrganizationId") == F.col("pred.OrganizationId"))
                & (F.col("src.TicketId") == F.col("pred.TicketId"))
                & (F.col("src.SourceType") == F.col("pred.SourceType"))
                & (F.col("src.SourceId") == F.col("pred.SourceId"))
            ),
            "inner",
        )
        .select(
            F.col("src.OrganizationId").alias("OrganizationId"),
            F.col("src.TicketId").alias("TicketId"),
            F.col("src.Folio").alias("Folio"),
            F.col("src.BranchId").alias("BranchId"),
            F.col("src.EquipmentId").alias("EquipmentId"),
            F.col("src.EquipmentName").alias("EquipmentName"),
            F.col("src.SourceType").alias("SourceType"),
            F.col("src.SourceId").alias("SourceId"),
            F.col("src.SourceCreatedAt").alias("SourceCreatedAt"),
            F.col("src.SourceUpdatedAt").alias("SourceUpdatedAt"),
            F.col("src.RawTextExcerpt").alias("RawTextExcerpt"),
            F.col("src.NormalizedText").alias("NormalizedText"),
            F.col("src.SourceLastChangedAt").alias("SourceLastChangedAt"),
            F.col("pred.InsightType").alias("PredInsightType"),
            F.col("pred.CanonicalCode").alias("PredCanonicalCode"),
            F.col("pred.CanonicalName").alias("PredCanonicalName"),
            F.col("pred.ClassifierConfidence").alias("PredClassifierConfidence"),
        )
    )
    insight_type = F.col("PredInsightType")
    code = F.upper(F.col("PredCanonicalCode"))
    name = F.coalesce(F.col("PredCanonicalName"), code)
    normalized_name = normalize_text_column(name)
    problem = _problem_qualifier_column(F.col("NormalizedText"))

    return _base_insight_columns(
        joined
        .withColumn("InsightType", insight_type)
        .withColumn("InsightPhraseHash", F.sha2(F.concat_ws("|", insight_type, code), 256))
        .withColumn("ExtractedEquipmentText", F.col("EquipmentName"))
        .withColumn(
            "ExtractedFailurePhrase",
            F.when(insight_type == F.lit("FALLA"), normalized_name),
        )
        .withColumn(
            "NormalizedFailurePhrase",
            F.when(insight_type == F.lit("FALLA"), normalized_name),
        )
        .withColumn(
            "FailurePhraseHash",
            F.when(insight_type == F.lit("FALLA"), F.sha2(normalized_name, 256)),
        )
        .withColumn(
            "FailureConceptHash",
            F.when(insight_type == F.lit("FALLA"), F.sha2(code, 256)),
        )
        .withColumn(
            "CanonicalFailureCode",
            F.when(insight_type == F.lit("FALLA"), code),
        )
        .withColumn(
            "FailureConceptStatus",
            F.when(insight_type == F.lit("FALLA"), F.lit("CANDIDATE")),
        )
        .withColumn(
            "ExtractedSolutionPhrase",
            F.when(insight_type == F.lit("SOLUCION"), normalized_name),
        )
        .withColumn(
            "NormalizedSolutionPhrase",
            F.when(insight_type == F.lit("SOLUCION"), normalized_name),
        )
        .withColumn(
            "SolutionPhraseHash",
            F.when(insight_type == F.lit("SOLUCION"), F.sha2(normalized_name, 256)),
        )
        .withColumn(
            "SolutionConceptHash",
            F.when(insight_type == F.lit("SOLUCION"), F.sha2(code, 256)),
        )
        .withColumn(
            "CanonicalSolutionCode",
            F.when(insight_type == F.lit("SOLUCION"), code),
        )
        .withColumn(
            "SolutionConceptStatus",
            F.when(insight_type == F.lit("SOLUCION"), F.lit("CANDIDATE")),
        )
        .withColumn(
            "ExtractedSolutionPartText",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), normalized_name),
        )
        .withColumn(
            "NormalizedSolutionPartText",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), normalized_name),
        )
        .withColumn(
            "SolutionPartHash",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), F.sha2(normalized_name, 256)),
        )
        .withColumn(
            "SolutionPartConceptHash",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), F.sha2(code, 256)),
        )
        .withColumn(
            "CanonicalSolutionPartName",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), code),
        )
        .withColumn(
            "SolutionPartConceptStatus",
            F.when(insight_type == F.lit("REFACCION_SOLUCION"), F.lit("CANDIDATE")),
        )
        .withColumn("ExtractionMethod", F.lit("ML"))
        .withColumn("ExtractionConfidence", F.col("PredClassifierConfidence").cast("double"))
        .withColumn("IsNewConceptCandidate", F.lit(True))
        .withColumn(
            "ExtractedInsightSubject",
            F.coalesce(
                F.col("NormalizedSolutionPartText"),
                F.col("ExtractedSolutionPartText"),
                F.col("EquipmentName"),
            ),
        )
        .withColumn(
            "ExtractedProblemQualifier",
            F.coalesce(problem, F.col("CanonicalFailureCode")),
        )
        .withColumn(
            "NormalizedProblemQualifier",
            normalize_text_column(F.coalesce(problem, F.col("CanonicalFailureCode"))),
        )
        .select(
            "OrganizationId",
            "TicketId",
            "Folio",
            "BranchId",
            "EquipmentId",
            "EquipmentName",
            "SourceType",
            "SourceId",
            "SourceCreatedAt",
            "SourceUpdatedAt",
            "RawTextExcerpt",
            "NormalizedText",
            "InsightType",
            "InsightPhraseHash",
            "ExtractedEquipmentText",
            "ExtractedFailurePhrase",
            "NormalizedFailurePhrase",
            "FailurePhraseHash",
            "FailureConceptHash",
            "CanonicalFailureCode",
            "FailureConceptStatus",
            "ExtractedSolutionPhrase",
            "NormalizedSolutionPhrase",
            "SolutionPhraseHash",
            "SolutionConceptHash",
            "CanonicalSolutionCode",
            "SolutionConceptStatus",
            "ExtractedSolutionPartText",
            "NormalizedSolutionPartText",
            "SolutionPartHash",
            "SolutionPartConceptHash",
            "CanonicalSolutionPartName",
            "SolutionPartConceptStatus",
            "ExtractedInsightSubject",
            "ExtractedProblemQualifier",
            "NormalizedProblemQualifier",
            "ExtractionMethod",
            "ExtractionConfidence",
            "IsNewConceptCandidate",
            "SourceLastChangedAt",
        )
    )


def build_mantiz_ticket_text_insight_detail_from_sources_df(
    sources_df: DataFrame,
    classifier_predictions_df: Optional[DataFrame] = None,
) -> DataFrame:
    solution_seed_df = _solution_seed_df(sources_df)
    insights = [
        _failure_insights_df(sources_df),
        _solution_insights_df(solution_seed_df),
        _solution_part_insights_df(solution_seed_df),
    ]
    if classifier_predictions_df is not None:
        insights.append(_classifier_prediction_insights_df(sources_df, classifier_predictions_df))
    return (
        reduce(lambda left, right: left.unionByName(right), insights)
        .dropDuplicates(
            [
                "OrganizationId",
                "TicketId",
                "SourceType",
                "SourceId",
                "InsightType",
                "InsightPhraseHash",
            ]
        )
        .select(*INSIGHT_DETAIL_COLUMNS)
    )


def build_mantiz_ticket_text_insight_detail_df(
    tickets_df: DataFrame,
    ticket_comments_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
    classifier_predictions_df: Optional[DataFrame] = None,
) -> DataFrame:
    context_df = _ticket_context_df(
        tickets_df,
        branches_df,
        clients_df,
        equipment_instances_df,
        equipment_catalog_df,
        failure_types_df,
        suppliers_df,
        organization_id,
    )
    sources_df = _source_texts_df(context_df, ticket_comments_df)
    return build_mantiz_ticket_text_insight_detail_from_sources_df(
        sources_df,
        classifier_predictions_df,
    )


def _source_priority_column():
    return (
        F.when(F.col("SourceType") == F.lit("TICKET_RESOLUTION_NOTES"), F.lit(7))
        .when(F.col("SourceType") == F.lit("TICKET_APPROVAL_NOTES"), F.lit(6))
        .when(F.col("SourceType") == F.lit("TICKET_CANCELLATION_NOTES"), F.lit(6))
        .when(F.col("SourceType") == F.lit("TICKET_COMMENT"), F.lit(5))
        .when(F.col("SourceType") == F.lit("TICKET_DESCRIPTION"), F.lit(4))
        .when(F.col("SourceType") == F.lit("TICKET_SCHEDULE_NOTES"), F.lit(3))
        .when(F.col("SourceType") == F.lit("TICKET_TITLE"), F.lit(2))
        .otherwise(F.lit(1))
    )


def _source_rollup_df(sources_df: DataFrame) -> DataFrame:
    return (
        sources_df
        .groupBy("OrganizationId", "TicketId")
        .agg(
            F.max("SourceLastChangedAt").alias("LastTextSourceChangedAt"),
            F.to_json(
                F.array_sort(
                    F.collect_set(
                        F.struct(
                            F.col("SourceType").alias("SourceType"),
                            F.col("SourceId").alias("SourceId"),
                            F.col("SourceCreatedAt").alias("SourceCreatedAt"),
                            F.col("SourceUpdatedAt").alias("SourceUpdatedAt"),
                            F.col("RawTextExcerpt").alias("RawTextExcerpt"),
                        )
                    )
                )
            ).alias("TextEvidenceJson"),
        )
    )


def _ranked_insights_df(insight_detail_df: DataFrame) -> DataFrame:
    sort_hash = F.coalesce(
        F.col("FailureConceptHash"),
        F.col("SolutionConceptHash"),
        F.col("SolutionPartConceptHash"),
        F.col("InsightPhraseHash"),
    )
    window = (
        Window
        .partitionBy("OrganizationId", "TicketId", "InsightType")
        .orderBy(
            F.col("ExtractionConfidence").desc_nulls_last(),
            F.col("SourcePriority").desc_nulls_last(),
            F.col("SourceLastChangedAt").desc_nulls_last(),
            sort_hash.asc_nulls_last(),
        )
    )
    return (
        insight_detail_df
        .withColumn("SourcePriority", _source_priority_column())
        .withColumn("DetailRank", F.row_number().over(window))
    )


def _detail_counts_df(insight_detail_df: DataFrame) -> DataFrame:
    return (
        insight_detail_df
        .groupBy("OrganizationId", "TicketId")
        .agg(
            F.count(F.lit(1)).cast("int").alias("DetailInsightCount"),
            F.max(F.when(F.col("ExtractionMethod") == F.lit("ML"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("HasMlMethod"),
            F.max("ExtractionConfidence").alias("ClassifierConfidence"),
            F.max(F.col("IsNewConceptCandidate").cast("int")).cast("boolean").alias("HasNewConceptCandidate"),
        )
    )


def _primary_failure_df(ranked_df: DataFrame) -> DataFrame:
    return (
        ranked_df
        .filter(F.col("InsightType") == F.lit("FALLA"))
        .filter(F.col("DetailRank") == F.lit(1))
        .select(
            "OrganizationId",
            "TicketId",
            F.col("CanonicalFailureCode").alias("FailureCode"),
            F.col("ExtractedInsightSubject").alias("FailureSubject"),
            F.col("ExtractedProblemQualifier").alias("FailureProblem"),
        )
    )


def _primary_solution_df(ranked_df: DataFrame) -> DataFrame:
    return (
        ranked_df
        .filter(F.col("InsightType") == F.lit("SOLUCION"))
        .filter(F.col("DetailRank") == F.lit(1))
        .select(
            "OrganizationId",
            "TicketId",
            F.col("CanonicalSolutionCode").alias("SolutionCode"),
            F.col("ExtractedInsightSubject").alias("SolutionSubject"),
            F.col("ExtractedProblemQualifier").alias("PrimarySolutionProblem"),
        )
    )


def _primary_solution_part_df(ranked_df: DataFrame) -> DataFrame:
    return (
        ranked_df
        .filter(F.col("InsightType") == F.lit("REFACCION_SOLUCION"))
        .filter(F.col("DetailRank") == F.lit(1))
        .select(
            "OrganizationId",
            "TicketId",
            F.col("CanonicalSolutionPartName").alias("SolutionPartName"),
            F.col("ExtractedProblemQualifier").alias("PrimarySolutionPartProblem"),
        )
    )


def _insights_json_df(insight_detail_df: DataFrame) -> DataFrame:
    code = F.coalesce(
        F.col("CanonicalFailureCode"),
        F.col("CanonicalSolutionCode"),
        F.col("CanonicalSolutionPartName"),
    )
    phrase = F.coalesce(
        F.col("NormalizedFailurePhrase"),
        F.col("NormalizedSolutionPhrase"),
        F.col("NormalizedSolutionPartText"),
    )
    concept_hash = F.coalesce(
        F.col("FailureConceptHash"),
        F.col("SolutionConceptHash"),
        F.col("SolutionPartConceptHash"),
    )
    return (
        insight_detail_df
        .groupBy("OrganizationId", "TicketId")
        .agg(
            F.to_json(
                F.array_sort(
                    F.collect_set(
                        F.struct(
                            F.col("InsightType").alias("Type"),
                            code.alias("Code"),
                            phrase.alias("Phrase"),
                            F.col("ExtractedInsightSubject").alias("Subject"),
                            F.col("ExtractedProblemQualifier").alias("Problem"),
                            F.col("CanonicalSolutionPartName").alias("PartName"),
                            F.col("ExtractionMethod").alias("Method"),
                            F.col("ExtractionConfidence").alias("Confidence"),
                            F.col("SourceType").alias("SourceType"),
                            F.col("SourceId").alias("SourceId"),
                            concept_hash.alias("ConceptHash"),
                        )
                    )
                )
            ).alias("InsightsJson")
        )
    )


def build_mantiz_ticket_text_insights_fact_df(
    tickets_df: DataFrame,
    ticket_comments_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
    insight_detail_df: Optional[DataFrame] = None,
    classifier_predictions_df: Optional[DataFrame] = None,
) -> DataFrame:
    context_df = _ticket_context_df(
        tickets_df,
        branches_df,
        clients_df,
        equipment_instances_df,
        equipment_catalog_df,
        failure_types_df,
        suppliers_df,
        organization_id,
    )
    sources_df = _source_texts_df(context_df, ticket_comments_df)
    detail_df = (
        insight_detail_df
        if insight_detail_df is not None
        else build_mantiz_ticket_text_insight_detail_from_sources_df(
            sources_df,
            classifier_predictions_df,
        )
    )
    source_rollup = _source_rollup_df(sources_df)
    ranked = _ranked_insights_df(detail_df)
    counts = _detail_counts_df(detail_df)
    insights_json = _insights_json_df(detail_df)

    fact = (
        context_df
        .select(
            "OrganizationId",
            "TicketId",
            "Folio",
            "BranchId",
            "EquipmentId",
            "EquipmentName",
            "CreatedAt",
            "UpdatedAt",
        )
        .join(source_rollup, ["OrganizationId", "TicketId"], "left")
        .join(counts, ["OrganizationId", "TicketId"], "left")
        .join(_primary_failure_df(ranked), ["OrganizationId", "TicketId"], "left")
        .join(_primary_solution_df(ranked), ["OrganizationId", "TicketId"], "left")
        .join(_primary_solution_part_df(ranked), ["OrganizationId", "TicketId"], "left")
        .join(insights_json, ["OrganizationId", "TicketId"], "left")
        .withColumn("DetailInsightCount", F.coalesce(F.col("DetailInsightCount"), F.lit(0)).cast("int"))
        .withColumn("InsightStatus", F.when(F.col("DetailInsightCount") > F.lit(0), F.lit("CLASSIFIED")).otherwise(F.lit("SIN_CLASIFICAR")))
        .withColumn(
            "ClassificationMethod",
            F.when(F.col("DetailInsightCount") == F.lit(0), F.lit("SIN_CLASIFICAR"))
            .when(F.coalesce(F.col("HasMlMethod"), F.lit(0)) > F.lit(0), F.lit("ML"))
            .otherwise(F.lit("RULE")),
        )
        .withColumn("ClassifierConfidence", F.coalesce(F.col("ClassifierConfidence"), F.lit(0.0)).cast("double"))
        .withColumn("NeedsReview", ((F.col("DetailInsightCount") == F.lit(0)) | F.coalesce(F.col("HasNewConceptCandidate"), F.lit(False))).cast("boolean"))
        .withColumn("InsightsJson", F.coalesce(F.col("InsightsJson"), F.lit("[]")))
        .withColumn("TextEvidenceJson", F.coalesce(F.col("TextEvidenceJson"), F.lit("[]")))
        .withColumn("SparePartName", F.col("SolutionPartName"))
        .withColumn("SparePartUnitPrice", F.lit(None).cast("decimal(18,4)"))
        .withColumn("SparePartCurrency", F.lit(None).cast("string"))
        .withColumn(
            "SourceLastChangedAt",
            F.greatest(
                F.coalesce(F.col("LastTextSourceChangedAt"), F.lit("1900-01-01 00:00:00").cast("timestamp")),
                F.coalesce(F.col("UpdatedAt"), F.col("CreatedAt"), F.lit("1900-01-01 00:00:00").cast("timestamp")),
            ),
        )
    )
    return fact.select(*INSIGHT_COLUMNS)


def build_mantiz_ticket_text_insights_df(
    tickets_df: DataFrame,
    ticket_comments_df: DataFrame,
    branches_df: DataFrame,
    clients_df: DataFrame,
    equipment_instances_df: DataFrame,
    equipment_catalog_df: DataFrame,
    failure_types_df: DataFrame,
    suppliers_df: DataFrame,
    organization_id: str,
    classifier_predictions_df: Optional[DataFrame] = None,
) -> DataFrame:
    detail_df = build_mantiz_ticket_text_insight_detail_df(
        tickets_df=tickets_df,
        ticket_comments_df=ticket_comments_df,
        branches_df=branches_df,
        clients_df=clients_df,
        equipment_instances_df=equipment_instances_df,
        equipment_catalog_df=equipment_catalog_df,
        failure_types_df=failure_types_df,
        suppliers_df=suppliers_df,
        organization_id=organization_id,
        classifier_predictions_df=classifier_predictions_df,
    )
    return build_mantiz_ticket_text_insights_fact_df(
        tickets_df=tickets_df,
        ticket_comments_df=ticket_comments_df,
        branches_df=branches_df,
        clients_df=clients_df,
        equipment_instances_df=equipment_instances_df,
        equipment_catalog_df=equipment_catalog_df,
        failure_types_df=failure_types_df,
        suppliers_df=suppliers_df,
        organization_id=organization_id,
        insight_detail_df=detail_df,
    )


def build_mantiz_text_insight_review_queue_df(insights_df: DataFrame) -> DataFrame:
    failure = (
        insights_df
        .filter(F.col("InsightType") == F.lit("FALLA"))
        .filter(F.col("FailureConceptHash").isNotNull())
        .select(
            "InsightType",
            F.col("FailureConceptHash").alias("ConceptHash"),
            F.col("CanonicalFailureCode").alias("CanonicalCode"),
            F.regexp_replace(F.col("CanonicalFailureCode"), "_", " ").alias("CanonicalName"),
            F.col("NormalizedFailurePhrase").alias("RepresentativeText"),
            "SourceLastChangedAt",
            "TicketId",
            "ExtractionMethod",
        )
    )
    solution = (
        insights_df
        .filter(F.col("InsightType") == F.lit("SOLUCION"))
        .filter(F.col("SolutionConceptHash").isNotNull())
        .select(
            "InsightType",
            F.col("SolutionConceptHash").alias("ConceptHash"),
            F.col("CanonicalSolutionCode").alias("CanonicalCode"),
            F.regexp_replace(F.col("CanonicalSolutionCode"), "_", " ").alias("CanonicalName"),
            F.col("NormalizedSolutionPhrase").alias("RepresentativeText"),
            "SourceLastChangedAt",
            "TicketId",
            "ExtractionMethod",
        )
    )
    part = (
        insights_df
        .filter(F.col("InsightType") == F.lit("REFACCION_SOLUCION"))
        .filter(F.col("SolutionPartConceptHash").isNotNull())
        .select(
            "InsightType",
            F.col("SolutionPartConceptHash").alias("ConceptHash"),
            F.col("CanonicalSolutionPartName").alias("CanonicalCode"),
            F.col("CanonicalSolutionPartName").alias("CanonicalName"),
            F.col("NormalizedSolutionPartText").alias("RepresentativeText"),
            "SourceLastChangedAt",
            "TicketId",
            "ExtractionMethod",
        )
    )

    return (
        reduce(lambda left, right: left.unionByName(right), [failure, solution, part])
        .groupBy("InsightType", "ConceptHash")
        .agg(
            F.max("CanonicalCode").alias("CanonicalCode"),
            F.max("CanonicalName").alias("CanonicalName"),
            F.min("RepresentativeText").alias("RepresentativeText"),
            F.min("SourceLastChangedAt").alias("FirstSeenAt"),
            F.max("SourceLastChangedAt").alias("LastSeenAt"),
            F.count(F.lit(1)).cast("int").alias("CandidateCount"),
            F.countDistinct("TicketId").cast("int").alias("TicketCount"),
            F.max("TicketId").alias("LastTicketId"),
            F.max("ExtractionMethod").alias("SuggestedByMethod"),
        )
        .select(*REVIEW_QUEUE_COLUMNS)
    )
