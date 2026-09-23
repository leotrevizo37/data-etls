IF OBJECT_ID(N'serving.vwSenseDevEuiUplinkReadingsDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseDevEuiUplinkReadingsDaily;
    END;
GO

IF OBJECT_ID(N'observability.vwSenseDevEuiUplinkReadingsDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW observability.vwSenseDevEuiUplinkReadingsDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCommunicationHealthWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCommunicationHealthWeekly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCommunicationHealthDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCommunicationHealthDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCommunicationHealthHourly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCommunicationHealthHourly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseGatewayHealthHourly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseGatewayHealthHourly;
    END;
GO

IF OBJECT_ID(N'observability.vwChirpStackTenantGatewayHealthHourly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW observability.vwChirpStackTenantGatewayHealthHourly;
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factChirpStackDeviceMetrics;
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factChirpStackGatewayMetrics;
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factChirpStackDeviceEventMetrics;
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factChirpStackDeviceGatewayMetrics;
    END;
GO

IF OBJECT_ID(N'observability.factChirpStackDeviceMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factChirpStackDeviceMetrics;
    END;
GO

IF OBJECT_ID(N'observability.factChirpStackGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factChirpStackGatewayMetrics;
    END;
GO

IF OBJECT_ID(N'observability.factChirpStackDeviceEventMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factChirpStackDeviceEventMetrics;
    END;
GO

IF OBJECT_ID(N'observability.factChirpStackDeviceGatewayMetrics', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factChirpStackDeviceGatewayMetrics;
    END;
GO

IF OBJECT_ID(N'raw.MqttEventHistoryRejectedRow', N'U') IS NOT NULL
    BEGIN
        DROP TABLE raw.MqttEventHistoryRejectedRow;
    END;
GO

IF OBJECT_ID(N'ctl.IngestionControl', N'U') IS NOT NULL
    BEGIN
        DELETE FROM ctl.IngestionControl
        WHERE IngestionName = N'factChirpStackMetrics';
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorHealthDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorHealthDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorHealthWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorHealthWeekly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCoverageHourly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCoverageHourly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCoverageDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCoverageDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensorCoverageWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensorCoverageWeekly;
    END;
GO

IF OBJECT_ID(N'observability.vwFactRedingsAuditsDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW observability.vwFactRedingsAuditsDaily;
    END;
GO

IF OBJECT_ID(N'observability.vwFactRedingsAuditsWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW observability.vwFactRedingsAuditsWeekly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseReadingsDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseReadingsDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseReadingsWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseReadingsWeekly;
    END;
GO

IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW dwh.vwFactReadingsMeasurementWeekly;
    END;
GO

IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW dwh.vwFactReadingsMeasurementDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseReadingsHourly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseReadingsHourly;
    END;
GO

IF OBJECT_ID(N'serving.vwSenseSensors', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSenseSensors;
    END;
GO

IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementDevice', N'V') IS NOT NULL
    BEGIN
        DROP VIEW dwh.vwFactReadingsMeasurementDevice;
    END;
GO

IF OBJECT_ID(N'stg.MantizTextInsightReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTextInsightReviewQueue;
    END;
GO

IF OBJECT_ID(N'stg.MantizTicketTextInsightDetail', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTicketTextInsightDetail;
    END;
GO

IF OBJECT_ID(N'stg.MantizTicketTextInsightChangedTicket', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTicketTextInsightChangedTicket;
    END;
GO

IF OBJECT_ID(N'stg.factMantizTicketTextInsights', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factMantizTicketTextInsights;
    END;
GO

IF OBJECT_ID(N'stg.factMantizTicketLifecycle', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factMantizTicketLifecycle;
    END;
GO

IF OBJECT_ID(N'stg.SmartauditsAiCommentReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.SmartauditsAiCommentReviewQueue;
    END;
GO

IF OBJECT_ID(N'stg.factSmartauditsCategories', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factSmartauditsCategories;
    END;
GO

IF OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factReadingsMeasurement;
    END;
GO

IF OBJECT_ID(N'ctl.MantizTextInsightReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.MantizTextInsightReviewQueue;
    END;
GO

IF OBJECT_ID(N'ctl.MantizTextInsightClassifierModels', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.MantizTextInsightClassifierModels;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsAiCommentReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsAiCommentReviewQueue;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsClassifierModels', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsClassifierModels;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsAiCommentLookup', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsAiCommentLookup;
    END;
GO

IF OBJECT_ID(N'dwh.factMantizTicketTextInsights', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factMantizTicketTextInsights;
    END;
GO

IF OBJECT_ID(N'dwh.factMantizTicketLifecycle', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factMantizTicketLifecycle;
    END;
GO

IF OBJECT_ID(N'dwh.factSmartauditsCategories', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factSmartauditsCategories;
    END;
GO

IF OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factSensorEvents;
    END;
GO

IF OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factReadingsMeasurement;
    END;
GO

IF OBJECT_ID(N'dwh.factLecturas', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factLecturas;
    END;
GO

IF OBJECT_ID(N'dwh.dimSensorExpectedVal', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimSensorExpectedVal;
    END;
GO

IF OBJECT_ID(N'dwh.dimSidonProdDimensions', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimSidonProdDimensions;
    END;
GO

IF OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factRedingsAudits;
    END;
GO

IF OBJECT_ID(N'stg.dimAspNetUsers', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.dimAspNetUsers;
    END;
GO

IF OBJECT_ID(N'stg.factSidonUserUsage', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factSidonUserUsage;
    END;
GO

IF OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factRedingsAudits;
    END;
GO

IF OBJECT_ID(N'observability.dimAspNetUsers', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.dimAspNetUsers;
    END;
GO

IF OBJECT_ID(N'observability.factSidonUserUsage', N'U') IS NOT NULL
    BEGIN
        DROP TABLE observability.factSidonUserUsage;
    END;
GO
