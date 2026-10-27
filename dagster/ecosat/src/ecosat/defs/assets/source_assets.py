from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["ecosat", "dbo_sensors"]),
    group_name="ecosat",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["ecosat", "dbo_sensor_tys"]),
    group_name="ecosat",
)
dbo_devices = SourceAsset(
    key=AssetKey(["ecosat", "dbo_devices"]),
    group_name="ecosat",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["ecosat", "dbo_device_tys"]),
    group_name="ecosat",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["ecosat", "dbo_sublocations"]),
    group_name="ecosat",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["ecosat", "dbo_sublocation_tys"]),
    group_name="ecosat",
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["ecosat", "dbo_ProgramProfileTys"]),
    group_name="ecosat",
)

dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["ecosat", "dbo_ProgramProfiles"]),
    group_name="ecosat",
)

dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["ecosat", "dbo_ProgramSchedules"]),
    group_name="ecosat",
)

dbo_GroupSensors = SourceAsset(
    key=AssetKey(["ecosat", "dbo_GroupSensors"]),
    group_name="ecosat",
)

dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["ecosat", "dbo_GroupHasSchedules"]),
    group_name="ecosat",
)

dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["ecosat", "dbo_GroupHasSensors"]),
    group_name="ecosat",
)

dbo_aspnetusers = SourceAsset(
    key=AssetKey(["ecosat", "dbo_aspnetusers"]),
    group_name="ecosat",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["ecosat", "dbo_systemlogs"]),
    group_name="ecosat",
)
