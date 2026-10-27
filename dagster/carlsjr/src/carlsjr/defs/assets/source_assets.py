from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_sensors"]),
    group_name="carlsjr",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_sensor_tys"]),
    group_name="carlsjr",
)
dbo_devices = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_devices"]),
    group_name="carlsjr",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_device_tys"]),
    group_name="carlsjr",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_sublocations"]),
    group_name="carlsjr",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_sublocation_tys"]),
    group_name="carlsjr",
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_ProgramProfileTys"]),
    group_name="carlsjr",
)

dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_ProgramProfiles"]),
    group_name="carlsjr",
)

dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_ProgramSchedules"]),
    group_name="carlsjr",
)

dbo_GroupSensors = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_GroupSensors"]),
    group_name="carlsjr",
)

dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_GroupHasSchedules"]),
    group_name="carlsjr",
)

dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_GroupHasSensors"]),
    group_name="carlsjr",
)

dbo_aspnetusers = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_aspnetusers"]),
    group_name="carlsjr",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["carlsjr", "dbo_systemlogs"]),
    group_name="carlsjr",
)
