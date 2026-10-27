from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["emerson", "dbo_sensors"]),
    group_name="emerson",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["emerson", "dbo_sensor_tys"]),
    group_name="emerson",
)
dbo_devices = SourceAsset(
    key=AssetKey(["emerson", "dbo_devices"]),
    group_name="emerson",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["emerson", "dbo_device_tys"]),
    group_name="emerson",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["emerson", "dbo_sublocations"]),
    group_name="emerson",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["emerson", "dbo_sublocation_tys"]),
    group_name="emerson",
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["emerson", "dbo_ProgramProfileTys"]),
    group_name="emerson",
)

dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["emerson", "dbo_ProgramProfiles"]),
    group_name="emerson",
)

dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["emerson", "dbo_ProgramSchedules"]),
    group_name="emerson",
)

dbo_GroupSensors = SourceAsset(
    key=AssetKey(["emerson", "dbo_GroupSensors"]),
    group_name="emerson",
)

dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["emerson", "dbo_GroupHasSchedules"]),
    group_name="emerson",
)

dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["emerson", "dbo_GroupHasSensors"]),
    group_name="emerson",
)

dbo_aspnetusers = SourceAsset(
    key=AssetKey(["emerson", "dbo_aspnetusers"]),
    group_name="emerson",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["emerson", "dbo_systemlogs"]),
    group_name="emerson",
)
