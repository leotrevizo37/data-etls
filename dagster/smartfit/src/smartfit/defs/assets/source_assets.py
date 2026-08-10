from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["smartfit", "dbo_sensors"]),
    group_name="smartfit",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["smartfit", "dbo_sensor_tys"]),
    group_name="smartfit",
)
dbo_devices = SourceAsset(
    key=AssetKey(["smartfit", "dbo_devices"]),
    group_name="smartfit",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["smartfit", "dbo_device_tys"]),
    group_name="smartfit",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["smartfit", "dbo_sublocations"]),
    group_name="smartfit",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["smartfit", "dbo_sublocation_tys"]),
    group_name="smartfit",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["smartfit", "dbo_aspnetusers"]),
    group_name="smartfit",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["smartfit", "dbo_systemlogs"]),
    group_name="smartfit",
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["smartfit", "dbo_ProgramProfileTys"]),
    group_name="smartfit",
)
dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["smartfit", "dbo_ProgramProfiles"]),
    group_name="smartfit",
)
dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["smartfit", "dbo_ProgramSchedules"]),
    group_name="smartfit",
)
dbo_GroupSensors = SourceAsset(
    key=AssetKey(["smartfit", "dbo_GroupSensors"]),
    group_name="smartfit",
)
dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["smartfit", "dbo_GroupHasSchedules"]),
    group_name="smartfit",
)
dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["smartfit", "dbo_GroupHasSensors"]),
    group_name="smartfit",
)
