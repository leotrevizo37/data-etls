from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_sensors"]),
    group_name="mcdonalds",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_sensor_tys"]),
    group_name="mcdonalds",
)
dbo_devices = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_devices"]),
    group_name="mcdonalds",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_device_tys"]),
    group_name="mcdonalds",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_sublocations"]),
    group_name="mcdonalds",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_sublocation_tys"]),
    group_name="mcdonalds",
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_ProgramProfileTys"]),
    group_name="mcdonalds",
)
dbo_ProgramProfiles = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_ProgramProfiles"]),
    group_name="mcdonalds",
)
dbo_ProgramSchedules = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_ProgramSchedules"]),
    group_name="mcdonalds",
)
dbo_GroupSensors = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_GroupSensors"]),
    group_name="mcdonalds",
)
dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_GroupHasSchedules"]),
    group_name="mcdonalds",
)
dbo_GroupHasSensors = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_GroupHasSensors"]),
    group_name="mcdonalds",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_aspnetusers"]),
    group_name="mcdonalds",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["mcdonalds", "dbo_systemlogs"]),
    group_name="mcdonalds",
)
