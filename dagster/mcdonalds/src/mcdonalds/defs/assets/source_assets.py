from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey("dbo_sensors"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey("dbo_sensor_tys"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_devices = SourceAsset(
    key=AssetKey("dbo_devices"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_device_tys = SourceAsset(
    key=AssetKey("dbo_device_tys"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_sublocations = SourceAsset(
    key=AssetKey("dbo_sublocations"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey("dbo_sublocation_tys"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dwh_factlecturas = SourceAsset(
    key=AssetKey("dwh_factlecturas"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_ProgramProfileTys = SourceAsset(
    key=AssetKey("dbo_ProgramProfileTys"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_ProgramProfiles = SourceAsset(
    key=AssetKey("dbo_ProgramProfiles"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_ProgramSchedules = SourceAsset(
    key=AssetKey("dbo_ProgramSchedules"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_GroupSensors = SourceAsset(
    key=AssetKey("dbo_GroupSensors"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_GroupHasSchedules = SourceAsset(
    key=AssetKey("dbo_GroupHasSchedules"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
dbo_GroupHasSensors = SourceAsset(
    key=AssetKey("dbo_GroupHasSensors"),
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
