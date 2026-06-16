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
