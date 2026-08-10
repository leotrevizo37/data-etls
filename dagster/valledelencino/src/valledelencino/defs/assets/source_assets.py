from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_sensors"]),
    group_name="valledelencino",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_sensor_tys"]),
    group_name="valledelencino",
)
dbo_devices = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_devices"]),
    group_name="valledelencino",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_device_tys"]),
    group_name="valledelencino",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_sublocations"]),
    group_name="valledelencino",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_sublocation_tys"]),
    group_name="valledelencino",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_aspnetusers"]),
    group_name="valledelencino",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["valledelencino", "dbo_systemlogs"]),
    group_name="valledelencino",
)
