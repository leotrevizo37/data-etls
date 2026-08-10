from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_sensors"]),
    group_name="mcdonalds_cdp",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_sensor_tys"]),
    group_name="mcdonalds_cdp",
)
dbo_devices = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_devices"]),
    group_name="mcdonalds_cdp",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_device_tys"]),
    group_name="mcdonalds_cdp",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_sublocations"]),
    group_name="mcdonalds_cdp",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_sublocation_tys"]),
    group_name="mcdonalds_cdp",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_aspnetusers"]),
    group_name="mcdonalds_cdp",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["mcdonalds_cdp", "dbo_systemlogs"]),
    group_name="mcdonalds_cdp",
)
