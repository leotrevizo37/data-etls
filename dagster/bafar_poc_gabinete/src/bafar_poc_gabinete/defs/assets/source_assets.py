from dagster import SourceAsset, AssetKey


dbo_sensors = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_sensors"]),
    group_name="bafar_poc_gabinete",
)
dbo_sensor_tys = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_sensor_tys"]),
    group_name="bafar_poc_gabinete",
)
dbo_devices = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_devices"]),
    group_name="bafar_poc_gabinete",
)
dbo_device_tys = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_device_tys"]),
    group_name="bafar_poc_gabinete",
)
dbo_sublocations = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_sublocations"]),
    group_name="bafar_poc_gabinete",
)
dbo_sublocation_tys = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_sublocation_tys"]),
    group_name="bafar_poc_gabinete",
)
dbo_aspnetusers = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_aspnetusers"]),
    group_name="bafar_poc_gabinete",
)
dbo_systemlogs = SourceAsset(
    key=AssetKey(["bafar_poc_gabinete", "dbo_systemlogs"]),
    group_name="bafar_poc_gabinete",
)
