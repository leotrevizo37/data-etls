from dagster import SourceAsset, AssetKey

dbo_sensors = SourceAsset(key=AssetKey("dbo_sensors"), group_name="sense_operations")
dbo_sensor_tys = SourceAsset(key=AssetKey("dbo_sensor_tys"), group_name="sense_operations")
dbo_devices = SourceAsset(key=AssetKey("dbo_devices"), group_name="sense_operations")
dbo_device_tys = SourceAsset(key=AssetKey("dbo_device_tys"), group_name="sense_operations")
dbo_sublocations = SourceAsset(key=AssetKey("dbo_sublocations"), group_name="sense_operations")
dbo_sublocation_tys = SourceAsset(key=AssetKey("dbo_sublocation_tys"), group_name="sense_operations")
dwh_factlecturas = SourceAsset(key=AssetKey("dwh_factlecturas"), group_name="sense_operations")
