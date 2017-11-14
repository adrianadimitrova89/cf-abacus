
export ORG_GUID='org2'
export SPACE_GUID='space1'
export DATE_IN_MS="$(date +%s000)"
export URL='http://localhost:9080/v1/metering/collected/usage'
export BODY="{\"start\":$DATE_IN_MS,\"end\":$DATE_IN_MS,\"organization_id\":\"$ORG_GUID\",\"space_id\":\"$SPACE_GUID\",\"resource_id\":\"linux-container\",\"plan_id\":\"basic\",\"consumer_id\":\"app:1fb61c1f-2db3-4235-9934-00097845b80d\",\"resource_instance_id\":\"1fb61c1f-2db3-4235-9934-00097845b80d\",\"measured_usage\":[{\"measure\":\"current_instance_memory\",\"quantity\":512},{\"measure\":\"current_running_instances\",\"quantity\":1},{\"measure\":\"previous_instance_memory\",\"quantity\":0},{\"measure\":\"previous_running_instances\",\"quantity\":0}]}"
curl -ik  -H 'Content-Type: application/json' -X POST -d $BODY $URL
