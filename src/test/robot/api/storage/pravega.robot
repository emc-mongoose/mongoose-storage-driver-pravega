*** Settings ***
Library  OperatingSystem
Library  CSVLibrary
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${DATA_DIR} =  src/test/robot/api/storage/data
${LOG_DIR} =  build/log
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose-storage-driver-pravega
${MONGOOSE_CONTAINER_DATA_DIR} =  /data
${MONGOOSE_CONTAINER_NAME} =  mongoose-storage-driver-pravega

*** Test Cases ***
Create Event Stream Test
    [Tags]  create_event_stream
    ${node_addr} =  Get Environment Variable  SERVICE_HOST  127.0.0.1
    ${step_id} =  Set Variable  create_event_stream_test
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --load-op-limit-count=1000
    ...  --storage-driver-limit-concurrency=10
    ...  --storage-namespace=scope1
    ...  --storage-net-node-addrs=${node_addr}
    &{env_params} =  Create Dictionary
    ${std_out} =  Execute Mongoose Scenario  ${DATA_DIR}  ${env_params}  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  CREATE  1000  0  1048576000

Create Byte Streams Test
    [Tags]  create_byte_streams
    ${node_addr} =  Get Environment Variable  SERVICE_HOST  127.0.0.1
    ${step_id} =  Set Variable  create_byte_streams_test
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --load-op-limit-count=100
    ...  --storage-driver-limit-concurrency=10
    ...  --storage-driver-stream-data=bytes
    ...  --storage-namespace=scope2
    ...  --storage-net-node-addrs=${node_addr}
    &{env_params} =  Create Dictionary
    ${std_out} =  Execute Mongoose Scenario  ${DATA_DIR}  ${env_params}  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  CREATE  100  0  104857600

Read Byte Streams Test
    [Tags]  read_byte_streams
    ${node_addr} =  Get Environment Variable  SERVICE_HOST  127.0.0.1
    ${step_id} =  Set Variable  read_byte_streams_test
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --load-op-limit-count=100
    ...  --storage-driver-limit-concurrency=10
    ...  --storage-driver-stream-data=bytes
    ...  --storage-namespace=scope3
    ...  --storage-net-node-addrs=${node_addr}
    ...  --run-scenario=${MONGOOSE_CONTAINER_DATA_DIR}/read.js
    &{env_params} =  Create Dictionary  ITEM_LIST_FILE=${MONGOOSE_CONTAINER_DATA_DIR}/${step_id}.csv
    ${std_out} =  Execute Mongoose Scenario  ${DATA_DIR}  ${env_params}  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  READ  100  0  104857600

*** Keyword ***
Execute Mongoose Scenario
    [Timeout]  5 minutes
    [Arguments]   ${shared_data_dir}  ${env}  ${args}
    ${docker_env_vars} =  Evaluate  ' '.join(['-e %s=%s' % (key, value) for (key, value) in ${env}.items()])
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  Host working dir: ${host_working_dir}
    ${base_version} =  Get Environment Variable  BASE_VERSION
    ${image_version} =  Get Environment Variable  VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${MONGOOSE_CONTAINER_NAME}
    ...  --network host
    ...  ${docker_env_vars}
    ...  --volume ${host_working_dir}/${shared_data_dir}:${MONGOOSE_CONTAINER_DATA_DIR}
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.mongoose/${base_version}/log
    ...  ${MONGOOSE_IMAGE_NAME}:${image_version}
    ...  ${args}
    ${std_out} =  Run  ${cmd}
    [Return]  ${std_out}

Remove Mongoose Node
    ${std_out} =  Run  docker logs ${MONGOOSE_CONTAINER_NAME}
    Log  ${std_out}
    Run  docker stop ${MONGOOSE_CONTAINER_NAME}
    Run  docker rm ${MONGOOSE_CONTAINER_NAME}

Start Containers
    [Return]  0

Stop Containers
    Remove Mongoose Node

Validate Metrics Total Log File
    [Arguments]  ${step_id}  ${op_type}  ${count_succ}  ${count_fail}  ${transfer_size}
    @{metricsTotal} =  Read CSV File To Associative  ${LOG_DIR}/${step_id}/metrics.total.csv
    Should Be Equal As Strings  &{metricsTotal[0]}[OpType]  ${op_type}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountSucc]  ${count_succ}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountFail]  ${count_fail}
    Should Be Equal As Strings  &{metricsTotal[0]}[Size]  ${transfer_size}
