*** Settings ***
Force Tags  create_and_read_events
Library  OperatingSystem
Library  CSVLibrary
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose-storage-driver-pravega
${MONGOOSE_IMAGE_VERSION} =  testing
${MONGOOSE_CONTAINER_NAME} =  mongoose-storage-driver-pravega
${MONGOOSE_CONTAINER_DATA_DIR} =  /data

${PRAVEGA_IMAGE_NAME} =  pravega/pravega
${PRAVEGA_IMAGE_VERSION} =  latest
${PRAVEGA_CONTAINER_NAME} =  pravega_standalone
${PRAVEGA_HOST_IP} =  HOST_IP=127.0.0.1
${PRAVEGA_CONTROLLER_PORT} =  9090
${PRAVEGA_SEGMENT_STORE_PORT} =  12345
${PRAVEGA_RUNNING_MODE} =  standalone

${LOG_DIR} =  build/log
${DATA_DIR} =  src/test/robot/api/storage/data
*** Test Cases ***
Create and read Events Test
    ${step_id} =  Set Variable  create_and_read_events_test
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --storage-net-node-port=${PRAVEGA_CONTROLLER_PORT}
    ...  --load-op-recycle
    ...  --run-scenario=${MONGOOSE_CONTAINER_DATA_DIR}/${step_id}.js
    ${std_out} =  Execute Mongoose Scenario  ${args}
    Log  ${std_out}
    Validate Metrics Total Log File  ${step_id}  READ  5  0  5242880

*** Keyword ***
Execute Mongoose Scenario
    [Timeout]  5 minutes
    [Arguments]  ${args}
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  ${host_working_dir}
    ${version} =  Get Environment Variable  MONGOOSE_VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name=${MONGOOSE_CONTAINER_NAME}
    ...  --network host
    ...  --volume ${host_working_dir}/${DATA_DIR}:${MONGOOSE_CONTAINER_DATA_DIR}
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.mongoose/${version}/log
    ...  ${MONGOOSE_IMAGE_NAME}:${MONGOOSE_IMAGE_VERSION}
    ...  ${args}
    ${std_out} =  Run  ${cmd}
    [Return]  ${std_out}

Remove Mongoose Node
    Run  docker stop ${MONGOOSE_CONTAINER_NAME}
    Run  docker rm ${MONGOOSE_CONTAINER_NAME}

Start Pravega Standalone
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  -d
    ...  --name=${PRAVEGA_CONTAINER_NAME}
    ...  -e ${PRAVEGA_HOST_IP}
    ...  -p ${PRAVEGA_CONTROLLER_PORT}:${PRAVEGA_CONTROLLER_PORT}
    ...  -p ${PRAVEGA_SEGMENT_STORE_PORT}:${PRAVEGA_SEGMENT_STORE_PORT}
    ...  ${PRAVEGA_IMAGE_NAME}:${PRAVEGA_IMAGE_VERSION}
    ...  ${PRAVEGA_RUNNING_MODE}
    ${std_out} =  Run  ${cmd}
    Log  ${std_out}

Remove Pravega Standalone
    Run  docker stop ${PRAVEGA_CONTAINER_NAME}
    Run  docker rm ${PRAVEGA_CONTAINER_NAME}

Start Containers
    Start Pravega Standalone

Stop Containers
    Remove Mongoose Node
    Remove Pravega Standalone

Validate Metrics Total Log File
    [Arguments]  ${step_id}  ${op_type}  ${count_succ}  ${count_fail}  ${transfer_size}
    @{metricsTotal} =  Read CSV File To Associative  ${LOG_DIR}/${step_id}/metrics.total.csv
    Should Be Equal As Strings  &{metricsTotal[0]}[OpType]  ${op_type}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountSucc]  ${count_succ}
    Should Be Equal As Strings  &{metricsTotal[0]}[CountFail]  ${count_fail}
    Should Be Equal As Strings  &{metricsTotal[0]}[Size]  ${transfer_size}
