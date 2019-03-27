*** Settings ***
Force Tags  create_stream
Library  OperatingSystem
Library  CSVLibrary
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose-storage-driver-pravega
${MONGOOSE_IMAGE_VERSION} =  testing
${MONGOOSE_CONTAINER_NAME} =  mongoose-storage-driver-pravega

${PRAVEGA_IMAGE_NAME} =  pravega/pravega
${PRAVEGA_IMAGE_VERSION} =  0.3.2
${PRAVEGA_CONTAINER_NAME} =  pravega_standalone
${PRAVEGA_HOST_IP} =  HOST_IP=127.0.0.1
${PRAVEGA_CONTROLLER_PORT} =  9090
${PRAVEGA_SEGMENT_STORE_PORT} =  12345
${PRAVEGA_RUNNING_MODE} =  standalone

${LOG_DIR} =  build/log

*** Test Cases ***
Create Events Test
    ${step_id} =  Set Variable  create_stream_test
    ${stream_name} =  Set Variable  streamtest
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --item-output-path=${stream_name}
    ...  --load-op-limit-count=1
    ...  --storage-driver-limit-concurrency=1
    ${std_out} =  Execute Mongoose Scenario  ${args}
    Log  ${std_out}
    Validate Metrics Log File  ${step_id}  ${stream_name}

*** Keyword ***
Execute Mongoose Scenario
    [Timeout]  5 minutes
    [Arguments]  ${args}
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  ${host_working_dir}
    ${version} =  Get Environment Variable  BASE_VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name=${MONGOOSE_CONTAINER_NAME}
    ...  --network host
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

Validate Metrics Log File
    [Arguments]  ${step_id}  ${stream_name}
    ${result} =  Grep File  ${LOG_DIR}/${step_id}/3rdparty.log  Stream created successfully: ${stream_name}
    Run Keyword If  "${result}"!="${EMPTY}"
    ...  Log  passed
    ...  ELSE  Fail  Stream ${stream_name} not created
