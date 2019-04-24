*** Settings ***
Force Tags  create_stream
Library  OperatingSystem
Library  CSVLibrary
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose-storage-driver-pravega
${MONGOOSE_CONTAINER_NAME} =  mongoose-storage-driver-pravega

${LOG_DIR} =  build/log

*** Test Cases ***
Create Events Test
    ${step_id} =  Set Variable  create_stream_test
    ${stream_name} =  Set Variable  streamtest
    Remove Directory  ${LOG_DIR}/${step_id}  recursive=True
    ${args} =  Catenate  SEPARATOR= \\\n\t
    ...  --load-step-id=${step_id}
    ...  --item-data-size=1KB
    ...  --item-output-path=${stream_name}
    ...  --load-op-limit-count=1
    ...  --storage-namespace=goose
    ...  --storage-net-node-addrs=172.17.0.4
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
    ${image_version} =  Get Environment Variable  VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${MONGOOSE_CONTAINER_NAME}
    ...  --network host
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.mongoose/${version}/log
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

Validate Metrics Log File
    [Arguments]  ${step_id}  ${stream_name}
    ${result} =  Grep File  ${LOG_DIR}/${step_id}/3rdparty.log  Stream created successfully: ${stream_name}
    Run Keyword If  "${result}"!="${EMPTY}"
    ...  Log  passed
    ...  ELSE  Fail  Stream ${stream_name} not created
