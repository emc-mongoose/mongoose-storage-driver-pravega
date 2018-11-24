*** Settings ***
Library  OperatingSystem
Test Setup  Start Containers
Test Teardown  Stop Containers

*** Variables ***
${MONGOOSE_IMAGE_NAME} =  emcmongoose/mongoose
${MONGOOSE_IMAGE_VERSION} =  latest

${PRAVEGA_IMAGE_NAME} =  pravega/pravega
${PRAVEGA_IMAGE_VERSION} =  latest
${PRAVEGA_HOST_IP} =  HOST_IP=127.0.0.1
${PRAVEGA_CONTROLLER_PORT} =  9090
${PRAVEGA_SEGMENT_STORE_PORT} =  12345
${PRAVEGA_RUNNING_MODE} =  standalone

*** Test Cases ***
Create Events Test
    Validate  ${defaults}

*** Keyword ***
Start Mongoose Node
    Run  docker run --name=mongoose -d ${MONGOOSE_IMAGE_NAME}:${MONGOOSE_IMAGE_VERSION}

Stop Mongoose Node
    Run  docker stop mongoose
    Run  docker rm mongoose

Start Pravega Standalone
    Run  docker run --name=pravega -d -e ${PRAVEGA_HOST_IP} -p ${PRAVEGA_CONTROLLER_PORT}:${PRAVEGA_CONTROLLER_PORT} -p ${PRAVEGA_SEGMENT_STORE_PORT}:${PRAVEGA_SEGMENT_STORE_PORT} ${PRAVEGA_IMAGE_NAME}:${PRAVEGA_IMAGE_VERSION} ${PRAVEGA_RUNNING_MODE}

Stop Pravega Standalone
    Run  docker stop pravega
    Run  docker rm pravega

Start Containers
    Start Mongoose Node
    Start Pravega Standalone

Stop Containers
    Stop Mongoose Node
    Stop Pravega Standalone

Validate
    [Arguments]  ${defaults}
    Run  echo ${defaults}