#!/usr/bin/env python3
# ===================================================================================
# Copyright (C) 2021 Fraunhofer Gesellschaft. All rights reserved.
# ===================================================================================
# This Acumos software file is distributed by Fraunhofer Gesellschaft
# under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# This file is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============LICENSE_END========================================================

import grpc
from concurrent import futures
import time

# import the generated classes :
import orchestrator_pb2
import orchestrator_pb2_grpc

# import the function we made :
import start_orchestrator as so

port = 8061


# create a class to define the server functions, derived from
class start_orchestrator(orchestrator_pb2_grpc.start_orchestratorServicer):
    def executePipeline(self, request, context):
        # define the buffer of the response :
        response = orchestrator_pb2.PipelineStatus()
        # get the value of the response by calling the desired function :
        response.statusCode, response.statusText = so.executePipeline(request.blueprint, request.dockerinfo,
                                                                      request.protoszip)
        return response


# create a grpc server :
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

orchestrator_pb2_grpc.add_start_orchestratorServicer_to_server(start_orchestrator(), server)

print("Starting server. Listening on port : " + str(port))
server.add_insecure_port("[::]:{}".format(port))
server.start()

try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
