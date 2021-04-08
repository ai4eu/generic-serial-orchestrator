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

"""This module starts the generic serial orchestrator
and it also expects a directory named work_dir where all the files related to a pipeline will be stored and
deleted for the next run!"""
import os
import generic_serial_orchestrator as orch
import mergeproto as mp
import zipfile
import glob
import sys
import traceback

curr_dir = os.getcwd()
work_dir_path = os.path.join(curr_dir,"work_dir")

def string_to_blueprint_json(text):
    """Converts text to json file"""
    path = os.path.join(work_dir_path,"blueprint_gen.json")
    text_file = open(path, "w")
    text_file.write(text)
    text_file.close()


def string_to_dockerinfo_json(text):
    """converts text to json file"""
    path = os.path.join(work_dir_path,"dockerinfo_gen.json")
    text_file = open(path, "w")
    text_file.write(text)
    text_file.close()


def bytes_to_zip(bytestream):
    """Converts bytes to file"""
    path = os.path.join(work_dir_path,"pipeline_proto.zip")
    f = open(path, "wb")
    f.write(bytestream)
    f.close()


def extract_zip(file):
    """extracts the zip file to a file directory"""
    path = os.path.join(work_dir_path,file)
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall("work_dir")


def get_proto_files():
    """get all the proto files inside the extracted directory"""
    proto_files = []
    work_dir = "work_dir"
    lst = os.listdir(work_dir)
    for file in lst:
        if file.endswith(".proto"):
            proto_files.append(file)
    return proto_files


def delete_files():
    path = os.path.join(curr_dir + "/work_dir/*")
    files = glob.glob(path)
    for f in files:
        if os.path.isdir(f):
            continue
            print("It is a folder")
        else:
            os.remove(f)
            print("file deleted", f)


def executePipeline(blueprint, dockerinfo, protoszip):
    try:
        delete_files()
        string_to_blueprint_json(blueprint)
        string_to_dockerinfo_json(dockerinfo)
        bytes_to_zip(protoszip)
        extract_zip("pipeline_proto.zip")
        proto_files = get_proto_files()

        """Merge the protobuf files to single consolidated protobuf file"""
        merge_cls = mp.ProtoMerger(proto_files)
        messages, services = merge_cls.prepare_dict()
        proto_path = os.path.join("work_dir" + "/pipeline.proto")
        merge_cls.prepare_map_service_rpc()
        merge_cls.write_to_merged_proto(messages, services, proto_path)

        """generate stubs and skeleton"""
        pc = mp.ProtoComplier()
        pc.generate_pb2_pb2c(proto_path)

        rpc_service_map = merge_cls.rpc_service_map

        """Call the orchestrator"""
        orchestrator = orch.GenericOrchestrator()
        dockerinfo_path = "work_dir/dockerinfo_gen.json"
        blueprint_path = "work_dir/blueprint_gen.json"
        orchestrator.execute_pipeline(blueprint_path, dockerinfo_path, rpc_service_map)
        status = 200
        status_msg = "Orchestrator successfully started"

    except Exception as e:
        status = 400
        status_msg = e
        print('executePipeline status:', status_msg, traceback.format_exc())
        sys.stdout.flush()

    return status, status_msg

