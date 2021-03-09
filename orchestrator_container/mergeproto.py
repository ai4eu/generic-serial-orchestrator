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

import subprocess
import os


class ProtoMerger:
    """Merge proto files. There is also a check for duplicate messages"""
    messages = {}
    services = {}
    proto_files = []

    def __init__(self, files):
        self.proto_files = files

    def check_duplicate_in_dict(self, dict):
        """

        :param dict: Dictionary
        :return: True if duplicates exist else False
        """
        rev_multidict = {}
        for key, value in dict.items():
            rev_multidict.setdefault(value, set()).add(key)
        duplicate = [key for key, values in rev_multidict.items() if len(values) > 1]
        if len(duplicate) > 0:
            return True

    def prepare_dict(self):
        """

        :return:
            messages: dictionary of messages in list of proto files
            services: dictionary of services in list of proto files
        """

        for file in self.proto_files:
            msg_value = []
            ser_value = []
            flag_message = False
            flag_service = False

            file_path = os.path.join("work_dir/" + file)

            for line in open(file_path, "r"):

                if flag_message:
                    msg_value.append(line)
                    if line == '}\n' or line == '}' or line == '}\t\n':
                        all_values = ''.join(map(str, msg_value))
                        self.messages[key] = all_values
                        flag_message = False
                        msg_value.clear()

                if flag_service:
                    ser_value.append(line)
                    if line == '}\n' or line == '}' or line == '}\t\n':
                        all_values = ''.join(map(str, ser_value))
                        self.services[key] = all_values
                        flag_message = False
                        ser_value.clear()

                if "message" in line:
                    flag_service = False
                    key = line
                    flag_message = True

                if "service" in line:
                    flag_message = False
                    key = line
                    flag_service = True

        return self.messages, self.services

    def write_to_merged_proto(self, messages, services, file):
        """

        :param messages: dictionary of messages
        :param services: dictionary of services
        :param file: name of output file which in our case will always we pipeline.proto as orchestrator expected it
                    that way.
        :return: No return
        """
        #file_path = os.path.join("work_dir/" + file)
        outfile = open(file, "a")
        init_line = 'syntax = ' + '"' + 'proto3' + '"' + ';' + '\n'
        data_broker_line = 'import' + '\t' + '"' + 'google/protobuf/empty.proto' + '"' + ';' + '\n' + '\n'
        outfile.write(init_line)
        outfile.write(data_broker_line)

        dup = self.check_duplicate_in_dict(messages)
        if dup:
            print("Input Protobufs are inconsistent")
        else:
            for key, value in messages.items():
                key_value = key + "\n" + value
                outfile.write(key_value)
                outfile.write("\n")

        dup = self.check_duplicate_in_dict(services)

        if dup:
            print("Input Protobufs are inconsistent")
        else:
            for key, value in services.items():
                key_value = key + "\n" + value
                outfile.write(key_value)
                outfile.write("\n")
        print("Proto Files successfully merged...!")
        outfile.close()


class ProtoComplier:
    """This class generates the stubs and skeletons for the combined protofile"""

    def generate_pb2_pb2c(self, protofile):
        """ genrate pb2 and pb2_grpc files for combined pipeline.proto
        :arg
                protofile(str): Path of combined proto file
        """

        subprocess.call(
            ['python3', '-m', 'grpc_tools.protoc', '-I.', '--python_out=.', '--grpc_python_out=.', protofile])


