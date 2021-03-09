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

"""The merged protobuf for the pipeline needs to be named pipeline.proto"""
import json
from extractjson import json_extract
from queue import Queue
import grpc
import importlib
from google.protobuf import empty_pb2


class Node(object):
    """A Class for storing details about Nodes in pipeline"""
    node_name = ""
    protobuf_loc = ""
    service_name = ""
    input_msg = ""
    output_msg = ""
    port = ""


class InputNode(object):
    """A Simple Class for Input Node of the Pipeline"""
    node_name = ""
    service_name = ""


class PipelineReader(object):
    """This class is responsible for reading the dockerinfo and blueprint files generated by the AcuCompose Validator and
    getting all the useful details about ech Node in a pipeline"""
    data = {}
    port_info = {}
    total_Nodes = 0

    def __init__(self, path, dockerinfo_path):
        with open(path) as f:
            self.data = json.load(f)
        self.total_Nodes = len(self.data['nodes'])

        with open(dockerinfo_path) as f:
            self.port_info = json.load(f)

    def get_input_node(self):
        value = json_extract(self.data['input_ports'][0], "container_name")
        node_name = value[0]

        value1 = json_extract(self.data['input_ports'][0]['operation_signature'], "operation_name")
        service_name = value1[0]

        return node_name, service_name

    def get_nodes(self, i):
        values1 = json_extract(self.data['nodes'][i], "container_name")
        node_name = values1[0]

        return node_name

    def get_node_properties(self, i):
        values1 = json_extract(self.data['nodes'][i], "container_name")
        nodename = values1[0]

        values2 = json_extract(self.data['nodes'][i], "proto_uri")
        protobuf_loc = values2[0]

        values3 = json_extract(self.data['nodes'][i]['operation_signature_list'][0], "operation_name")
        service_name = values3[0]

        values4 = json_extract(self.data['nodes'][i]['operation_signature_list'][0], "input_message_name")
        input_msg = values4[0]

        values5 = json_extract(self.data['nodes'][i]['operation_signature_list'][0], "output_message_name")
        output_msg = values5[0]

        """extract node port information from dockerinfo.json"""
        value6 = json_extract(self.port_info['docker_info_list'][i], "port")
        value7 = json_extract(self.port_info['docker_info_list'][i], "ip_address")
        #port = "172.17.0.1:" + str(value6[0])
        port = str(value7[0]) + ":" + str(value6[0])

        return nodename, protobuf_loc, service_name, input_msg, output_msg, port

    def get_adjacent_nodes(self, i):
        values = json_extract(self.data['nodes'][i]['operation_signature_list'][0], "connected_to")
        node = json_extract(values, "container_name")
        return node


class Graph:
    """Class for Graph Data Structure"""

    def __init__(self, Nodes):
        self.nodes = Nodes
        self.adj_list = {}

        for node in self.nodes:
            self.adj_list[node] = []

    def print_adj_list(self):
        for node in self.nodes:
            print(node, "->", self.adj_list[node])

    def add_edge(self, u, v):
        self.adj_list[u].append(v)

    def degree(self, node):
        deg = len(self.adj_list[node])
        return deg


class GenericOrchestrator:
    """Class for executing the pipeline"""

    def bfs_traversal(self, adj_list, input_node):
        """This Function implements the BFS algorithm on a adjacency list of a graph
        This is intended to support branches for parallel paths in a pipeline

        Input: adjacency list, starting Node of the graph
        Output: returns a BFS Traversal path
        """
        visited = {}
        level = {}  # distance dictionary
        parent = {}
        bfs_traversal_output = []
        queue = Queue()

        for node in adj_list.keys():
            visited[node] = False
            parent[node] = None
            level[node] = -1

        print(visited)

        source = input_node
        visited[source] = True
        level[source] = 0
        parent[source] = None

        queue.put(source)

        while not queue.empty():
            u = queue.get()
            bfs_traversal_output.append(u)

            for v in adj_list[u]:
                if not visited[v]:
                    visited[v] = True
                    parent[v] = u
                    level[v] = level[u] + 1
                    queue.put(v)

        return bfs_traversal_output

    def find_node_in_pipeline(self, node, pipeline, p):
        """Function to find the index of the node the pipeline given the node name
        :argument:
            node(str): Nodename
            pipeline(List): pipeline instance which contains list of node properties
        :returns
            index_to_return(int): Index of the node in list pipeline
        """
        for i in range(p.total_Nodes):

            if node == pipeline[i].node_name:
                index_to_return = i
                break

        return index_to_return

    def get_all_stubs(self, path):
        """Function to get all the stubs from the combined protofile
        :arg
            path(str): path of pipeline_pb2_grpc.py
        :returns
            stubs(List): List containing the stubs
        """

        stubs = []
        """store all the stubs in a list"""
        for line in open(path, "r"):
            if "Stub" in line:
                split1 = line.split()
                split2 = split1[1].split('(')
                stub_str = split2[0]
                stubs.append(stub_str)

        return stubs

    def start_node(self, i, stubs, pipe_line):
        """A generic Function to get port,stub,request and response for each node
        Input: Index of the node
        Output: Port address, stub method, request method, response method"""

        pb2 = importlib.import_module("work_dir.pipeline_pb2")
        pb2_grpc = importlib.import_module("work_dir.pipeline_pb2_grpc")

        port = pipe_line[i].port
        channel = grpc.insecure_channel(port)
        # create a stub (client)
        stub_method = getattr(pb2_grpc, stubs[i])
        stub = stub_method(channel)

        if i == 0:
            request_method = empty_pb2.Empty
        else:
            request_method = getattr(pb2, pipe_line[i].input_msg)

        response_before_call = getattr(pb2, pipe_line[i].output_msg)

        response_method = getattr(stub, pipe_line[i].service_name)

        return request_method, response_method, response_before_call

    def link_nodes(self, bfs_list, pipe_line, p, stubs, num_nodes, current_node=0, previous_response=None):
        """A Recursive function to link nodes in a pipeline
        :arg
            bfs_list(List): List containing BFS traversal for the graph created
            pipe_line(List): List of Node objects
            p(object): Instance of class PipelineReader
            stubs(List): List of stubs returned by get_all_stubs
            current_node(int): Index of current node
            previous_response: response of the previous node
        """

        i = self.find_node_in_pipeline(bfs_list[current_node], pipe_line, p)
        request_method, response_method, response_before_call = self.start_node(i, stubs, pipe_line)

        if previous_response is None:
            """This is for the first Node i.e Databroker in the pipeline which according to ai4eu container 
            specification has empty input type"""
            request_1 = request_method()
            response_1 = response_method(request_1)
            response_before = response_before_call()
        else:
            response_before = response_before_call()
            response_1 = response_method(previous_response)

        print("*********************************************")
        print("Response of node", current_node)
        print(response_before)
        print(response_1)

        """Our termination case out of recursive function is when response before grpc call is equal to 
         response after grpc call!!!"""
        if response_before != response_1:
            """Increment the count of current node and call the function recursively until all nodes are exhausted"""
            current_node = current_node + 1
            if current_node == num_nodes:
                """continue message dispatching through the pipeline until data broker(Input Node) is exhausted"""
                current_node = 0
            self.link_nodes(bfs_list, pipe_line, p, stubs, num_nodes, current_node=current_node,
                            previous_response=response_1)

    def execute_pipeline(self, blueprint, dockerinfo):
        """Start Pipeline Excecution
        :arg
            blueprint(string): Path of blueprint.json
            dockerinfo(string): Path of dockerinfo.json
        """

        stubs = self.get_all_stubs("work_dir/pipeline_pb2_grpc.py")

        """create pipeline reader instance"""
        p = PipelineReader(blueprint, dockerinfo)

        """create a list of node instances"""
        pipe_line = [Node() for i in range(p.total_Nodes)]

        """Empty List for Nodes and Adjacent nodes"""
        nodes = []
        adj_nodes = []

        print("Total number of nodes in pipeline", p.total_Nodes)

        input_node = InputNode()
        input_node.node_name, input_node.service_name = p.get_input_node()

        for i in range(p.total_Nodes):
            """get all nodes and their adjacent nodes in the pipeline"""
            node_name = p.get_nodes(i)
            nodes.append(node_name)

            adj_node = p.get_adjacent_nodes(i)
            adj_nodes.append(adj_node)

        """create a instance of class Graph"""
        graph = Graph(nodes)

        print(nodes)
        print(adj_nodes)
        print("\n")
        print("adjacency list before adding edges")
        graph.print_adj_list()

        """Add edges to the graph"""
        for i in range(p.total_Nodes):
            adj_list = adj_nodes[i]
            if len(adj_list) > 0:
                for j in range(len(adj_list)):
                    graph.add_edge(nodes[i], adj_list[j])

        print("\n")
        print("adjacency list after edding edges")
        graph.print_adj_list()

        """get all node properties"""
        for i in range(p.total_Nodes):
            pipe_line[i].node_name, pipe_line[i].protobuf_loc, \
            pipe_line[i].service_name, pipe_line[i].input_msg, pipe_line[i].output_msg, pipe_line[i].port \
                = p.get_node_properties(i)

        print("\n")

        """Graph Traversal using BFS Algorithm"""
        bfs_list = self.bfs_traversal(graph.adj_list, input_node.node_name)

        print("bfs output:", bfs_list)
        num_nodes = p.total_Nodes

        """Using Recursion to link nodes in a pipeline"""
        self.link_nodes(bfs_list, pipe_line, p, stubs, num_nodes, current_node=0, previous_response=None)
