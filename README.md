# generic-serial-orchestrator

Generic Serial Orchestrator is triggered using the client-server architecture of the gRPC.
The client script will be included in solution.zip package when the composite solution will be deployed as a local solution.
The kubernetes script will also create the deployments for the orchestrator server.

To execute a pipeline, follow the following steps:
1) extract the solution.zip.
2) Run kubernetes client script to deploy all the services in your kubernetes namespace. 
   '''
     python3 kubernetes-client-script.py
   '''
3) Verify the deployment.
   '''
      kubectl -n yournamespace get all
   '''   
4) Run the orchestrator client script in your local environment.
   '''
      python3 orchestrator_client.py ip_address:nodeport
   '''   
