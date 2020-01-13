#!/bin/bash
# Source of the redis Server yaml and further documentation
# https://github.com/GoogleCloudPlatform/kubernetes-bigquery-python/tree/master/redis

# At first we create the Redis-Service on K8
kubectl create -f redis-master-service.yaml # Creates the Redis Service
kubectl create -f redis-master.yaml # Creates the Redis instace
kubectl get pods # Check if Pod is running
read -p "Please wait for Pod-Setup (~10 sec) and press [Enter] key to continue..."
kubectl port-forward deployment/redis-master 6379 & # Port forwarding from kubernetes service to (lokal) shell
# sudo apt-get install redis-tools # If you want to insert Jobs manually, you can do it with redis-tools
sudo pip3 install redis # install redis on the shell for script execution
python3 Scheduler.py # run scheduler script
pkill kubectl -9 # Kill port-forwarding
