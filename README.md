<!--TODO: Remove "previously referred to as master" references from this doc once this terminology is fully removed from k8s-->
# Cluster Autoscaler Virtual


## Introduction

This is a copy of the k8s Cluster Autoscaler v1.32.1 core code that scales-out/scales-in
virtual nodes in a virtual cluster when a workload is deployed/un-deployed.

A virtual cluster comprising of the `kube-apiserver`, `etcd` and `kube-scheduler` can be started with https://github.com/unmarshall/kvcl

All other cloud-providers excepting for the `virtual` provider have been removed

## Usage

### Pre-requisites

1. Ensure you have checked out and setup https://github.com/unmarshall/kvcl
1. Ensure you have a ClusterSnapshot.

###  Launch

Execute `start-cav.sh`

Ex: `./hack/start-cav.sh`


### Simulate Scaling 

TODO



