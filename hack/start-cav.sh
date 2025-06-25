#!/usr/bin/env zsh
set -eo pipefail

echoErr() { echo "$@" 1>&2; }

if [[ -z  $(pgrep kvcl ) ]]; then
  echoErr "Kindly checkout and launch https://github.com/unmarshall/kvcl"
  exit 1
fi

virtualKubecfg="/tmp/kvcl.yaml"
if [[ ! -f /tmp/kvcl.yaml ]]; then
  echoErr "$virtualKubecfg is not present"
  exit 2
fi

clusterSnapshotPath="$1"
if [[ -z "$clusterSnapshotPath" ]]; then
  echoErr "Usage: ./hack/start-cav.sh <clusterSnapshotPath>"
  exit 3
fi

if [[ ! -f "$clusterSnapshotPath" ]]; then
  echoErr "No clusterSnapshotPath at: $clusterSnapshotPath"
  exit 4
fi

export VIRTUAL_AUTOSCALER_CONFIG="/tmp/vas-config.json"
echo "Extracting VAS Config from $clusterSnapshotPath to $VIRTUAL_AUTOSCALER_CONFIG..."
jq '.AutoscalerConfig' "$clusterSnapshotPath" > "$VIRTUAL_AUTOSCALER_CONFIG"
echo "Building CA Virtual..."
go build -o cluster-autoscaler main.go
echo "Starting CA Virtual with VIRTUAL_AUTOSCALER_CONFIG: $VIRTUAL_AUTOSCALER_CONFIG"
./cluster-autoscaler --kubeconfig "$virtualKubecfg" \
  --leader-elect=false \
  --kube-client-qps=100 \
  --kube-client-burst=100 \
  2>&1 | tee /tmp/ca.log

