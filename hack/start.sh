#!/usr/bin/env zsh
set -eo pipefail

echoErr() { echo "$@" 1>&2; }

kvclPid=""


cleanup() {
  if [[ -n "$kvclPid" ]]; then
     echo "Killing Kvcl process $kvclPid"
     kill "$kvclPid" 2>/dev/null
  fi
  for p in $(pgrep -f kvcl); do kill -9 $p;done
  for p in $(pgrep -f envtest); do kill -9 $p;done

  for p in $(pgrep -f procmon); do kill -9 $p;done
}

trap cleanup EXIT

goPath=$(go env GOPATH)
if [[ -z "$goPath" ]]; then
  echoErr "GOPATH not available using go env OOPATH"
  exit 1
fi

caBin="./cluster-autoscaler"
if [[ ! -f "$caBin" ]]; then
  echoErr "Kindly build CA virtual with 'make build' before invoking start"
  exit 1
fi

#clusterSnapshotPath="$1"
#if [[ -z "$clusterSnapshotPath" ]]; then
#  echoErr "Usage: ./hack/start.sh <clusterSnapshotPath>"
#  exit 2
#fi

#if [[ ! -f "$clusterSnapshotPath" ]]; then
#  echoErr "No clusterSnapshotPath at: $clusterSnapshotPath"
#  exit 2
#fi

kvclDir="$goPath/src/github.com/unmarshall/kvcl"
launcherKvcl="$kvclDir/hack/launch.sh"
if [[ ! -f "$launcherKvcl" ]]; then
  echoErr "cannot find kvcl launcher. Kindly ensure kvcl is checked out at $kvclDir"
fi

rm /tmp/kvcl-*.log || echo "No kvcl logs found."
(
  cd "$kvclDir" && "$launcherKvcl"
) &
kvclPid=$!

sleep 7
if [[ -z  $(pgrep kvcl ) ]]; then
  echoErr "kvcl seem to have failed launched. Kindly check log."
  exit 2
fi
echo "Launched KVCL with pid: $kvclPid"


virtualKubecfg="/tmp/kvcl.yaml"
if [[ ! -f /tmp/kvcl.yaml ]]; then
  echoErr "$virtualKubecfg is not present"
  exit 3
fi

echo "Launching procmon.."
(
procmon -d /tmp -n scale-perf cluster-autoscaler kube-apiserver
) &
procmonPid=$!
sleep 10
if [[ -z  $(pgrep procmon ) ]]; then
  echoErr "procmon seem to have failed launched. Aborting"
  exit 4
fi

echo "Starting CA Virtual with VIRTUAL_AUTOSCALER_CONFIG: $VIRTUAL_AUTOSCALER_CONFIG"
"$caBin" --kubeconfig "$virtualKubecfg" \
  --leader-elect=false \
  --kube-client-qps=100 \
  --kube-client-burst=100 \
  2>&1 | tee /tmp/ca.log

