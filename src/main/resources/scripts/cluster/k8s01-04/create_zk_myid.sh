echo "$1" > myid.$1
ssh k8s0$1 mkdir -p /tmp/zookeeper
scp myid.$1 k8s0$1:/tmp/zookeeper/myid
