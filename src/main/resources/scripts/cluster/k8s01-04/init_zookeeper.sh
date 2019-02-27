mkdir -p zookeeper_myid_files
cd zookeeper_myid_files
for i in {1..4} ; do ../create_zk_myid.sh $i; done
