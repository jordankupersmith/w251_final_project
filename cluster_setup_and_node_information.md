# Full Cluster Setup (Spark + Cassandra on 4 nodes)

## Provision 4 VM's

slcli vs create --datacenter=sjc01 --hostname=wiki1 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64
slcli vs create --datacenter=sjc01 --hostname=wiki2 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64
slcli vs create --datacenter=sjc01 --hostname=wiki3 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64
slcli vs create --datacenter=sjc01 --hostname=wiki4 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64


## Node Information

37292553  wiki1          198.23.108.53    10.90.61.249  sjc01 YDv86lrb
37288917  wiki2          198.23.108.52    10.90.61.241   sjc01  NAT2exYb
37289001  wiki3          198.23.108.54    10.90.61.242   sjc01  BW2ADnul
37289031  wiki4         198.23.108.51    10.90.61.243   sjc01  ZymD36km


### Update /etc/hosts on each node
/etc/hosts
127.0.0.1 localhost.localdomain localhost10.90.61.249 wiki1.mnelson.ca wiki110.90.61.241 wiki2.mnelson.ca wiki2
10.90.61.242 wiki3.mnelson.ca wiki3
10.90.61.243 wiki4.mnelson.ca wiki4

### SSH Setup
Enable SSH between Nodes Private IPs
Create a keypair on master and copy it to the other systems (when prompted by ssh-keygen, use defaults):
ssh-keygen -f ~/.ssh/id_rsa -b 2048 -t rsa -C 'w251 Final Project Key'

From Master (where SSH was setup)
scp /root/.ssh/id_rsa root@wiki2:/root/.ssh/id_rsa
scp /root/.ssh/id_rsa.pub root@wiki2:/root/.ssh/id_rsa.pub
scp /root/.ssh/id_rsa root@wiki3:/root/.ssh/id_rsa
scp /root/.ssh/id_rsa.pub root@wiki3:/root/.ssh/id_rsa.pub
scp /root/.ssh/id_rsa root@wiki4:/root/.ssh/id_rsa
scp /root/.ssh/id_rsa.pub root@wiki4:/root/.ssh/id_rsa.pub

Still on the master, accept all keys by SSHing to each box and typing "yes" and, once you're logged into the remote box, typing CTRL-d:
for i in 0.0.0.0 wiki1 wiki2 wiki3 wiki4; do ssh $i; done

### Identify 1000GB Disks and mount to /data
fdisk -l

Assuming the disk is called /dev/xvdc 
mkdir /data
mkfs.ext4 /dev/xvdc

Add this line to /etc/fstab (with the appropriate disk path):  
/dev/xvdc /data                   ext4    defaults,noatime        0 0

Mount your disk and set the appropriate perms
mount /data
chmod 1777 /data


## Install Spark 1.6

Install Java, SBT, and Spark on all nodes
Install packages:
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
yum install -y java-1.8.0-openjdk-headless sbt

Set the proper location of JAVA_HOME and test it:
echo export JAVA_HOME=\"$(readlink -f $(which java) | grep -oP '.*(?=/bin)')\" >> /root/.bash_profile
source /root/.bash_profile
$JAVA_HOME/bin/java -version

Download and extract a recent, prebuilt version of Spark:
curl http://www.gtlib.gatech.edu/pub/apache/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.6.tgz | tar -zx -C /usr/local --show-transformed --transform='s,/*[^/]*,spark,'

Or try this download location (working)
curl https://archive.apache.org/dist/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.6.tgz | tar -zx -C /usr/local --show-transformed --transform='s,/*[^/]*,spark,'

For convenience, set $SPARK_HOME:
echo export SPARK_HOME=\"/usr/local/spark\" >> /root/.bash_profile
source /root/.bash_profile

### Configure Spark
On wiki1, create the new file $SPARK_HOME/conf/slaves with content:
wiki1
wiki2
wiki3
wiki4

View Running Spark Cluster in Browser
http://198.23.108.53:8080/

Start Spark from master
Once you’ve set up the conf/slaves file, you can launch or stop your cluster with the following shell scripts, based on Hadoop’s deploy scripts, and available in $SPARK_HOME/:
	• sbin/start-master.sh - Starts a master instance on the machine the script is executed on
	• sbin/start-slaves.sh - Starts a slave instance on each machine specified in the conf/slaves file
	• sbin/start-all.sh - Starts both a master and a number of slaves as described above
	• sbin/stop-master.sh - Stops the master that was started via the bin/start-master.sh script
	• sbin/stop-slaves.sh - Stops all slave instances on the machines specified in the conf/slaves file
	• sbin/stop-all.sh - Stops both the master and the slaves as described above

Start the master first, then open browser and see http://<master_ip>:8080/:
$SPARK_HOME/sbin/start-master.sh

Then, run the start-slaves script, refresh the window and see the new workers (note that you can execute this from the master).
$SPARK_HOME/sbin/start-slaves.sh


## Install CASSANDRA 2.2
https://www.ca.com/us/services-support/ca-support/ca-support-online/knowledge-base-articles.TEC1714429.html

### Installing Cassandra on single nodes
Download and extract the package:
cd /tmp
wget http://mirror.cc.columbia.edu/pub/software/apache/cassandra/2.2.10/apache-cassandra-2.2.10-bin.tar.gz
tar -zxf apache-cassandra-2.2.10-bin.tar.gz

Move it to a proper folder:
sudo mv apache-cassandra-2.2.10/ /opt/

Next, make sure that the folders Cassandra accesses, such as the log folder, exists and that Cassandra has the right to write on it:
sudo mkdir /var/lib/cassandra
sudo mkdir /var/log/cassandra
sudo mkdir /data/cassandra
sudo chown -R $USER:$GROUP /var/lib/cassandra
sudo chown -R $USER:$GROUP /var/log/cassandra
sudo chown -R $USER:$GROUP /data/cassandra

To setup Cassandra environment variables, add the following lines to /etc/profile.d/cassandra.sh using vi or cat:
export CASSANDRA_HOME=/opt/apache-cassandra-2.2.10
export PATH=$PATH:$CASSANDRA_HOME/bin

You should now reboot the node, so everything is updated:
sudo reboot
 
Log back in and and confirm that everything is set properly:
sudo sh $CASSANDRA_HOME/bin/cassandra # Starts Cassandra
sudo sh $CASSANDRA_HOME/bin/cqlsh #Starts CQL shell

### Setting up Cassandra cluster
Before configuring each node, make sure Cassandra is not running:
$pkill cassandra
 
You'll also need to clear data:
$sudo rm -rf /var/lib/cassandra/*
 
4 node, single data center, single seed Cassandra cluster:
wiki1
wiki2
wiki3
wiki4

Configuration on nodes is done through customizing cassandra.yaml file **main config file**:
$vi $CASSANDRA_HOME/conf/cassandra.yaml

https://wiki.apache.org/cassandra/MultinodeCluster10

#### Adjusted Parameters in cassandra.yaml

wiki1
cluster_name: 'WikiSpark Cluster'
seed_provider:
	- class_name: org.apache.cassandra.locator.SimpleSeedProvider parameters:
		- seeds: 10.90.61.249
concurrent_reads: 32
concurrent_writes: 32
concurrent_counter_writes: 32
listen_address: 10.90.61.249
rpc_address: 0.0.0.0
rpc_port: 9160
broadcast_rpc_address: localhost
data_file_directories: /data/cassandra

Copied cassandra.yaml from wiki1 to the others then changed listen_address
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.241:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.242:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.243:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working

Delete & rename yaml file on each of the other 3 nodes
rm $CASSANDRA_HOME/conf/cassandra.yaml
mv $CASSANDRA_HOME/conf/cassandra.yaml.working $CASSANDRA_HOME/conf/cassandra.yaml

Change on other nodes:
wiki2
listen_address: 10.90.61.241 

wiki3
listen_address: 10.90.61.242

wiki4
listen_address: 10.90.61.243


Once you have adjusted cassandra.yaml on all the nodes, start cassandra on nodes, doing it on the seed node first:
$sudo sh $CASSANDRA_HOME/bin/cassandra

Check Status of Cassandra Cluster
 netstat -ant | grep 7000 (Make sure it is not still looking for 127.0.0.1:7000

Get a description of the cluster:
$CASSANDRA_HOME/bin/nodetool describecluster
 
Confirm that all the nodes are up:
$CASSANDRA_HOME/bin/nodetool status

To Kill Cassandra
pkill cassandra


## Run Spark - Cassandra Connector
- we could try to install this, but the connector github suggested to use a spark package which I've done below

Assume we are using
Spark 1.5 or 1.6
Cassandra 2.2
Scala 2.10

https://github.com/datastax/spark-cassandra-connector
https://spark-packages.org/package/datastax/spark-cassandra-connector

Run on Main Cassandra / Spark Node
$SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.2-s_2.10


