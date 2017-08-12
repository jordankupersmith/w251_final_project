# Full Cluster Setup (Spark + Cassandra on 4 nodes)

## Provision 4 VM's

slcli vs create --datacenter=sjc01 --hostname=wiki1 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64<br>
slcli vs create --datacenter=sjc01 --hostname=wiki2 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64<br>
slcli vs create --datacenter=sjc01 --hostname=wiki3 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64<br>
slcli vs create --datacenter=sjc01 --hostname=wiki4 --domain=mnelson.ca --billing=hourly --key=masterkey --cpu=2 --memory=8192 --disk=25  --disk=1000 --san  --network=1000 --os=CENTOS_7_64<br>


## Node Information

37292553  wiki1          198.23.108.53    10.90.61.249  sjc01 YDv86lrb <br>
37288917  wiki2          198.23.108.52    10.90.61.241   sjc01  NAT2exYb <br>
37289001  wiki3          198.23.108.54    10.90.61.242   sjc01  BW2ADnul <br>
37289031  wiki4         198.23.108.51    10.90.61.243   sjc01  ZymD36km <br>


### Update /etc/hosts on each node
/etc/hosts <br>
127.0.0.1 localhost.localdomain localhost <br>
10.90.61.249 wiki1.mnelson.ca wiki1 <br>
10.90.61.241 wiki2.mnelson.ca wiki2 <br>
10.90.61.242 wiki3.mnelson.ca wiki3 <br>
10.90.61.243 wiki4.mnelson.ca wiki4 <br>

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
fdisk -l<br>

Assuming the disk is called /dev/xvdc <br>
mkdir /data<br>
mkfs.ext4 /dev/xvdc<br>

Add this line to /etc/fstab (with the appropriate disk path):<br>
  
/dev/xvdc /data                   ext4    defaults,noatime        0 0<br>

Mount your disk and set the appropriate perms<br>
mount /data<br>
chmod 1777 /data<br>


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
Download and extract the package: <br>
cd /tmp <br>
wget http://mirror.cc.columbia.edu/pub/software/apache/cassandra/2.2.10/apache-cassandra-2.2.10-bin.tar.gz <br>
tar -zxf apache-cassandra-2.2.10-bin.tar.gz <br>

Move it to a proper folder: <br>
sudo mv apache-cassandra-2.2.10/ /opt/ <br>

Next, make sure that the folders Cassandra accesses, such as the log folder, exists and that Cassandra has the right to write on it: <br>
sudo mkdir /var/lib/cassandra <br>
sudo mkdir /var/log/cassandra <br>
sudo mkdir /data/cassandra <br>
sudo chown -R $USER:$GROUP /var/lib/cassandra <br>
sudo chown -R $USER:$GROUP /var/log/cassandra <br>
sudo chown -R $USER:$GROUP /data/cassandra <br>

To setup Cassandra environment variables, add the following lines to /etc/profile.d/cassandra.sh using vi or cat: <br>
export CASSANDRA_HOME=/opt/apache-cassandra-2.2.10 <br>
export PATH=$PATH:$CASSANDRA_HOME/bin <br>

You should now reboot the node, so everything is updated: <br>
sudo reboot <br>
 
Log back in and and confirm that everything is set properly: <br>
sudo sh $CASSANDRA_HOME/bin/cassandra # Starts Cassandra <br>
sudo sh $CASSANDRA_HOME/bin/cqlsh #Starts CQL shell <br>

### Setting up Cassandra cluster <br>
Before configuring each node, make sure Cassandra is not running: <br>
$pkill cassandra <br>
 
You'll also need to clear data: <br>
$sudo rm -rf /var/lib/cassandra/* <br>
 
4 node, single data center, single seed Cassandra cluster: <br>
wiki1 <br>
wiki2 <br>
wiki3 <br>
wiki4 <br>

Configuration on nodes is done through customizing cassandra.yaml file **main config file**: <br>
$vi $CASSANDRA_HOME/conf/cassandra.yaml <br>

https://wiki.apache.org/cassandra/MultinodeCluster10 <br>

#### Adjusted Parameters in cassandra.yaml <br>

wiki1 <br>
cluster_name: 'WikiSpark Cluster' <br>
seed_provider: <br>
	- class_name: org.apache.cassandra.locator.SimpleSeedProvider parameters: <br>
		- seeds: 10.90.61.249 <br>
concurrent_reads: 32 <br>
concurrent_writes: 32<br>
concurrent_counter_writes: 32<br>
listen_address: 10.90.61.249<br>
rpc_address: 0.0.0.0<br>
rpc_port: 9160<br>
broadcast_rpc_address: localhost <br>
data_file_directories: /data/cassandra <br>

Copied cassandra.yaml from wiki1 to the others then changed listen_address <br>
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.241:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working <br>
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.242:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working <br>
scp /opt/apache-cassandra-2.2.10/conf/cassandra.yaml root@10.90.61.243:/opt/apache-cassandra-2.2.10/conf/cassandra.yaml.working <br>

Delete & rename yaml file on each of the other 3 nodes<br>
rm $CASSANDRA_HOME/conf/cassandra.yaml<br>
mv $CASSANDRA_HOME/conf/cassandra.yaml.working $CASSANDRA_HOME/conf/cassandra.yaml<br>

Change on other nodes:<br>
wiki2<br>
listen_address: 10.90.61.241 <br>

wiki3<br>
listen_address: 10.90.61.242<br>

wiki4<br>
listen_address: 10.90.61.243<br>


Once you have adjusted cassandra.yaml on all the nodes, start cassandra on nodes, doing it on the seed node first:<br>
$sudo sh $CASSANDRA_HOME/bin/cassandra<br>

Check Status of Cassandra Cluster<br>
 netstat -ant | grep 7000 (Make sure it is not still looking for 127.0.0.1:7000<br>

Get a description of the cluster:<br>
$CASSANDRA_HOME/bin/nodetool describecluster<br>
 
Confirm that all the nodes are up:<br>
$CASSANDRA_HOME/bin/nodetool status<br>

To Kill Cassandra<br>
pkill cassandra<br>


## Run Spark - Cassandra Connector
- we could try to install this, but the connector github suggested to use a spark package which I've done below<br>

Assume we are using<br>
Spark 1.5 or 1.6<br>
Cassandra 2.2<br>
Scala 2.10<br>

https://github.com/datastax/spark-cassandra-connector<br>
https://spark-packages.org/package/datastax/spark-cassandra-connector<br>

Run on Main Cassandra / Spark Node<br>
$SPARK_HOME/bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.2-s_2.10<br>

## get into cassandra command prompt and test replication

$CASSANDRA_HOME/bin/cqlsh
Set the replication factor:<br>
>CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '4' };<br>

Create a test table:<br>
 >CREATE TABLE planet( catalog int PRIMARY KEY, name text, mass double, density float, albedo float ); <br>

Insert data into the table:<br>

 INSERT INTO planet (catalog, name, mass, density, albedo) VALUES ( 3, 'Earth', 5.9722E24, 5.513, 0.367);
 
Confirm that replication works by running the following on **each**  node: <br>

$CASSANDRA_HOME/bin/cqlsh -e "SELECT * FROM test.planet;"<br>

The test ran succesfully on all nodes.<br>

# Download Page Views 

https://dumps.wikimedia.org/other/pageviews/ is rate limited to a download speed of 1.5 mb/s. For the amount of data we are attempting to process, this is an unacceptably slow rate. Therefore, we are comissioning another VS on a different group member's account to mirror the wikimedia site. We will then use the public ip to SCP download the files to our cluster at a much faster download rate. <br>

The "storage" cluster is:<br>
slcli vs create --datacenter=sjc01 --hostname=wikistorage --domain=mnelson.ca --billing=hourly --key=softlayer  --cpu=2 --memory=4096 --disk=25 --disk=2000 --san --network=1000 --os=CENTOS_7_64<br>

37521507 :  wikistorage  :   50.23.97.102  :  10.54.225.3   :   sjc01    :   PRyyk43v : jordan <br>
37552841 :  wikistorage  :  169.53.147.154  :  10.122.178.252  :  sjc01  :  RKvyjnx3  :  Dave <br>
37578599 :  wikistorage  :  50.23.91.12   :  10.54.14.17  :   sjc01  :  FA9SJ75p  :  Utthaman

We mount the 2 TB HD as instructed above.<br>

The download script is download.py and it is run in the background with:<br>
	nohup python -u download.py > download_log &

It is estimated to take 7 days to download all pageview data back to May 1, 2015. 
	
	

