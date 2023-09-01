# DEPART: Replica Decoupling for Distributed Key-Value Storage

## 1. Introduction
  Modern distributed key-value (KV) stores adopt replication for fault tolerance by distributing replicas of KV pairs across nodes. However, existing distributed KV stores often manage all replicas in the same index structure, thereby leading to significant I/O costs beyond the replication redundancy. We propose a notion called replica decoupling, which decouples the storage management of the primary and redundant copies of replicas, so as to not only mitigate the I/O costs in indexing, but also provide tunable performance. In particular, we design a novel two-layer log that enables tunable ordering for the redundant copies to achieve balanced read/write performance. We implement a distributed KV store prototype, DEPART, atop Cassandra. Experiments show that DEPART outperforms Cassandra in all performance aspects under various consistency levels and parameter settings. Specifically, under the eventual consistency setting, DEPART achieves up to 1.43x, 2.43x, 2.68x, and 1.44x throughput for writes, reads, scans, and updates, respectively.

## Publications
* Qiang Zhang, Yongkun Li, Patrick P. C. Lee, Yinlong Xu, Si Wu. "**DEPART: Replica Decoupling for Distributed Key-Value Storage**". Proceedings of the 20th USENIX Conference on File and Storage Technologies (FAST 2022), February 2022.


## 2. Overview
* The prototype is written in Java based on [Cassandra-3.11.4](https://github.com/zhangqiangUSTC/cassandra-3.11.4)
* [The introduction on Apache Cassandra](https://docs.datastax.com/en/landing_page/doc/landing_page/cassandra.html)


## 3. Minimal setup to test the prototype
* Java >= 1.8 (OpenJDK and Oracle JVMS have been tested)
* Python 2.7 (for cqlsh)
* 16 GiB RAM
* 500 GiB HDD or SSD
* A 10Gb/s Ethernet switch
* Ubuntu 18.04 LTS or CentOS 7.6


## 4. Build and install DEPART project
* Getting the source code of DEPART  
`$ git clone --recursive https://github.com/ustcadsl/depart.git`

* Compile DEPART  
`$ cd depart`  
`$ ant clean & ant`


## 5. Build and install YCSB
* Clone the souce code of YCSB-0.15.0 ([YCSB-0.15.0](https://github.com/brianfrankcooper/YCSB))  
`$ git clone --recursive https://github.com/brianfrankcooper/YCSB.git`

* Compile the Cassandra package of YCSB  (Note that the maven environment need to be first installed)  
`$ mvn -pl site.ycsb:cassandra-binding -am clean package`



## 6. Testing the DEPART Prototype
* Start the Cassandra server in each node  
`$ bin/cassandra`

** Or For cassandra service forground
`$ bin/cassandra -f`

* Using the Cassandra Query Language in a node  
`$ bin/cqlsh $node IP$`

* Create keyspace (the database) and the table, so as to store user data  
`$ cqlsh> create keyspace ycsb WITH REPLICATION= {'class' : 'SimpleStrategy', 'replication_factor': 3};`  
`$ cqlsh> create table usertable ( y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar, field4 varchar, field5 varchar,field6 varchar,field7 varchar, field8 varchar, field9 varchar) WITH compaction={'class' : 'LeveledCompactionStrategy' };`

* Finally, using YCSB tool on the client node to issue requests to the cassandra cluster  
* **Load the database**  
`$ ./bin/ycsb load cassandra-cql -P workloads/workloadt  -p hosts=$node IP$ -threads $N1$ -p columnfamily=usertable -p recordcount=$N2$ ...`  

* **Run benchmarks based on the database**  
`$ ./bin/ycsb run cassandra-cql -P workloads/workloadt2  -p hosts=$node IP$ -threads $N1$ -p columnfamily=usertable -p operationcount=$N2$ ...`

* *Example:*
`$ ./bin/ycsb load cassandra-cql -P workloads/workloada -p hosts="127.0.0.1" -s -p columnfamily=usertable -p recordcount=1000`  
`$ ./bin/ycsb run cassandra-cql -P workloads/workloada -p hosts="127.0.0.1" -s -p columnfamily=usertable -p recordcount=1000`

## 7. Configure Depart for single node cluster(localhost)
* **Make cassandra /bin/ executable**
* `sudo chmod 777 -R ./bin/*`
  
* **Update cassandra.yaml file**
`rpc_address: localhost`
	
`listener_address: localhost`

`seed_provider:
  -class-name:
     parameters:
       - seeds: 127.0.0.1`

## 8. Configure Depart for Multi node cluster
* **Update cassandra.yaml file**
`rpc_address: <host-ip-address>`
	
`listener_address: <host-ip-address>`

`seed_provider:
  -class-name:
     parameters:
       - seeds: <node1-ip-address>,<node2-ip-address>`



## 8. Contact
* Please email to Mahmudul Hasan (hasancsedu5@gmail.com) if you have any questions.
