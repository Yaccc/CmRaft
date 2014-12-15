Project CmRaft 
================

Project CmRaft is a Java implementation of Raft algorithm, which is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems.
Check this link for more detailed information about Raft algorithm: [https://raftconsensus.github.io/](https://raftconsensus.github.io/)

CmRaft implemented leader election, log replication and log persistence so far. Membership changing and log compaction will be implemented soon.

Build 
------

### Prerequisites: 
- JDK 1.7
- Maven 3.x

### Download source code: 
		git clone https://github.com/chicm/CmRaft.git

### Compile and make tar ball: 
		mvn package -DskipTests

### Import to eclipse: 
		mvn eclispe:eclipse
Then use eclipse to open the project. To run a unit test, which will create a local cluster and test the basic functions in command line console:
		
		maven test
### Key value store APIs usage:
Key value store APIs allow users to manipulate key value pairs against a replicated key value store inside CmRaft cluster, including adding, updating, deleting, listing, etc. It's very simple to use key value store APIs:

		try(Connection conn = ConnectionManager.getConnection()) { 
			KeyValueStore kvs = conn.getKeyValueStore();
			kvs.set("key1", "value1"); 
			kvs.set("key1", "value2"); 
			kvs.delete("key1");  
			String value = kvs.get("key1");  
			List<KeyValue> result = kvs.list(); 
		} finally {
		}

Contact: Chi Cheng Min, [chicm.dev@gmail.com](mailto:chicm.dev@gmail.com)

CmRaft中文说明
=============

CmRaft是Raft算法的Java的实现。CmRaft已经列在Raft官方网站的Raft实现列表中了，从官方网站首页下方的Where can I get Raft一节中可以看到CmRaft。
[https://raftconsensus.github.io/](https://raftconsensus.github.io/)

什么是Raft算法?
----------
简单来说，Raft是一种易于理解的一致性算法，其功能相当于Paxos。Raft出现以前, Paxos虽然是主流一致性协议（Chubby和ZooKeeper都是Paxos的实现），但其也以难以理解而著称，根据Paxos实现出一个稳定高效的版本比较困难。针对Paxos难以理解的缺陷，Raft设计的主要目的之一就是容易理解，Raft将整个算法过程分解为若干个独立的子过程，并且详细定义了每个子过程具体流程，容易理解和实现。Raft具备Paxos所有功能，根据Raft实现一致性协议更加容易实现高效稳定的版本，Raft在工程实践中优于Paxos, 因此Raft是一个更先进的一致性协议。
如需了解Raft协议原理，请参考我的博客：

[Raft系列文章之一: 什么是Raft?](http://blog.csdn.net/chicm/article/details/41788773)

[Raft系列文章之二：Leader选举](http://blog.csdn.net/chicm/article/details/41794475)

[Raft系列文章之三：Raft RPC 详解](http://blog.csdn.net/chicm/article/details/41909261)

将持续添加更多文章...

CmRaft技术架构
-------------
CmRaft系统架构如下图：
![CmRaft system architecture](http://chicm.github.io/images/cmraft_architecture.jpg)

CmRaft架构比较简单，核心部分包含如下模块，分别对应Raft算法各个功能

- Rpc层： 负责节点间的通信
- Leader选举(状态机)： 内部维护一个状态机和计时器，负责Raft Leader选举
- Log管理（多副本日志）：作为Leader或Follower时的Log添加，状态转换，持久化，压缩
- 集群成员变化：处理集群成员的动态变化，这部分正在开发中

外围模块包括：

- 用户API: 包括键值存储API和状态机API，状态机API将向用户提供CmRaft节点的状态信息，用户可在此基础上开发高可用的应用，这部分尚未完成。
- Shell: 提供一个命令行访问CmRaft的界面，尚未完成。
- 监控: 提供基于Web的监控界面，尚未完成

CmRaft用户API既可以在进程外使用，即可以把CmRaft作为一个独立进程启动，然后连接上去进行访问。也可以在进程内使用，即用户进程调用CmRaft API创建CmRaft节点，然后创建至改节点的连接后进行访问，在进程内访问时，将绕过RPC层，提供基于内存的访问，这种方式效率更高。

Raft算法集群中各节点使用RPC进行通信，CmRaft的RPC模块的序列化采用了Protocol Buffers, Protocol Buffers是Google开源的序列化框架，具备简单，性能高的特点，它本身并没有实现RPC, 因此CmRaft自己实现了RPC通信的代码。在网络通信层，CmRaft提供两种实现，一是基于Netty, Netty具备稳定，高效，性能好，扩展性好的特点，基于Netty开发的代码容易维护。二是基于JDK 7 NIO2异步socket实现，直接使用JDK overhead更低，所以性能更好，但稳定性，扩展性，代码可读性不如使用Netty, 目前最新的代码默认使用Netty。在使用Netty情况下，我对RPC层进行了性能测试，在我的32核2.0G Xeon Linux服务器上，对于1K大小的RPC请求，CmRaft的Rpc可以达到每秒18万TPS，而直接使用JDK NIO时则可以达到20万TPS。

键值对存储 (Key Value Store) API用法
-----------------------------
Key value store API提供键值对的添加，更改，删除，查询，用法很简单，例如：

		try(Connection conn = ConnectionManager.getConnection()) { //conn会自动关闭
			KeyValueStore kvs = conn.getKeyValueStore();
			kvs.set("key1", "value1"); //添加
			kvs.set("key1", "value2"); //更改
			kvs.delete("key1");  //删除
			String value = kvs.get("key1");  //查询单条	
			List<KeyValue> result = kvs.list();  //查询所有Key Value
		} finally {
		}


状态机API用法
------------
待更新...

作者：迟承敏， [chicm.dev@gmail.com](mailto:chicm.dev@gmail.com)