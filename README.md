Project CmRaft 
================

Project CmRaft is a Java implementation of Raft algorithm, which is  is a distributed consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems.
Check this link for more detailed information about Raft algorithm: https://raftconsensus.github.io/

CmRaft strictly follows Raft protocol to implement Raft functions, includes state machine, leader election, leader failure detection, log replication, etc. The core functions of CmRaft are ready, user APIs, management tools, monitor facilities, log compaction functions are still under construction.

Build 
======

Prerequisites:
JDK 1.7
Maven 3.x

Download source code:
git clone https://github.com/chicm/CmRaft.git

Compile and make tar ball:
mvn package

Import to eclipse:
mvn eclispe:eclipse
Then use eclipse to open the project.

To run a unit test, which will create a local cluster and test the basic functions in command line console:
maven test