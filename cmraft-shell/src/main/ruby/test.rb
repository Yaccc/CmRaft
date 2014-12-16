require 'java'

#LocalCluster clu = LocalCluster.create(10, 13999);

cluster = Java::com.chicm.cmraft.core.LocalCluster.create ( 3, 10028)
sleep(10)
conn = cluster.getConnection()
kvs = conn.getKeyValueStore()
kvs.set("aaa", "bbb")
p "***************"
r = kvs.list("aaa").get(0).toString()
p "result:" + r
