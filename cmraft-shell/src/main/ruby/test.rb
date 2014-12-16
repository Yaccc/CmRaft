require 'java'

#LocalCluster clu = LocalCluster.create(10, 13999);

cluster = Java::com.chicm.cmraft.core.LocalCluster.create ( 3, 10028)

