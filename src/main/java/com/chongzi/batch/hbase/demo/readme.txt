准备工作：

1、安装hbase(zookeeper、hdfs)

2、在hbase中创建namespace和table

create_namespace 'learing_flink'

create 'learing_flink:users',{NAME=>'F',BLOCKCACHE=>true,BLOOMFILTER=>'ROW',COMPRESSION=>'SNAPPY',DATA_BLOCK_ENCODING => 'PREFIX_TREE', BLOCKSIZE => '65536'}

3、写数据到hbase(使用HadoopOutputFormat)



4、从Hbase读取数据(使用HadoopInputFormat)

5、常用命令
//查看数据
scan 'learing_flink:users'
//count
count 'learing_flink:users'
//清空表数据
truncate_preserve 'learing_flink:users'
