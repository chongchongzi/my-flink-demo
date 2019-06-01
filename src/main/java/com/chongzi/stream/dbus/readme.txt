一、准备工作

1、安装mysql

2、安装zk、hadoop、hbase、kafka

3、安装canal

    CREATE USER 'canal'@'%' IDENTIFIED BY 'canal%123';
    GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
    FLUSH PRIVILEGES;


二、模拟数据
http://www.wstmart.net/database-907.html
http://www.wstmart.net/database-932.html

a、商品表

CREATE TABLE test.dajiangtai_goods (
	goodsId INT(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
	goodsName varchar(50) NOT NULL COMMENT '商品名称',
	sellingPrice DECIMAL(11,2) DEFAULT 0.00 NOT NULL COMMENT '售价',
	goodsStock INT(11) DEFAULT 0 NOT NULL COMMENT '商品总库存',
	appraiseNum INT(11) DEFAULT 0 NOT NULL COMMENT '评价数',
	CONSTRAINT dajiangtai_goods_PK PRIMARY KEY (goodsId)
)ENGINE=InnoDB DEFAULT CHARSET=utf8

b、订单表

CREATE TABLE test.dajiangtai_orders (
  orderId int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  orderNo varchar(50) NOT NULL COMMENT '订单号',
  userId int(11) NOT NULL COMMENT '用户ID',
  goodId int(11) NOT NULL COMMENT '商品ID',
  goodsMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '商品总金额',
  realTotalMoney decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '实际订单总金额',
  payFrom int(11) NOT NULL DEFAULT '0' COMMENT '支付来源(1:支付宝，2：微信)',
  province varchar(50) NOT NULL COMMENT '省份',
  createTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`orderId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

c、flow表
CREATE TABLE test.dbus_flow (
  flowId int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  mode int(11) NOT NULL COMMENT '存储类型(#PHOENIX  #NATIVE   #STRING,默认STRING)',
  databaseName varchar(50) NOT NULL COMMENT 'database',
  tableName varchar(50) NOT NULL COMMENT 'table',
  hbaseTable varchar(50) NOT NULL COMMENT 'hbaseTable',
  family varchar(50) NOT NULL COMMENT 'family',
  uppercaseQualifier TINYINT(1) NOT NULL COMMENT '字段名转大写, 默认为true',
  commitBatch int(11) NOT NULL COMMENT '字段名转大写, 默认为true',
  rowKey varchar(100) NOT NULL COMMENT '组成rowkey的字段名，必须用逗号分隔',
  status int(11) NOT NULL COMMENT '状态:1-初始,2:就绪,3:运行',
  PRIMARY KEY (flowId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8



四、HBase建表

create 'learing_flink:dajiangtai_orders',{NAME=>'0',BLOCKCACHE=>true,BLOOMFILTER=>'ROW',COMPRESSION=>'SNAPPY',DATA_BLOCK_ENCODING => 'PREFIX_TREE', BLOCKSIZE => '65536'}



访问phoenix命令行：
./sqlline.py slave01,slave02,slave03:2181:/hbase-unsecure


CREATE TABLE IF NOT EXISTS learing_flink.dajiangtai_goods(
id char(10) not null primary key,
goodsId INTEGER,
goodsName VARCHAR(50),
sellingPrice DECIMAL(11,2),
goodsStock INTEGER,
appraiseNum INTEGER
)DATA_BLOCK_ENCODING='PREFIX_TREE',COMPRESSION='SNAPPY';




三、canal FlatMessage

   json结构，可转通过Schema转为FlatMessage

1> FlatMessage [id=4, database=test, table=student, isDdl=false, type=UPDATE, es=1551597969000, ts=1551597969334, sql=, sqlType={password=12, name=12, id=4, age=4}, mysqlType={password=varchar(25), name=varchar(25), id=int(11) unsigned, age=int(10)}, data=[{password=11, name=张三, id=5, age=35}], old=[{age=23}]]
2> FlatMessage [id=5, database=test, table=student, isDdl=false, type=INSERT, es=1551602824000, ts=1551602824496, sql=, sqlType={password=12, name=12, id=4, age=4}, mysqlType={password=varchar(25), name=varchar(25), id=int(11) unsigned, age=int(10)}, data=[{password=33, name=李四, id=6, age=36}], old=null]
