package db

import (
	"Open_IM/src/common/config"
	"Open_IM/src/common/log"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"sync"
	"time"
)

type mysqlDB struct {
	sync.RWMutex
	dbMap map[string]*gorm.DB
}
// 初始化数据库，创建表
func initMysqlDB() {
	//When there is no open IM database, connect to the mysql built-in database to create openIM database
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		config.Config.Mysql.DBUserName, config.Config.Mysql.DBPassword, config.Config.Mysql.DBAddress[0], "mysql")

	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		log.Error("", "", dsn)
		panic(err)
	}

	//Check the database and table during initialization
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ;", config.Config.Mysql.DBDatabaseName)
	err = db.Exec(sql).Error
	if err != nil {
		panic(err)
	}
	db.Close()

	dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		config.Config.Mysql.DBUserName, config.Config.Mysql.DBPassword, config.Config.Mysql.DBAddress[0], config.Config.Mysql.DBDatabaseName)
	db, err = gorm.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
// 用户注册表
	sqlTable := "CREATE TABLE IF NOT EXISTS `user` (\n  `uid` varchar(64) NOT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `icon` varchar(1024) DEFAULT NULL,\n  `gender` int(11) unsigned zerofill DEFAULT NULL,\n  `mobile` varchar(32) DEFAULT NULL,\n  `birth` varchar(16) DEFAULT NULL,\n  `email` varchar(64) DEFAULT NULL,\n  `ex` varchar(1024) DEFAULT NULL,\n  `create_time` datetime DEFAULT NULL,\n  PRIMARY KEY (`uid`),\n  UNIQUE KEY `uk_uid` (`uid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 用户好友表
	sqlTable = "CREATE TABLE IF NOT EXISTS `friend` (\n  `owner_id` varchar(255) NOT NULL,\n  `friend_id` varchar(255) NOT NULL,\n  `comment` varchar(255) DEFAULT NULL,\n  `friend_flag` int(11) NOT NULL,\n  `create_time` datetime NOT NULL,\n  PRIMARY KEY (`owner_id`,`friend_id`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 好友申请表
	sqlTable = "CREATE TABLE IF NOT EXISTS  `friend_request` (\n  `req_id` varchar(255) NOT NULL,\n  `user_id` varchar(255) NOT NULL,\n  `flag` int(11) NOT NULL DEFAULT '0',\n  `req_message` varchar(255) DEFAULT NULL,\n  `create_time` datetime NOT NULL,\n  PRIMARY KEY (`user_id`,`req_id`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 黑名单
	sqlTable = "CREATE TABLE IF NOT EXISTS `black_list` (\n  `uid` varchar(32) NOT NULL COMMENT 'uid',\n  `begin_disable_time` datetime DEFAULT NULL,\n  `end_disable_time` datetime DEFAULT NULL,\n  `ex` varchar(1024) DEFAULT NULL,\n  PRIMARY KEY (`uid`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 用户黑名单
	sqlTable = "CREATE TABLE IF NOT EXISTS `user_black_list` (\n  `owner_id` varchar(255) NOT NULL,\n  `block_id` varchar(255) NOT NULL,\n  `create_time` datetime NOT NULL,\n  PRIMARY KEY (`owner_id`,`block_id`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 群组
	sqlTable = "CREATE TABLE IF NOT EXISTS `group` (\n  `group_id` varchar(255) NOT NULL,\n  `name` varchar(255) DEFAULT NULL,\n  `introduction` varchar(255) DEFAULT NULL,\n  `notification` varchar(255) DEFAULT NULL,\n  `face_url` varchar(255) DEFAULT NULL,\n  `create_time` datetime DEFAULT NULL,\n  `ex` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`group_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 群组成员
	sqlTable = "CREATE TABLE IF NOT EXISTS `group_member` (\n  `group_id` varchar(255) NOT NULL,\n  `uid` varchar(255) NOT NULL,\n  `nickname` varchar(255) DEFAULT NULL,\n  `user_group_face_url` varchar(255) DEFAULT NULL,\n  `administrator_level` int(11) NOT NULL,\n  `join_time` datetime NOT NULL,\n  PRIMARY KEY (`group_id`,`uid`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 群申请记录
	sqlTable = "CREATE TABLE IF NOT EXISTS `group_request` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `group_id` varchar(255) NOT NULL,\n  `from_user_id` varchar(255) NOT NULL,\n  `to_user_id` varchar(255) NOT NULL,\n  `flag` int(10) NOT NULL DEFAULT '0',\n  `req_msg` varchar(255) DEFAULT '',\n  `handled_msg` varchar(255) DEFAULT '',\n  `create_time` datetime NOT NULL,\n  `from_user_nickname` varchar(255) DEFAULT '',\n  `to_user_nickname` varchar(255) DEFAULT NULL,\n  `from_user_face_url` varchar(255) DEFAULT '',\n  `to_user_face_url` varchar(255) DEFAULT '',\n  `handled_user` varchar(255) DEFAULT '',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8mb4;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}
// 聊天记录
	sqlTable = "CREATE TABLE IF NOT EXISTS  `chat_log` (\n  `msg_id` varchar(128) NOT NULL,\n  `send_id` varchar(255) NOT NULL,\n  `session_type` int(11) NOT NULL,\n  `recv_id` varchar(255) NOT NULL,\n  `content_type` int(11) NOT NULL,\n  `msg_from` int(11) NOT NULL,\n  `content` varchar(1000) NOT NULL,\n  `remark` varchar(100) DEFAULT NULL,\n  `sender_platform_id` int(11) NOT NULL,\n  `send_time` datetime NOT NULL,\n  PRIMARY KEY (`msg_id`) USING BTREE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;"
	err = db.Exec(sqlTable).Error
	if err != nil {
		panic(err)
	}

}
// 获取数据库链接
func (m *mysqlDB) DefaultGormDB() (*gorm.DB, error) {
	return m.GormDB(config.Config.Mysql.DBAddress[0], config.Config.Mysql.DBDatabaseName)
}

func (m *mysqlDB) GormDB(dbAddress, dbName string) (*gorm.DB, error) {
	m.Lock()
	defer m.Unlock()
// 不同地址的库作为key，查找已有的链接，如果不存在链接， 那么建立
	k := key(dbAddress, dbName)
	if _, ok := m.dbMap[k]; !ok {
		if err := m.open(dbAddress, dbName); err != nil {
			return nil, err
		}
	}
	return m.dbMap[k], nil
}

func (m *mysqlDB) open(dbAddress, dbName string) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		config.Config.Mysql.DBUserName, config.Config.Mysql.DBPassword, dbAddress, dbName)
		// gorm模块
	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		return err
	}
// 设置数据库连接池的 最大链接数，空闲数 存活时间
	db.SingularTable(true)
	db.DB().SetMaxOpenConns(config.Config.Mysql.DBMaxOpenConns)
	db.DB().SetMaxIdleConns(config.Config.Mysql.DBMaxIdleConns)
	db.DB().SetConnMaxLifetime(time.Duration(config.Config.Mysql.DBMaxLifeTime) * time.Second)

	if m.dbMap == nil {
		m.dbMap = make(map[string]*gorm.DB)
	}
	k := key(dbAddress, dbName)
	m.dbMap[k] = db
	return nil
}
