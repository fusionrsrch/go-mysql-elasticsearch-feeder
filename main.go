package main

import (
	"fmt"
	"github.com/fusionrsrch/go-mysql-elasticsearch-feeder/elastic"
	"github.com/fusionrsrch/go-mysql/client"
	"github.com/fusionrsrch/go-mysql/mysql"
	"github.com/fusionrsrch/go-mysql/replication"
	"os"
	"strconv"
)

var (
	//binlogFile    string = "mysql-bin.000002"
	// global makes strconv somehow
	//binlogPos     uint32
	slaveServerId uint32 = 123
	masterHost    string = "127.0.0.1"
	masterPort    uint16 = 3306
	masterUser    string = "root"
	masterPass    string = ""
	conn          *client.Conn
)

func main() {

	fmt.Println("Go MySQL ElasticSearch Feeder")

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	syncer := replication.NewBinlogSyncer(slaveServerId, "mysql")

	//fmt.Println(syncer)
	//fmt.Printf("%+v\n", syncer)

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err := syncer.RegisterSlave(masterHost, masterPort, masterUser, masterPass)
	//fmt.Printf("%+v\n", syncer)

	if err != nil {
		fmt.Println(err)
		return
	}

	//pos := &Vertex{3, 4}

	// Find binlogFile, binlogPos
	mpos, err := getBinLogAndPosition()
	if err != nil {
		fmt.Println(err)
		return
	}

	//pos := mysql.Position{"/tmp/dude", 4}

	// Start sync with sepcified binlog file and position
	//streamer, _ := syncer.StartSync(binlogFile, binlogPos)
	streamer, err := syncer.StartSync(*mpos)
	// //fmt.Printf("%+v\n", streamer)

	if err != nil {
		fmt.Println(err)
		return
	}

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	for {
		ev, err := streamer.GetEvent()
		//_, err := streamer.GetEvent()

		if err != nil {
			fmt.Println(err)
			return
		}
		// Dump event
		ev.Dump(os.Stdout)
	}
}

// Execute a SQL
func ExecuteSQL(cmd string, args ...interface{}) (rr *mysql.Result, err error) {

	//c.connLock.Lock()
	//defer c.connLock.Unlock()

	// retryNum := 3
	// for i := 0; i < retryNum; i++ {
	// 	if c.conn == nil {
	masterPortStr := strconv.FormatUint(uint64(masterPort), 10)

	var addr string = masterHost + ":" + masterPortStr
	conn, err = client.Connect(addr, masterUser, masterPass, "")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	// 	}

	rr, err = conn.Execute(cmd, args...)
	if err != nil && err != mysql.ErrBadConn {
		return
	} else if err == mysql.ErrBadConn {
		// 		c.conn.Close()
		// 		c.conn = nil
		// 		continue
		conn.Close()
	} else {
		return
	}
	// }
	return
}

func getBinLogAndPosition() (mp *mysql.Position, err error) {
	//fmt.Println(" checkBinlogRowFormat")
	//var binlogPos uint32 = 0
	var binlogPos uint64

	res, err := ExecuteSQL(`SHOW MASTER STATUS;`)
	if err != nil {
		return nil, err
	}

	binlogFile, _ := res.GetString(0, 0)
	binlogPosStr, _ := res.GetString(0, 1)

	fmt.Println(" ", binlogFile, binlogPos)

	binlogPos, _ = strconv.ParseUint(binlogPosStr, 0, 64)

	return &mysql.Position{binlogFile, uint32(binlogPos)}, nil
}
