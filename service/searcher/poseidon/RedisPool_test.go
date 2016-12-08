package poseidon

import (
	"fmt"
	"testing"
)

var servers []string = []string{
	"127.0.0.1:9999:61c7b9811799779d",
	"127.0.0.1:6134:74de338358be6568:1",
}

// func Test_multipool(t *testing.T) {
// 	Rp.Init(servers)
// 	fmt.Println(RedisMultiPool[1].Get().Do("keys", "*"))
// }
//
func Test_Getconn(t *testing.T) {
	Rp.Init(servers)
	conn := GetConn("hhjj")

	fmt.Println(conn)
}
