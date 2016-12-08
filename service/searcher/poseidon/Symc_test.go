package poseidon

import (
	"log"
	"testing"
)

func Test_StmcGet(t *testing.T) {
	log.Println("test...")
	m := make(map[string]string, 10)
	m["160630401156861"] = ""
	m["160630401156816"] = ""
	m["160630401156858"] = ""
	m["160630401156874"] = ""
	m["160630401156817"] = ""

	r, err := SymcGet(m)
	if r != nil {
		panic(err)
	}
	log.Println(r)
}
