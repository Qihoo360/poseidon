package main

import (
	idgenerator "github.com/Qihoo360/poseidon/service/idgenerator/module"
	meta "github.com/Qihoo360/poseidon/service/meta/module"
	proxy "github.com/Qihoo360/poseidon/service/proxy/module"
	searcher "github.com/Qihoo360/poseidon/service/searcher/module"
	"github.com/zieckey/simgo"
)

func main() {
	fw := simgo.DefaultFramework
	fw.RegisterModule("meta", meta.New())
	fw.RegisterModule("idgenerator", idgenerator.New())
	fw.RegisterModule("proxy", proxy.New())
	fw.RegisterModule("searcher", searcher.New())
	err := fw.Initialize()
	if err != nil {
		panic(err.Error())
	}

	fw.Run()
}
