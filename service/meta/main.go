package main

import (
	"github.com/Qihoo360/poseidon/service/meta/module"
	"github.com/zieckey/simgo"
)

func main() {
	fw := simgo.DefaultFramework
	fw.RegisterModule("meta_service", module.New())
	err := fw.Initialize()
	if err != nil {
		panic(err.Error())
	}

	fw.Run()
}
