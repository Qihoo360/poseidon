package poseidon

import (
	"fmt"
	"github.com/zieckey/simgo"
	"net/url"
	"strings"
	"time"
)

var SimgoFramework *simgo.Framework

type BusinessTrait interface {
	BusinessName() string
	FieldShortName(field string) string

	IndexStoredAddr() string
	IndexStoredKey(day string, field string, token string) string
	IndexFilePath(day string, field string, token string) string

	DocStoredAddr() string
	DocIdStoredKey(day string, docId uint64) string
	DocFilePath(day string, name string) string
}

//var testTrait = TestTrait{name: "test"}

func BusinessTraitInstance(business string) BusinessTrait {
	testTrait :=  TestTrait{name: business}
	return &testTrait
	//multi business
	/*switch business {
	case "test":
		return &testTrait
	}

	return &testTrait*/
}

type TestTrait struct {
	name string
}

func (self *TestTrait) BusinessName() string {
	return self.name
}

/**
 * 字段映射
 */
func (self *TestTrait) FieldShortName(field string) string {
	return field
}

func (self *TestTrait) IndexStoredAddr() string {
	indexStoredAddr, _ := SimgoFramework.Conf.SectionGet("searcher", "indexStored")
	indexStoredAddr = strings.Replace(indexStoredAddr, "business", self.name, -1)
	return indexStoredAddr
}

func (self *TestTrait) IndexStoredKey(day string, field string, token string) string {
	token = strings.ToLower(url.QueryEscape(token))
	hashId := indexHashId(normalizedToken(field, token))
	indexMetaKey := fmt.Sprintf("%08d", hashId/InvertedIndexGzCount)
	date := strings.Replace(day, "-", "", -1)
	return field + date[2:8] + indexMetaKey
}

func (self *TestTrait) IndexFilePath(day string, field string, token string) string {
	token = strings.ToLower(url.QueryEscape(token))
	hashId := indexHashId(normalizedToken(field, token))
	indexMetaKey := fmt.Sprintf("%08d", hashId/InvertedIndexGzCount)

	return "/home/poseidon/src/" + self.name +"/index/" + day + "/" + field + "index/part-" +
		MetaIdHdfsFilePart(indexMetaKey) + ".gz"
}

func (self *TestTrait) DocStoredAddr() string {
	docidStoredAddr, _ := SimgoFramework.Conf.SectionGet("searcher", "docStored")
	docidStoredAddr = strings.Replace(docidStoredAddr, "business", self.name, -1)
	return docidStoredAddr
}

func (self *TestTrait) DocIdStoredKey(day string, docId uint64) string {
	day = strings.Replace(day, "-", "", -1)
	if len(day) != 8 {
		return ""
	}
	return fmt.Sprintf("%s%v", day[2:8], docId)
}

func (self *TestTrait) DocFilePath(day string, name string) string {
	shortName := name[:len(name)-4]
	hour := name[len(name)-4 : len(name)-2]
	minute := name[len(name)-2:]

	return "/home/poseidon/src/" + self.name +"/" + day + "/" +
		shortName + "_" + day + "-" + hour + "-" + minute + ".gz"
}

/**
 * 计算归档日期 默认一个月
 */
func GetArchiveDay() string {
	day := time.Now().AddDate(0, -1, -1) // 上月前1号之前会被归档
	return fmt.Sprintf("%4d-%02d-%02d", day.Year(), day.Month(), day.Day())
}
