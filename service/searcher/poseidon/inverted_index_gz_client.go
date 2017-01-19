package poseidon

import (
	"errors"
	"fmt"
	poseidon_if "github.com/Qihoo360/poseidon/service/searcher/proto"
	"github.com/golang/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"log"
	neturl "net/url"
	"strings"
)

const InvertedIndexGzCount = 200

type InvertedIndexGzClient struct{}

func logId(id *poseidon_if.DocId) uint64 {
	return uint64(id.DocId)*1e6 + uint64(id.RowIndex)
}

type DocItemList []poseidon_if.DocId

func (a DocItemList) Len() int           { return len(a) }
func (a DocItemList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DocItemList) Less(i, j int) bool { return logId(&a[i]) < logId(&a[j]) }

type IndexDataResult struct {
	Token    string
	Pv, Uv   int
	DocItems DocItemList
	Err      error
}

var ToEscapeFields = map[string]int{
	"fname": 1,
}

func normalizedToken(field, token string) string {
	if _, ok := ToEscapeFields[field]; ok {
		token = neturl.QueryEscape(token)
	}

	return strings.ToLower(token)
}

func indexHashId(token string) int32 {
	hash := murmur3.Sum32([]byte(token))
	if int32(hash) < 0 {
		return -int32(hash)
	}
	return int32(hash)
}

func (self *InvertedIndexGzClient) storedGet(business, key string, dockeys map[string]string) (value []byte, err error) {
	log.Println("stored get key ", key)

	if dockeys == nil {
		return nil, errors.New("dockeys is null")
	}
	if v, ok := dockeys[key]; ok {
		log.Println("stored get key result= ", string(v))
		return []byte(v), nil
	}

	return nil, errors.New("dockey is not found")

}

func MetaIdHdfsFilePart(gzMetaId string) string {
	hash := 1
	for i := 0; i < len(gzMetaId); i++ {
		hash = (31 * hash) + int(gzMetaId[i])
	}
	return fmt.Sprintf("%05d", hash&2147483647%1000)
}

// 取各个token的IndexMeta列表
func (self *InvertedIndexGzClient) fetchIndexMeta(day string, business string, field string, token string, dockeys map[string]string) (*poseidon_if.InvertedIndexGzMeta, error) {

	data, err := self.storedGet(business, BusinessTraitInstance(business).IndexStoredKey(day, field, token), dockeys)

	if err != nil {
		log.Printf("fetchIndexMeta token=%s,storedGetError=%v", token, err)
		return nil, err
	}

	// protobuf 解析，得到meta的三个字段
	meta := &poseidon_if.InvertedIndexGzMeta{}
	err = proto.Unmarshal(data, meta)
	if err != nil {
		// TODO : error handling
		log.Printf("fetchIndexMeta token=%s,unmarshalError=%v", token, err)
		return nil, errors.New("stored read is faild")
	}

	meta.Path = BusinessTraitInstance(business).IndexFilePath(day, field, token)

	log.Printf("fetchIndexMeta ok, token=%s,filePath=%s,offset=%d,length=%d", token,
		meta.Path, meta.Offset, meta.Length)
	return meta, nil
}

func (self *InvertedIndexGzClient) fetchIndexData(day, field, token string, meta *poseidon_if.InvertedIndexGzMeta) (pv, uv int, docList DocItemList, err error) {
	if meta == nil {
		log.Printf("fetchIndexData nil meta, token=%s", token)
		return 0, 0, nil, errors.New("fetchIndexDataNilMeta")
	}
	indexData, err := hdfsReader.ReadZip(meta.Path, int64(meta.Offset), int64(meta.Length))
	// indexData, err := hdfsReader.Read(meta.Path, int64(meta.Offset), int64(meta.Length))
	if err != nil {
		log.Printf("ReadHDFS indexData read err %v token=%s", err, token)
		return 0, 0, nil, errors.New("read hdfs fail")
	}

	protoIndex := &poseidon_if.InvertedIndex{}
	err = proto.Unmarshal(indexData, protoIndex)
	if err != nil {
		// TODO : error handling
		log.Printf("ReadHDFS indexData Unmarshal err %v, token=%s", err, token)
		return 0, 0, nil, errors.New("read hdfs fail")
	}

	for tk, items := range protoIndex.Index {
		log.Printf("ReadHDFS indexData result tk=%s items=%d", tk, len(items.DocIds))
	}

	ntoken := strings.ToLower(neturl.QueryEscape(token))

	if items, ok := protoIndex.Index[ntoken]; ok {
		log.Printf("ReadHDFS indexData ok, token=%s,docIdCount=%d", ntoken, len(items.DocIds))
		for i, item := range items.DocIds {

			docId := item.DocId
			if i == 0 {
				pv = int(item.RowIndex)
				uv = int(item.DocId)
				fmt.Printf("fetchIndexData token stat token=%s pv=%d uv=%d\r\n", token, pv, uv)
				continue
			}
			if i > 1 {
				docId += docList[i-2].DocId
			}

			// log.Printf("fetchIndexData token=%s doc found %d,%d", token, docId, item.RowIndex)
			docList = append(docList, poseidon_if.DocId{DocId: docId, RowIndex: uint32(item.RowIndex)})
		}
		return pv, uv, docList, nil
	}
	log.Printf("ReadHDFS indexData field=%s token=%s not found", field, token)
	return 0, 0, docList, nil
}

func (self *InvertedIndexGzClient) FetchIndex(day string, business string, keywords map[string]string) (indexDataList []*IndexDataResult) {
	ch := make(chan IndexDataResult)
	tokenCount := 0

	var err error
	dockeys := make(map[string]string, len(keywords))
	result := make(map[string]string, len(keywords))

	for filed, token := range keywords {
		sk := BusinessTraitInstance(business).IndexStoredKey(day, filed, token)
		dockeys[sk] = ""
		log.Println("Add dockey ", sk)
	}
	result, err = SymcGet(BusinessTraitInstance(business).IndexStoredAddr(), dockeys)
	if err != nil {
		log.Println("GetkeyBySymc err ", err)
	}

	log.Println("FetchIndex symc key", result)

	for field, tokens := range keywords {
		tokenList := strings.Split(tokens, "\t")
		for _, token := range tokenList {
			if len(token) == 0 {
				continue
			}
			tokenCount++
			go func(field, token string) {
				meta, err := self.fetchIndexMeta(day, business, field, token, result)
				// TODO : 直接读HDFS的 InvertedIndexGz
				var docItems DocItemList
				var pv, uv int
				if err == nil {
					log.Printf("FetchIndex routine field=%s token=%s path=%s", field, token, meta.Path)
					pv, uv, docItems, err = self.fetchIndexData(day, field, token, meta)
				} else {
					log.Printf("FetchIndex routine field=%s token=%s nil_data", field, token)
				}
				log.Printf("FetchIndex routine field=%s token=%s docList.size=%d", field, token, len(docItems))
				ch <- IndexDataResult{Token: token, Pv: pv, Uv: uv, DocItems: docItems, Err: err}
			}(field, token)
		}
	}

	for i := 0; i < tokenCount; i++ {
		select {
		case result := <-ch:
			indexDataList = append(indexDataList, &result)
		}
	}
	return indexDataList
}
