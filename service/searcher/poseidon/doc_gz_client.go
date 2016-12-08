package poseidon

import (
	"encoding/base64"
	"errors"
	"fmt"
	poseidon_if "github.com/Qihoo360/poseidon/service/searcher/proto"
	set "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/proto"
	"log"
	"sort"
	"strings"
)

type DocIdList []int32

func (a DocIdList) Len() int           { return len(a) }
func (a DocIdList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DocIdList) Less(i, j int) bool { return a[i] < a[j] }

type DocDataResult struct {
	DocId    uint64
	RowIndex uint32
	Data     []byte
	Err      error
}

func NewDocDataResult() *DocDataResult {
	return &DocDataResult{}
}

func (self *DocDataResult) LogId() uint64 {
	return uint64(self.DocId)*1e6 + uint64(self.RowIndex)
}

func (self *DocDataResult) FilterColumns(seperator string, columns []int) {
	fields := strings.Split(string(self.Data), seperator)
	var result string
	for i, col := range columns {
		if i > 0 {
			result += seperator
		}
		if col >= 0 && col < len(fields) {
			result += fields[col]
		}
	}

	self.Data = []byte(result)
}

func (self *DocDataResult) ToJson(business string) string {
	var data []byte

	if self.Err == nil {
		data = self.Data
	} else {
		data = []byte(self.Err.Error())
	}

	return fmt.Sprintf("{\"doc_id\":%d,\"row_index\":%d,\"base64\":1,\"data\":\"%s\"}",
		self.DocId, self.RowIndex, base64.StdEncoding.EncodeToString(data))
}

type DocDataResultList []*DocDataResult

func (a DocDataResultList) Len() int           { return len(a) }
func (a DocDataResultList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DocDataResultList) Less(i, j int) bool { return a[i].LogId() < a[j].LogId() }

type DocGzClient struct {
}

func SliceItemToInterface(src DocItemList) []interface{} {
	dst := make([]interface{}, len(src))
	for i := range src {
		dst[i] = logId(&src[i])
	}
	return dst
}
func SliceInterfaceToItem(src []interface{}) DocItemList {
	dst := make(DocItemList, len(src))
	for i := range src {
		logId := src[i].(uint64)
		dst[i].DocId = uint64(logId / 1e6)
		dst[i].RowIndex = uint32(logId % 1e6)
	}
	return dst
}

func (self *DocGzClient) DocIdIntersect(tokenDocItems *[]DocItemList) DocItemList {
	if len(*tokenDocItems) <= 0 {
		return DocItemList{}
	}
	intersect := set.NewSetFromSlice(SliceItemToInterface((*tokenDocItems)[0]))

	for i := 1; i < len(*tokenDocItems); i++ {
		current := set.NewSetFromSlice(SliceItemToInterface((*tokenDocItems)[i]))
		intersect = intersect.Intersect(current)
	}

	ret := SliceInterfaceToItem(intersect.ToSlice())
	sort.Sort(ret)
	return ret
}

func (self *DocGzClient) storedGet(business string, key string, dockeys map[string]string) (Value []byte, err error) {
	if dockeys == nil {
		log.Println("dockeys is nil")
		return nil, errors.New("dockeys is null")
	}
	if v, ok := dockeys[key]; ok {
		return []byte(v), nil
	}

	return nil, errors.New("doc key not found")
}

func (self *DocGzClient) fetchDocMeta(day string, business string, docId uint64, dockeys map[string]string) (*poseidon_if.DocGzMeta, error) {
	// Get src single value
	data, err := self.storedGet(business, BusinessTraitInstance(business).DocIdStoredKey(day, docId), dockeys)
	if err != nil {
		log.Printf("fetchDocMeta doc=%v,error=%v", docId, err)
		return nil, err
	}

	meta := &poseidon_if.DocGzMeta{}
	err = proto.Unmarshal(data, meta)
	if err != nil {
		// TODO : error handling
		log.Printf("fetchDocMeta Unmarshal error,doc=%d", docId)
		return nil, err
	}
	if len(meta.Path) == 0 {
		log.Printf("fetchDocMeta error, docId=%d,filePath=%s,offset=%d,length=%d", docId,
			meta.Path, meta.Offset, meta.Length)
		return nil, errors.New("stored key not found")
	}

	meta.Path = BusinessTraitInstance(business).DocFilePath(day, meta.Path)

	log.Printf("fetchDocMeta ok, docId=%d,filePath=%s,offset=%d,length=%d", docId,
		meta.Path, meta.Offset, meta.Length)
	return meta, nil
}

func (self *DocGzClient) fetchDocData(business string, meta *poseidon_if.DocGzMeta) (docData []byte, err error) {
	if meta == nil {
		fmt.Printf("fetchDocData nil meta\r\n")
		return nil, errors.New("fetchDocData params meta is null")
	}

	docData, err = hdfsReader.ReadZip(meta.Path, int64(meta.Offset), int64(meta.Length))

	if err != nil {
		log.Printf("fetchDocData docData err %v", err)
		return nil, err
	}
	log.Printf("fetchDocData ok, filePath=%s,offset=%d,length=%d",
		meta.Path, meta.Offset, meta.Length)
	return docData, nil
}

func ParseDocRow(data []byte, rowIndex uint32) []byte {
	items := strings.Split(string(data), "\n")
	if int(rowIndex) < len(items) {
		return []byte(items[rowIndex])
	}
	return data
}

func (self *DocGzClient) FetchDocItems(day string, business string, docItems *DocItemList) (dataList DocDataResultList) {
	ch := make(chan DocDataResult)
	var err error
	dockeys := make(map[string]string, len(*docItems))
	result := make(map[string]string, len(*docItems))

	for _, dl := range *docItems {
		sk := BusinessTraitInstance(business).DocIdStoredKey(day, dl.DocId)
		dockeys[sk] = ""
	}
	result, err = SymcGet(BusinessTraitInstance(business).DocStoredAddr(), dockeys)
	if err != nil {
		log.Println("GetkeyBySymc err ", err)
	}

	rowIndexList := []uint32{}
	for pos, docItem := range *docItems {
		rowIndexList = append(rowIndexList, docItem.RowIndex)

		if pos == (len(*docItems)-1) || docItem.DocId != (*docItems)[pos+1].DocId {
			go func(docId uint64, rowIndexList []uint32, result map[string]string) {
				if len(rowIndexList) == 0 {
					return
				}

				log.Printf("goroutine fetchDocMeta doc=%d rowIndex=%v", docId, rowIndexList)
				meta, err := self.fetchDocMeta(day, business, docId, result)
				var data, row []byte
				if err == nil {
					data, err = self.fetchDocData(business, meta)
				}

				errPrefix := ""
				if meta != nil {
					errPrefix = meta.Path + ":"
				}

				for _, rowIndex := range rowIndexList {
					row = ParseDocRow(data, rowIndex)
					if err == nil {
						ch <- DocDataResult{DocId: docId, RowIndex: uint32(rowIndex), Data: row, Err: nil}
					} else {
						ch <- DocDataResult{DocId: docId, RowIndex: uint32(rowIndex), Data: []byte(errPrefix + err.Error()), Err: err}
					}
				}
			}(docItem.DocId, rowIndexList, result)

			rowIndexList = []uint32{}
		}

	}

	for i := 0; i < len(*docItems); i++ {
		select {
		case result := <-ch:
			dataList = append(dataList, &result)
		}
	}

	sort.Sort(dataList)
	return dataList
}
