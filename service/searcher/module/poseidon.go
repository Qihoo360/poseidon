package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Qihoo360/poseidon/service/searcher/poseidon"
	"github.com/zieckey/simgo"
	"io"
	"log"
	"net/http"
	"time"
)

var indexClient = poseidon.InvertedIndexGzClient{}
var docClient = poseidon.DocGzClient{}

type SearchCond struct {
	day      string
	business string

	keywords map[string]string
}

func (self *SearchCond) KeywordsJson() string {
	jsonStr := "{"
	count := 0
	for k, v := range self.keywords {
		if count > 0 {
			jsonStr += ",\r\n"
		}
		escapedK, _ := json.Marshal(k)
		escapedV, _ := json.Marshal(v)
		jsonStr += fmt.Sprintf(`%s:%s`, escapedK, escapedV)
		count++
	}
	jsonStr += "}"

	return jsonStr
}

type SearchRequest struct {
	pageSize    int
	pageNumber  int
	progressKey string
	pvOnly      int
	filter      string
	cond        SearchCond
}

func GetCookie(r *http.Request, name string) string {
	log.Println(r.Cookies())
	ck, err := r.Cookie(name)
	if err != nil {
		return ""
	}

	return ck.Value
}

func ParseReqBody(r *http.Request) (searchReq *SearchRequest, err error) {
	searchReq = &SearchRequest{}
	decoder := json.NewDecoder(r.Body)
	var reqJson map[string]interface{}
	err = decoder.Decode(&reqJson)
	if err != nil {
		return nil, err
	}
	reqJson, ok := reqJson["query"].(map[string]interface{})
	if !ok {
		return nil, errors.New("query required")
	}

	searchReq.progressKey, _ = reqJson["progress_key"].(string)

	searchReq.pageSize = int(reqJson["page_size"].(float64))
	searchReq.pageNumber = int(reqJson["page_number"].(float64))

	if options, ok := reqJson["options"].(map[string]interface{}); ok {
		for k, v := range options {
			if k == "pv_only" {
				searchReq.pvOnly = int(v.(float64))
			} else if k == "filter" {
				searchReq.filter = v.(string)
			}
		}
	}

	log.Printf("request filter = %s", searchReq.filter)

	searchReq.cond.day = reqJson["day"].(string)
	searchReq.cond.business = reqJson["business"].(string)

	searchReq.cond.keywords = make(map[string]string)

	keywords, ok := reqJson["keywords"].(map[string]interface{})
	if !ok {
		return nil, errors.New("keywords required")
	}
	for k, v := range keywords {
		searchReq.cond.keywords[k] = v.(string)
	}

	return searchReq, nil

}

func SearchDocItems(req *SearchRequest) (total, pv, uv int, docs poseidon.DocItemList, err error) {
	// 1,2. fetch InvertedIndex Meat & DocIdList
	indexDataList := indexClient.FetchIndex(req.cond.day, req.cond.business, req.cond.keywords)

	tokensDocItemList := []poseidon.DocItemList{}
	for _, indexData := range indexDataList {
		if indexData.Err != nil {
			log.Printf("SearchDocItems token %s FetchIndex err=%v", indexData.Token, indexData.Err)
			return 0, 0, 0, nil, indexData.Err
		}
		log.Printf("SearchDocItems token %s FetchIndex ok,docListSize=%d", indexData.Token, len(indexData.DocItems))
		tokensDocItemList = append(tokensDocItemList, indexData.DocItems)
	}

	// step 3. filter docId list
	commonDocItems := docClient.DocIdIntersect(&tokensDocItemList)

	if len(indexDataList) == 1 {
		pv = indexDataList[0].Pv
		uv = indexDataList[0].Uv
	} else {
		pv = len(commonDocItems)
		uv = len(commonDocItems)
	}

	// step 4,5 page id list
	minIndex := req.pageNumber * req.pageSize
	if minIndex > len(commonDocItems) {
		minIndex = len(commonDocItems)
	}
	maxIndex := (req.pageNumber + 10) * req.pageSize // 返回10页ID列表，因为有过滤的情况
	if maxIndex > len(commonDocItems) {
		maxIndex = len(commonDocItems)
	}

	return len(commonDocItems), pv, uv, commonDocItems[minIndex:maxIndex], nil
}

const MAX_CONCURRENCY = 64

func GetEndOffset(docItems poseidon.DocItemList, begin int, maxConcurrency int) int {
	concurrency := 0
	prevDocId := uint64(0)
	pos := begin
	for ; pos < len(docItems); pos++ {
		if docItems[pos].DocId == prevDocId {
			continue
		}
		prevDocId = docItems[pos].DocId
		concurrency++
		if concurrency >= maxConcurrency {
			break
		}
	}

	return pos
}

func GetPreviousDay() string {
	day := time.Now().AddDate(0, 0, -2)
	return fmt.Sprintf("%4d-%02d-%02d", day.Year(), day.Month(), day.Day())
}

func DoSearch(req *SearchRequest) (total, pv, uv int, result []*poseidon.DocDataResult, err error) {
	total, pv, uv, pageDocItems, err := SearchDocItems(req) // 取docId列表

	if err != nil {
		return 0, 0, 0, nil, err
	}

	docDataList := poseidon.DocDataResultList{}

	if req.pvOnly == 0 {
		end := 0
		for end < len(pageDocItems) {
			begin := end
			end = GetEndOffset(pageDocItems, begin, MAX_CONCURRENCY)

			itemSlice := pageDocItems[begin:end]
			docSlice := docClient.FetchDocItems(req.cond.day, req.cond.business, &itemSlice) // 根据id列表取文档内容

			if len(req.filter) > 0 && len(docSlice) > 0 {
				if err == nil {
					log.Printf("cloudRuleFilter ok, input=%d output=%d", len(docSlice), len(docDataList))
					docSlice = docDataList
				} else {
					log.Printf("cloudRuleFilter err %v", err)
					return 0, 0, 0, nil, err
				}
			}
			docDataList = append(docDataList, docSlice...)
			if len(docDataList) >= req.pageSize {
				docDataList = docDataList[:req.pageSize]
				break
			}
		}


		if len(req.filter) > 0 && len(docDataList) > 0 {
			if err == nil {
				log.Printf("cloudRuleFilter ok, input=%d output=%d", len(docDataList), len(docDataList))
				docDataList = docDataList
			} else {
				log.Printf("cloudRuleFilter err %v", err)
				return 0, 0, 0, nil, err
			}
		}
	}
	return total, pv, uv, docDataList, nil
}

func (s *Searcher) handleSearch(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err == nil {
			log.Println(err)
		}
	}()
	w.Header().Set("Content-Type", "application/json")

	searchReq, err := ParseReqBody(r)
	if err != nil {
		panic(err)
	}

	day := searchReq.cond.day
	total, pv, uv, searchResult, err := DoSearch(searchReq)

	log.Printf("---- %s handleSearch %v, err=%v", r.RemoteAddr, searchReq, err)

	if err != nil {
		panic(err)
	}
	respBody := fmt.Sprintf(
		`{"code":0,"err":"OK",
			"day":"%s",
			"keywords":%s,
			"hits":
			{"total":%d,
			"pv":%d,
			"uv":%d,
			"page_number":%d,
			"page_size":%d,
			"hits": [`,
		day, searchReq.cond.KeywordsJson(), total, pv, uv, searchReq.pageNumber, searchReq.pageSize)


	io.WriteString(w, respBody)

	for i, item := range searchResult {
		var respItem string
		if i > 0 {
			respItem = ","
		}
		// jsonItem, _ := json.Marshal(item)
		// item.FilterColumns(poseidon.LogDescriptorInstance().GetSeperator(searchReq.cond.business), fieldColumns)
		respItem = respItem + item.ToJson(searchReq.cond.business)
		io.WriteString(w, respItem)
		io.WriteString(w, "\r\n")
	}

	io.WriteString(w, "]}}\r\n\r\n")

	//log.Printf("Response Done %s", respBody)

}

/**
 * lvs 健康检查
 * @return {[type]} [description]
 */
func handlelvshealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

type Searcher struct {
}

func New() *Searcher {
	return &Searcher{}
}

func (s *Searcher) Initialize() error {
	poseidon.SimgoFramework = simgo.DefaultFramework

	simgo.HandleFunc("/service/search", s.handleSearch, s).Methods("POST")
	return nil
}

func (s *Searcher) Uninitialize() error {
	return nil
}
