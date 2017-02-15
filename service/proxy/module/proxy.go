package module

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/zieckey/simgo"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"strings"
	"strconv"
	"reflect"
	"unsafe"
	"encoding/base64"
	"fmt"
)

var (
	ErrConvert = errors.New("convert error")
	mu         sync.Mutex
)

//解析查询参数
type SearchRequestParams struct {
	Days        []interface{}          `json:"days"`
	Page_size   int32                  `json:"page_size"`
	Page_number int32                  `json:"page_number"`
	Day         string                 `json:"day"`
	Business    string                 `json:"business"`
	Keywords    map[string]interface{} `json:"keywords"`
	Options     map[string]interface{} `json:"options"`
	//	Filters     map[string]interface{} `json:"filters"`
}

func NewSearchRequestParams() *SearchRequestParams {
	return &SearchRequestParams{
		Days:     make([]interface{}, 0),
		Keywords: make(map[string]interface{}),
		Options:  make(map[string]interface{}),
		//		Filters:  make(map[string]interface{}),
	}
}

type QueryBody struct {
	Query *SearchRequestParams `json:"query"`
}

func NewQuery() *QueryBody {
	return &QueryBody{
		Query: NewSearchRequestParams(),
	}
}

type Proxy struct {
	poseidon_search_url string
	meta_service string
	Sqp                 *SearchRequestParams
}

func New() *Proxy {
	return &Proxy{
		Sqp: NewSearchRequestParams(),
	}
}

func (p *Proxy) Initialize() error {
	fw := simgo.DefaultFramework
	p.poseidon_search_url, _ = fw.Conf.SectionGet("proxy", "poseidon_search_url")
	p.meta_service, _ = fw.Conf.SectionGet("proxy", "meta_service")

	simgo.HandleFunc("/service/proxy/mdsearch", p.MdsearchAction, p).Methods("POST")
	simgo.HandleFunc("/service/proxy/stat", p.Stat, p).Methods("POST")
	p.Sqp = NewSearchRequestParams()
	return nil
}

func (p *Proxy) Uninitialize() error {
	return nil
}

/**
 * multi day search
 * @param  {[type]} this *SearchController) MdsearchAction( [description]
 * @return {[type]}      [description]
 */
func (p *Proxy) MdsearchAction(w http.ResponseWriter, r *http.Request) {
	days, err := p.GetDays(r)
	if err != nil {
		panic(err)
	}
	tasknum := len(days)
	log.Println("tasknum:", tasknum, days)
	log.Println(days)
	//init result channel container
	c := make(chan string, tasknum)

	for _, day := range days {
		if day == "" {
			continue
		}
		go p.send(day, c)
	}

	//recieve result
	//var response_num string
	buf := bytes.NewBuffer([]byte("["))
	for i := 0; i < tasknum; i++ {
		chanr := <-c
		buf.WriteString(chanr)
		if i != (tasknum - 1) {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]")
	w.Write(buf.Bytes())
}


func (p *Proxy) Stat(w http.ResponseWriter, r *http.Request) {
	params, err := p.getparams(r, true)

	if err != nil {
		panic(err)
	}

	business := params.Business
	types := [6]string{"docNum", "docSize", "docSizeCompressed", "indexSizeCompressed",
		"docidSize", "indexMiddleSize"}
	keys := make([]string, 6 * len(params.Days))
	for i, day := range params.Days {
		newday, _ := day.(string)
		newday = strings.Replace(newday, "-", "", -1)
		for j, t := range types {
			keys[i * 6 + j] = "stat_" + business + "_" + newday + "_" + t
		}
	}

	postStr := strings.Join(keys, "\n")

	sh := (*reflect.StringHeader)(unsafe.Pointer(&postStr))
	bh := reflect.SliceHeader{sh.Data, sh.Len, 0}
	postBytes := *(*[]byte)(unsafe.Pointer(&bh))

	body := bytes.NewBuffer(postBytes)
	url := "http://" + p.meta_service + "/service/meta/" + business + "/index/get"
	req, err := http.NewRequest("POST", url, body)
	log.Println("send url ", url, postStr)
	if err != nil {
		log.Printf("send url %s httpNewErr=%v", url, err)
		panic(err)
	}

	req.Header.Set("Content-Type", "text/plain")
	//req.Header.Add("Accept-Encoding", "identity")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil  {
		log.Printf("send url %s doClientErr=%v", url, err)
		panic(err)
	}

	defer resp.Body.Close()

	re, err := ioutil.ReadAll(resp.Body)


	buf := bytes.NewBuffer([]byte("{"))
	for i, line := range strings.Split(string(re), "\n") {
		tokens := strings.Split(line, "\t")
		if(len(tokens) != 2) {
			continue
		}
		var val int64
		val = 0
		if(len(tokens[1]) > 0) {
			valBytes, _ := base64.StdEncoding.DecodeString(tokens[1])
			val, _ = strconv.ParseInt(string(valBytes), 10, 64)
		}
		keyTokens := strings.Split(tokens[0], "_")
		keyLen := len(keyTokens)
		curT := keyTokens[keyLen - 1]
		curDay := keyTokens[keyLen - 2]
		if(i > 0) {
			buf.WriteString(",\n")
		}
		str2 := fmt.Sprintf("%d", val)
		buf.WriteString("\"" + curDay + "_" + curT + "\": " + str2 )
	}

	buf.WriteString("}")
	w.Write(buf.Bytes())
}


/**
 * send request put data into channel
 * @param  {[type]} this *SearchController) send(day string, c chan string [description]
 * @return {[type]}      [description]
 */
func (p *Proxy) send(day string, c chan string) {
	defer func() {
		if err := recover(); err != nil {
			c <- "request timeout"
		}
	}()
	b, _ := p.GetPostBody(day)
	body := bytes.NewBuffer(b)
	req, err := http.NewRequest("POST", p.poseidon_search_url, body)
	log.Println("send url ", p.poseidon_search_url, body)
	if err != nil {
		log.Printf("send url %s httpNewErr=%v", p.poseidon_search_url, err)
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")
	//req.Header.Add("Accept-Encoding", "identity")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil  {
		log.Printf("send url %s doClientErr=%v", p.poseidon_search_url, err)
		panic(err)
	}

	defer resp.Body.Close()

	re, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("send url %s readAllErr=%v", p.poseidon_search_url, err)
		panic(err)
	}

	c <- string(re)
}

func (p *Proxy) getparams(r *http.Request, isSearch bool) (*SearchRequestParams, error) {
	jsonenc := json.NewDecoder(r.Body)
	searchParams := make(map[string]interface{}, 1000)
	err := jsonenc.Decode(&searchParams)
	if err != nil {
		return nil, err
	}

	query, ok := searchParams["query"].(map[string]interface{})
	if !ok {
		return nil, ErrConvert
	}

	if(isSearch) {
		p.Sqp.Page_size = int32(query["page_size"].(float64))
		p.Sqp.Page_number = int32(query["page_number"].(float64))
		p.Sqp.Keywords = query["keywords"].(map[string]interface{})

		p.Sqp.Options = query["options"].(map[string]interface{})
		//Sqp.Filters = query["filters"].(map[string]interface{})
	}

	p.Sqp.Business = query["business"].(string)

	if query["day"] != nil {
		p.Sqp.Day = query["day"].(string)
	}
	if query["days"] != nil {
		p.Sqp.Days = query["days"].([]interface{})
	}

	return p.Sqp, nil
}

func (p *Proxy) GetDays(r *http.Request) ([]string, error) {
	params, err := p.getparams(r, true)
	if err != nil {
		return nil, err
	}

	//初始化新容器，断离params大对象
	days := make([]string, len(params.Days))
	for i, day := range params.Days {
		if newday, ok := day.(string); ok {
			days[i] = newday
		}
	}

	return days, nil
}

func (p *Proxy) GetPostBody(day string) ([]byte, error) {
	mu.Lock()
	defer mu.Unlock()
	p.Sqp.Day = day
	query := NewQuery()
	query.Query = p.Sqp
	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	return body, nil
}
