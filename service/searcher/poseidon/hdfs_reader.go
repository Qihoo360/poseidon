package poseidon

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"time"
)

type HDFSReader struct{}

var hdfsReader = HDFSReader{}

func (self *HDFSReader) httpGet(url string, domain string, timeout int) (body []byte, err error) {
	duration := time.Duration(timeout * int(time.Millisecond))
	client := http.Client{
		Timeout: duration,
	}

	req, err := http.NewRequest("GET", url, nil)
	if len(domain) > 0 {
		req.Host = domain
	}
	resp, err := client.Do(req)

	if err != nil {
		log.Printf("httpGet error %s,url=%s", err.Error(), url)
		return nil, err
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("httpGet read body error %s,url=%s", err.Error(), url)
		return nil, err
	}
	// log.Printf("httpGet url=%s code=%d,body_len=%d", url, resp.StatusCode, len(body))
	return body, nil
}

func (self *HDFSReader) Read(filePath string, offset int64, length int64) (data []byte, err error) {
	escapedFilePath, _ := neturl.QueryUnescape(filePath)
	url := fmt.Sprintf("http://"+self.getHdfs()+"/read-hdfs?path=%s&offset=%d&length=%d",
		escapedFilePath, offset, length)
	data, err = self.httpGet(url, "", 30000)
	// str := base64.StdEncoding.EncodeToString(data)
	log.Printf("HDFSReader Read ok,len=%d\r\n", len(data))
	return data, err
}

func (self *HDFSReader) ReadZip(filePath string, offset int64, length int64) (data []byte, err error) {
	escapedFilePath, _ := neturl.QueryUnescape(filePath)
	url := fmt.Sprintf("http://"+self.getHdfs()+"/read-hdfs?path=%s&offset=%d&length=%d",
		escapedFilePath, offset, length)
	log.Printf("ReadZip url=%s", url)
	gzData, err := self.httpGet(url, "", 30000)
	if err != nil {
		return nil, err
	}

	gzReader, err := gzip.NewReader(bytes.NewBuffer(gzData))
	if err != nil {
		log.Printf("unzip err : len=%d", len(gzData))
		return nil, err
	}
	defer gzReader.Close()
	b, err := ioutil.ReadAll(gzReader)

	return b, err
}

func (self *HDFSReader) getHdfs() string {
	hdfspath, _ := SimgoFramework.Conf.SectionGet("searcher", "hdfs")
	return hdfspath
}
