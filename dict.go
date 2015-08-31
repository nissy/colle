package main

import (
    "sync"
    "io/ioutil"
    "net/http"
    "unsafe"
    "time"
    "encoding/xml"
    "net/url"
    "strconv"
    "strings"
    "github.com/PuerkitoBio/goquery"
    "github.com/garyburd/redigo/redis"
    "golang.org/x/text/encoding/japanese"
    "golang.org/x/text/transform"
)

type DictItemRedis struct {
    Advertiser      string `redis:"advertiser"`
    Dict            string `redis:"dict"`
    AffiliateURL    string `redis:"affiliate_url"`
    AffiliateItemId string `redis:"affiliate_item_id"`
    ListImage       string `redis:"list_image"`
    Images          string `redis:"images"`
}

type ResponseDMM struct {
    XMLName    xml.Name  `xml:"response"`
    TotalCount int       `xml:"result>total_count"`
    Item       []ItemDMM `xml:"result>items>item"`
}

type ItemDMM struct {
    ProductId      string      `xml:"product_id"`
    AffiliateURL   string      `xml:"affiliateURL"`
    ImageURL       ImageURLDMM `xml:"imageURL"`
    SampleImageURL []string    `xml:"sampleImageURL>sample_s>image"`
}

type ImageURLDMM struct {
    List  string `xml:"list"`
    Small string `xml:"small"`
    Large string `xml:"large"`
}

func (dm *DataManager) SetDict(dictname string) {
    switch dictname {
        case "DMMR18ACT": dm.SetDictDmmR18Act(dictname)
    }
}

func (dm *DataManager) SetDictDmmR18Act(dictname string) {
    baseurl := "http://www.dmm.co.jp/digital/videoa/-/actress/=/keyword="
    keywords := []string{"a", "i", "u", "e", "o", "ka", "ki", "ku", "ke", "ko", "sa", "si", "su", "se", "so", "ta", "ti", "tu", "te", "to", "na", "ni", "ne", "no", "ha", "hi", "hu", "he", "ho", "ma", "mi", "mu", "me", "mo", "ya", "yu", "yo", "ra", "ri", "ru", "re", "ro", "wa"}
    var wg sync.WaitGroup
    for _, keyword := range keywords {
        wg.Add(1)
        go func(keyword string) {
            for i := 1;; i++ {
                url := baseurl + keyword + "/page=" + strconv.Itoa(i) + "/"
                doc, err := goquery.NewDocument(url)
                if err != nil {
                    break
                }
                s := doc.Find(".act-box").Each(func(_ int, s *goquery.Selection) {
                    s.Find("img").Each(func(_ int, s *goquery.Selection) {
                        actname, _ := s.Attr("alt")
                        actimage, _ := s.Attr("src")
                        response, err := GetDmmAffiliate(dm.UserConfig.Dict.DMMR18ACT.ApiId, dm.UserConfig.Dict.DMMR18ACT.AffiliateId, doDmmEncoding(actname))
                        if err != nil {
                            return
                        }
                        con := dm.Get()
                        con.Do("SADD", REDISKEY_DICT_EXISTS, actname)
                        i := DictItemRedis{}
                        i.Advertiser = "DMM"
                        i.Dict = dictname
                        if len(response.Item) > 0 {
                            i.ListImage       = actimage
                            i.AffiliateItemId = response.Item[0].ProductId
                            i.AffiliateURL    = response.Item[0].AffiliateURL
                            i.Images          = strings.Join(response.Item[0].SampleImageURL, "\n")
                        }
                        con.Do("HMSET", redis.Args{REDISKEY_DICT_ITEM_PREFIX + actname}.AddFlat(i)...)
                    })
                })
                if len(strings.Replace(s.Text(), "\n", "", -1)) == 0 {
                    break
                }
            }
            wg.Done()
        }(keyword)
    }
    wg.Wait()
}

func GetDmmAffiliate(apiId string, affiliateId string, keyword string) (ResponseDMM, error) {
    values := url.Values{}
    values.Add("api_id", apiId)
    values.Add("affiliate_id", affiliateId)
    values.Add("operation", "ItemList")
    values.Add("version", "2.00")
    values.Add("timestamp", time.Now().Format("2006-01-02 15:04:05"))
    values.Add("site", "DMM.co.jp")
    values.Add("service", "digital")
    values.Add("floor", "videoa")
    values.Add("hits", "1")
    values.Add("sort", "review")
    values.Add("keyword", keyword)
    response, err := http.Get("http://affiliate-api.dmm.com/?" + values.Encode())
    var r ResponseDMM
    if err != nil {
        return r, err
    }
    defer response.Body.Close()
    body, err := ioutil.ReadAll(transform.NewReader(response.Body, japanese.EUCJP.NewDecoder()))
    contents := strings.Replace(string(body), "euc-jp", "UTF-8", -1)
    xml.Unmarshal(*(*[]byte)(unsafe.Pointer(&contents)), &r)
    return r, err
}

func doDmmEncoding(text string) string {
    ret, _ := ioutil.ReadAll(transform.NewReader(strings.NewReader(text), japanese.EUCJP.NewEncoder()))
    return string(ret)
}
