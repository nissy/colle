package main

import (
    "strings"
    "fmt"
    "io/ioutil"
    "sync"
    "strconv"
    "time"
    "github.com/garyburd/redigo/redis"
    "github.com/flosch/pongo2"
    "github.com/Sirupsen/logrus"
)

type DataManager struct {
    *redis.Pool
    *UserConfig
    *logrus.Logger
    ExecDir string
}

type ItemRedis struct {
    Id              int    `redis:"id"`
    FeedTitle       string `redis:"feed_title"`
    FeedLink        string `redis:"feed_link"`
    Title           string `redis:"title"`
    ImageLink       string `redis:"image_link"`
    PubDate         string `redis:"pub_date"`
    MatchingWord    string `redis:"matching_word"`
    Content         string `redis:"content"`
    Link            string `redis:"link"`
    OutLinkCnt      int    `redis:"outlink_cnt"`
    InLinkCnt       int    `redis:"inlink_cnt"`
    Category        string `redis:"category"`
    AffiliateURL    string `redis:"affiliate_url"`
    AffiliateItemId string `redis:"affiliate_item_id"`
    ListImage       string `redis:"list_image"`
    Images          string `redis:"images"`
}

type Item struct {
    ItemRedis
    PubDateTime     time.Time
    AffiliateImages []string
}

const (
    REDISKEY_FEED_EXISTS           = "feed:exists"
    REDISKEY_FEED_TIME             = "feed:time"
    REDISKEY_FEED_TIME_PREFIX      = "feed:time:"
    REDISKEY_FEED_ITEM_PREFIX      = "feed:item:"
    REDISKEY_FEED_RANK_PREFIX      = "feed:rank:"
    REDISKEY_FEED_RANK_DAYS_PREFIX = "feed:rank:days:"
    REDISKEY_DICT_EXISTS           = "dict:exists"
    REDISKEY_DICT_ITEM_PREFIX      = "dict:item:"
)

func NewRedisPool(protocol string, server string) *redis.Pool {
    return &redis.Pool {
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial(protocol, server)
            if err != nil {
                return nil, err
            }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

func NewDataManager(userconf *UserConfig, execdir string) *DataManager {
    loggerfilename := execdir + LOGFILE
    if len(userconf.Site.Log) > 0 {
        loggerfilename = userconf.Site.Log
    }
    return &DataManager{NewRedisPool(userconf.Redis.Protocol, userconf.Redis.Server), userconf, NewLogger(loggerfilename), execdir}
}

// "overriding" the Get method
func (dm *DataManager)Get() redis.Conn {
    conn := dm.Pool.Get()
    conn.Do("SELECT", dm.UserConfig.Redis.DatabaseNo)
    return conn
}

func (dm *DataManager) SetFeed(channel []Channel) {
    var dict []string
    for _, v := range channel {
        if v.IsDict {
            dict = dm.GetDict(REDISKEY_DICT_EXISTS)
            break
        }
    }
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        for _, v := range channel {
            feed := GetFeed(v.Url).ResponseData.Feed
            for _, entrie := range feed.Entries {
                if !dm.IsItemExists(entrie.Link) {
                    item := ItemRedis{}
                    if v.IsDict {
                        word := GetMatchingWord(entrie.Title, dict)
                        if word == nil {
                            continue
                        }
                        item.MatchingWord = word.(string)
                        dictDetail := dm.GetDictDetail(REDISKEY_DICT_ITEM_PREFIX + word.(string))
                        item.AffiliateURL = dictDetail.AffiliateURL
                        item.AffiliateItemId = dictDetail.AffiliateItemId
                        item.ListImage       = dictDetail.ListImage
                        item.Images          = dictDetail.Images
                    }
                    item.FeedTitle  = feed.Title
                    item.FeedLink   = feed.Link
                    item.Title      = entrie.Title
                    item.ImageLink  = GetImageLink(entrie.Content)
                    item.PubDate    = entrie.PublishedDate
                    item.Content    = entrie.ContentSnippet
                    item.Link       = entrie.Link
                    item.Category   = v.Category
                    item.OutLinkCnt = 0
                    item.InLinkCnt  = 0
                    dm.SetItem(item)
                }
            }
        }
        wg.Done()
    }()
    wg.Wait()
    dm.Logger.WithFields(SetUpdateLog("feed")).Info("update items")
}

func (dm *DataManager) GetDictDetail(key string) DictItemRedis {
    values, _ := redis.Values(dm.Get().Do("HGETALL", key))
    dictItem := DictItemRedis{}
    redis.ScanStruct(values, &dictItem)
    return dictItem
}

func (dm *DataManager) SetItem(i ItemRedis) {
    con  := dm.Get()
    i.Id  = dm.GetNewItemId()
    time := GetFeedDateTime(i.PubDate)
    itemkeyname := REDISKEY_FEED_ITEM_PREFIX + strconv.Itoa(i.Id)
    con.Send("MULTI")
    con.Send("SADD", REDISKEY_FEED_EXISTS, i.Link)
    con.Send("ZADD", REDISKEY_FEED_TIME, time.Format(GetDateTimeFormat()), itemkeyname)
    if len(i.Category) > 0 {
        con.Send("ZADD", REDISKEY_FEED_TIME_PREFIX + i.Category, time.Format(GetDateTimeFormat()), itemkeyname)
    }
    con.Send("HMSET", redis.Args{itemkeyname}.AddFlat(i)...)
    con.Send("EXPIREAT", itemkeyname, time.AddDate(0, 0, dm.UserConfig.Site.ItemExpire).Unix())
    con.Do("EXEC")
}

func (dm *DataManager) SetOutLinkIncrement(keyname string) {
    con := dm.Get()
    con.Do("ZINCRBY", REDISKEY_FEED_RANK_PREFIX + time.Now().Format(GetDateFormat()), 1, keyname)
    con.Do("HINCRBY", keyname, "outlink_cnt", 1)
}

func (dm *DataManager) SetInLinkIncrement(keyname string) {
    con := dm.Get()
    con.Do("HINCRBY", keyname, "inlink_cnt", 1)
}

func (dm *DataManager) IsItemExists(link string) bool {
    con := dm.Get()
    result, err := redis.Int(con.Do("SISMEMBER", REDISKEY_FEED_EXISTS, link))
    if err != nil {
        fmt.Println(err)
    }
    return result == 1
}

func (dm *DataManager) IsKeyExists(keyname string) bool {
    con := dm.Get()
    result, err := redis.Int(con.Do("EXISTS", keyname))
    if err != nil {
        fmt.Println(err)
    }
    return result == 1
}

func (dm *DataManager) GetNewItemId() int {
    con := dm.Get()
    result, err := redis.Int(con.Do("SCARD", REDISKEY_FEED_EXISTS))
    if err != nil {
        fmt.Println(err)
    }
    return result + 1
}

func (dm *DataManager) GetDict(keyname string) []string {
    con := dm.Get()
    result, _ := redis.Strings(con.Do("SMEMBERS", keyname))
    return result
}

func (dm *DataManager) GetNewFeedItem(keyname string, min string, max string, offset int, count int) []Item {
    con := dm.Get()
    strings, _ := redis.Strings(con.Do("ZREVRANGEBYSCORE", keyname, max, min, "limit", offset, count))
    return dm.GetItems(strings)
}

func (dm *DataManager) GetRankFeedItem(keyname string, min int, max int) []Item {
    con := dm.Get()
    strings, _ := redis.Strings(con.Do("ZREVRANGE", keyname, min, max))
    return dm.GetItems(strings)
}

func (dm *DataManager) GetItems(itemlist []string) []Item {
    var result []Item
    for _, keyname := range itemlist {
        item := dm.GetItem(keyname)
        result = append(result, item)
    }
    return result
}

func (dm *DataManager) GetItem(keyname string) Item {
    con := dm.Get()
    values, _ := redis.Values(con.Do("HGETALL", keyname))
    itemRedis := ItemRedis{}
    redis.ScanStruct(values, &itemRedis)
    datetime, _ := time.Parse(time.RFC1123Z, itemRedis.PubDate)
    var images []string
    if itemRedis.Images != "" {
        images = strings.Split(itemRedis.Images, "\n")
    }
    return Item{itemRedis, datetime, images}
}

func (dm *DataManager) SetRankRange(days []string, category string) {
    con := dm.Get()
    var args []interface{}
    var cargs []interface{}
    allrankkey := GetRankDaysKeyname(len(days), "")
    categoryrankkey := GetRankDaysKeyname(len(days), category)
    keycnt := strconv.Itoa(len(days))
    for _, v := range append([]string{allrankkey, keycnt}, days...) {
        args = append(args, interface{}(v))
    }
    con.Do("ZUNIONSTORE", args...)
    if category != "" {
        cargs = append(cargs, interface{}(categoryrankkey))
        cargs = append(cargs, interface{}("2"))
        cargs = append(cargs, interface{}(REDISKEY_FEED_TIME_PREFIX + category))
        cargs = append(cargs, interface{}(allrankkey))
        cargs = append(cargs, interface{}("WEIGHTS"))
        cargs = append(cargs, interface{}("0"))
        cargs = append(cargs, interface{}("1"))
        con.Do("ZINTERSTORE", cargs...)
    }
}

func (dm *DataManager) WriteRssFile(filename string, pctx pongo2.Context) {
    tpl, _ := pongo2.FromFile("rss2.j2")
    pctx["lastBuildDate"] = time.Now().Format(time.RFC1123)
    context, _ := tpl.ExecuteBytes(pctx)
    ioutil.WriteFile(filename, context, 0644)
    dm.Logger.WithFields(SetUpdateLog("rss")).Info("write file " + filename)
}

func (dm *DataManager) GetPageFeedItem(num int, category string, days int, count int) []Item {
    keyname := REDISKEY_FEED_TIME
    if category != "" {
        keyname = REDISKEY_FEED_TIME_PREFIX + category
    }
    daymin, daymax := GetDateTimeMinMax((days * -1), 0, GetDateTimeFormat())
    offset := count * (num - 1)
    return dm.GetNewFeedItem(keyname, daymin, daymax, offset, count)
}

func (dm *DataManager) GetPageFeedRankItem(num int, category string, days int, count int) []Item {
    dm.SetRankRange(GetDateTimeRange(((days -1) * -1), 0), category)
    rankmin := (num - 1) * count
    rankmax := rankmin + count - 1
    return dm.GetRankFeedItem(GetRankDaysKeyname(days, category), rankmin, rankmax)
}

func (dm *DataManager) GetCategoryItem(items []Item, category string, count int) []Item {
    var result []Item
    for _, item := range items {
        if item.Category == category {
            result = append(result, item)
        }
    }
    return result
}

func (dm *DataManager) SetData(key string) DictItemRedis {
    values, _ := redis.Values(dm.Get().Do("HGETALL", key))
    dictItem := DictItemRedis{}
    redis.ScanStruct(values, &dictItem)
    return dictItem
}
