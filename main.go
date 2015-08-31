package main

import (
    "fmt"
    "encoding/json"
    "io/ioutil"
    "strings"
    "flag"
    "net/http"
    "regexp"
    "time"
    "runtime"
    "net/url"
    "os"
    "path/filepath"
    "github.com/flosch/pongo2"
    "github.com/zenazn/goji"
    "github.com/zenazn/goji/web"
    "github.com/zenazn/goji/web/middleware"
    "github.com/jessevdk/go-flags"
    "github.com/Sirupsen/logrus"
)

type ResponseData struct {
    ResponseData    Feeds  `json:"responseData"`
    ResponseDetails string `json:"responseDetails"`
    ResponseStatus  int    `json:"responseStatus"`
}

type Feeds struct {
    Feed Feed `json:"feed"`
}

type Feed struct {
    FeedUrl     string   `json:"feedUrl"`
    Title       string   `json:"title"`
    Link        string   `json:"link"`
    Author      string   `json:"author"`
    Description string   `json:"description"`
    Type        string   `json:"type"`
    Entries     []Entrie `json:"entries"`
}

type Entrie struct {
    Title          string   `json:"title"`
    Link           string   `json:"link"`
    Author         string   `json:"author"`
    PublishedDate  string   `json:"publishedDate"`
    ContentSnippet string   `json:"contentSnippet"`
    Content        string   `json:"content"`
    Categories     []string `json:"categories"`
}

type UserConfig struct {
    Site  ConfigSite  `json:"site"`
    Redis ConfigRedis `json:"redis"`
    Feed  ConfigFeed  `json:"feed"`
    Dict  ConfigDict  `json:"dict"`
}

type ConfigSite struct {
    Title             string  `json:"title"`
    Url               string  `json:"url"`
    Mailaddress       string  `json:"mailaddress"`
    ListenPort        string  `json:"listenPort"`
    Log               string  `json:"log"`
    TemplateDir       string  `json:"templateDir"`
    AssetsDir         string  `json:"assetsDir"`
    ItemDays          int     `json:"itemDays"`
    ItemExpire        int     `json:"itemExpire"`
    PageNewItemCount  int     `json:"pageNewItemCount"`
    PageRankItemCount int     `json:"pageRankItemCount"`
    Service           Service `json:"service"`
}

type Service struct {
    GoogleAnalyticsTrackingId string `json:"googleAnalyticsTrackingId"`
}

type ConfigRedis struct {
    Protocol   string `json:"protocol"`
    Server     string `json:"server"`
    DatabaseNo int    `json:"databaseNo"`
}

type ConfigFeed struct {
    Category []ChannelCategory `json:"category"`
    Channel  []Channel         `json:"channel"`
}

type ChannelCategory struct {
    Dir   string `json:"dir"`
    Label string `json:"label"`
}

type Channel struct {
    Url      string `json:"url"`
    Category string `json:"category"`
    IsDict   bool   `json:"isDict"`
}

type ConfigDict struct {
    Use       []string  `json:"use"`
    DMMR18ACT DMMR18ACT `json:"DMMR18ACT"`
}

type DMMR18ACT struct {
    ApiId       string `json:"apiId"`
    AffiliateId string `json:"affiliateId"`
}

type CommandlineOptions struct {
    Version bool   `short:"v" long:"version" description:"Show program's version number"`
    Update  string `short:"u" long:"update"  description:"Update items / feed, dict"`
}

const (
    CONFIGFILE = "config.json"
    LOGFILE    = "app.log"
    VERSION    = "1.0"
)

var userconf   UserConfig
var configfile string
var cmdopt     CommandlineOptions

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    execdir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    execdir += "/"
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    parser := flags.NewParser(&cmdopt, flags.Default)
    parser.Name = "colle"
    parser.Usage = "[-u] [-v] 'Use config file'"
    args, err := parser.Parse()
    if err != nil {
        os.Exit(0)
    }

    if cmdopt.Version {
        fmt.Print("Version " + VERSION)
        os.Exit(0)
    }

    configfile = execdir + CONFIGFILE
    if len(args) > 0 {
        configfile = args[0]
    }

    userconf = NewUserConfig(configfile)
    dm := NewDataManager(&userconf, execdir)
    if dm.Get().Err() != nil {
        fmt.Println(dm.Get().Err().Error())
        os.Exit(1)
    }

    defer dm.Close()

    templateDir := dm.UserConfig.Site.TemplateDir
    if len(templateDir) == 0 {
        templateDir = execdir + "template"
    }
    pongo2.DefaultSet.SetBaseDirectory(templateDir)
    pongo2.Globals.Update(pongo2.Context{"CONFIG": dm.UserConfig})

    if len(cmdopt.Update) > 0 {
        switch cmdopt.Update {
            case "feed":
                dm.SetFeed(userconf.Feed.Channel)
            case "dict":
                for _, v := range userconf.Dict.Use {
                    dm.SetDict(v)
                }
        }
        os.Exit(0)
    }

    cntr := NewController(dm)
    goji.Get("/", cntr.Root)
    goji.Get("/:category/", cntr.Root)
    goji.Get("/feed", cntr.NewFeed)
    goji.Get("/:category/feed", cntr.NewFeed)

    assetsDir := dm.UserConfig.Site.AssetsDir
    if len(assetsDir) == 0 {
        assetsDir = execdir + "assets"
    }
    goji.Get("/*", http.FileServer(http.Dir(assetsDir)))

    api := web.New()
    goji.Handle("/api/*", api)
    api.Use(middleware.SubRouter)
    api.Post("/outlink/:id", cntr.ApiOutLink)

    flag.Set("bind", ":" + userconf.Site.ListenPort)
    goji.Serve()
}

func GetFeed(channel string) ResponseData {
    values := url.Values{}
    values.Add("num", "30")
    values.Add("v", "1.0")
    values.Add("q", channel)
    response, err := http.Get("http://ajax.googleapis.com/ajax/services/feed/load?" + values.Encode())
    if err != nil {
        fmt.Println(err)
    }
    defer response.Body.Close()
    contents, err := ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println(err)
    }
    var r ResponseData
    json.Unmarshal(contents, &r)
    return r
}

func GetMatchingWord(text string, dict []string) interface{} {
    for _, v := range dict {
        if strings.Contains(text, v) {
            return v
        }
    }
    return nil
}

func GetDateTimeRange(min int, max int) []string {
    t := time.Now().AddDate(0, 0, max)
    var result []string
    for i := min; i <= 0; i++ {
        result = append(result, REDISKEY_FEED_RANK_PREFIX + t.AddDate(0, 0, i).Format(GetDateFormat()))
    }
    return result
}

func GetDateTimeMinMax(min int, max int, format string) (string, string) {
    t := time.Now()
    daymin := t.AddDate(0, 0, min).Format(format)
    daymax := t.AddDate(0, 0, max).Format(format)
    return daymin, daymax
}

func GetFeedDateTime(datetime string) time.Time {
    t, _ := time.Parse(time.RFC1123Z, datetime)
    return t
}

func GetDateTimeFormat() string {
    return "20060102150405"
}

func GetDateFormat() string {
    return "20060102"
}

func GetImageLink(text string) string {
    r := regexp.MustCompile(`(https?)(:\/\/[-_.!~*\'()a-zA-Z0-9;\/?:\@&=+\$,%#]+)\.(jpg|jpeg)`)
    if r.FindAllString(text, -1) == nil {
        return ""
    }
    return r.FindAllString(text, -1)[0]
}

func NewUserConfig(filename string) (userconf UserConfig) {
    if regexp.MustCompile(`https?://[\w/:%#\$&\?\(\)~\.=\+\-]+`).MatchString(filename) {
        response, err := http.Get(filename)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        readconf, err := ioutil.ReadAll(response.Body)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        err = json.Unmarshal(readconf, &userconf)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
        return userconf
    }
    readconf, err := ioutil.ReadFile(filename)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    err = json.Unmarshal(readconf, &userconf)
    return userconf
}

func NewLogger(filename string) *logrus.Logger {
    logger := logrus.New()
    logger.Formatter = new(logrus.JSONFormatter)
    f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    logger.Out = f
    return logger
}

func SetUpdateLog(category string) logrus.Fields {
    return logrus.Fields{
            "category": category,
        }
}
