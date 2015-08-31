package main

import (
    "net/http"
    "strconv"
    "math/rand"
    "github.com/flosch/pongo2"
    "github.com/zenazn/goji/web"
)

type Controller struct {
    *DataManager
}

func NewController(dm *DataManager) *Controller {
    return &Controller{dm}
}

func (cntr Controller) ApiOutLink(c web.C, w http.ResponseWriter, r *http.Request) {
    keyname := REDISKEY_FEED_ITEM_PREFIX + c.URLParams["id"]
    if cntr.IsKeyExists(keyname) {
        cntr.SetOutLinkIncrement(keyname)
    }
}

func (cntr Controller) Root(c web.C, w http.ResponseWriter, r *http.Request) {
    tpl, err := pongo2.FromFile("main.j2")
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    pagenum, err := strconv.Atoi(r.URL.Query().Get("p"))
    if err != nil {
        pagenum = 1;
    }
    reqid := r.URL.Query().Get("id")
    if cntr.IsKeyExists(REDISKEY_FEED_ITEM_PREFIX + reqid) {
        cntr.SetInLinkIncrement(REDISKEY_FEED_ITEM_PREFIX + reqid)
    }
    category := c.URLParams["category"]
    items := cntr.GetPageFeedItem(pagenum, category, cntr.UserConfig.Site.ItemDays, cntr.UserConfig.Site.PageNewItemCount)
    rankitems := cntr.GetPageFeedRankItem(pagenum, category, cntr.UserConfig.Site.ItemDays, cntr.UserConfig.Site.PageRankItemCount)
    tpl.ExecuteWriter(pongo2.Context{"items": items, "rankitems": rankitems, "p": pagenum, "reqid": reqid, "category": category}, w)
}

func (cntr Controller) NewFeed(c web.C, w http.ResponseWriter, r *http.Request) {
    tpl, err := pongo2.FromFile("rss2.j2")
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    items := cntr.GetPageFeedItem(1, c.URLParams["category"], cntr.UserConfig.Site.ItemDays, cntr.UserConfig.Site.PageNewItemCount)
    tpl.ExecuteWriter(pongo2.Context{"items": items}, w)
}

func GetRankDaysKeyname(days int, category string) string {
    if category != "" {
        return REDISKEY_FEED_RANK_DAYS_PREFIX + category + ":" + strconv.Itoa(days)
    }
    return REDISKEY_FEED_RANK_DAYS_PREFIX + strconv.Itoa(days)
}

func GetRandItem(items []Item) []Item {
    for i := range items {
        j := rand.Intn(i + 1)
        items[i], items[j] = items[j], items[i]
    }
    return items
}
