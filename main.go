package main

import (
	"compress/gzip"
	"encoding/xml"
	"flag"
	"fmt"
	"golang.org/x/net/html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	version   = "1.0"
	defaultUA = "Optimus Sitemap Generator/" + version + " (http://patrickmylund.com/projects/osg/)"
)

var (
	// Sitemap time format is 2006-01-02T15:04:05-07:00
	SitemapTimeFormat         = strings.Replace(time.RFC3339, "Z", "-", -1)
	MaxDepth                  = 500
	throttle          *uint   = flag.Uint("c", 1, "pages to crawl at once")
	maxCrawl          *uint   = flag.Uint("max-crawl", 0, "maximum number of pages to crawl (0 = unlimited)")
	useragent         *string = flag.String("ua", defaultUA, "User-Agent header to send")
	gzipLevel         *int    = flag.Int("gzip-level", gzip.DefaultCompression, "compression level when generating a sitemap.xml.gz file (1 = fastest, 9 = best, -1 = default)")
	verbose           *bool   = flag.Bool("v", false, "show additional information about the generation process")
	nowarn            *bool   = flag.Bool("no-warn", false, "don't warn about pages that were not opened successfully")
	noRobots          *bool   = flag.Bool("no-robots", false, "ignores domains' robots.txt and page nofollow directives")
	noLastmod         *bool   = flag.Bool("no-lastmod", false, "don't include last-modified information in sitemap entries")
	indent            *bool   = flag.Bool("indent", false, "indent the entries with a tab")
)

var (
	oneCrawl chan bool
	sem      chan bool
	client   = http.DefaultClient
	all      = map[string]*Result{}
	mu       = sync.Mutex{}
	roots    []*url.URL
)

type TagType int

const (
	Unknown TagType = iota
	A
	Meta
)

type tag struct {
	Type      TagType
	Href      string
	Rel       string
	HttpEquiv string
	Content   string
}

type Crawler struct {
	Root *url.URL
	Ch   chan<- *Result
	wg   *sync.WaitGroup
}

type Result struct {
	Loc     string
	Lastmod string
}

func (c *Crawler) Crawl(u *url.URL, root *url.URL, lastmod string) {
	defer c.wg.Done()
	<-sem

	var (
		since *time.Time
		err   error
	)
	s := u.String()
	mu.Lock()
	_, found := all[s]
	if found {
		if *verbose {
			log.Println("Already crawled", s+"; skipping")
		}
		mu.Unlock()
		return
	}
	if lastmod != "" {
		parsed, err := time.Parse(SitemapTimeFormat, lastmod)
		if err != nil {
			log.Println("Couldn't parse Lastmod value in sitemap entry for", s+":", err)
		} else {
			since = &parsed
		}
	}
	r := &Result{
		Loc: s,
	}
	all[s] = r
	mu.Unlock()

	// TODO: abort if changefreq hasn't been reached

	defer func() {
		c.Ch <- r
	}()
	if *maxCrawl > 0 {
		_, ok := <-oneCrawl
		if !ok {
			return
		}
	}
	if *verbose {
		log.Println("Opening", s)
	}
	res, err := Get(s, since)
	if err != nil {
		log.Println("Error opening", s+":", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		log.Println("Link to", s, "is broken (404 Not Found)")
		return
	}
	lm, err := time.Parse(http.TimeFormat, res.Header.Get("Last-Modified"))
	r.Lastmod = lm.Format(SitemapTimeFormat)
	ct := res.Header.Get("Content-Type")
	var dontCrawl bool
	if strings.HasPrefix(ct, "text/html") {
		// Proceed
	} else if strings.HasPrefix(ct, "text/xml") {
		// TODO: Read sitemaps
		log.Println("Found a sitemap (child sitemap discovery not yet implemented)")
	} else {
		if *verbose {
			log.Println("Not crawling", s, "(content type "+ct+")")
		}
		dontCrawl = true
	}
	if !dontCrawl {
		links := c.GetLinks(res.Body, u, root)
		c.wg.Add(len(links))
		for _, v := range links {
			if *verbose {
				log.Println("Following link on", u, "to", v)
			}
			sem <- true
			go c.Crawl(v, root, "")
		}
	}
}

func (c *Crawler) GetLinks(r io.Reader, u *url.URL, root *url.URL) []*url.URL {
	shouldInclude := func(u *url.URL) bool {
		s := u.String()
		for _, v := range roots {
			for _, ov := range []string{"http://" + v.Host, "https://" + v.Host} {
				if strings.Contains(s, ov) {
					return true
				}
			}
		}
		return false
	}
	links := GetLinks(r, u)
	if *verbose && len(links) == 0 {
		log.Println("Found no links on", u)
	}
	var nlinks []*url.URL
	for _, v := range links {
		if v.Scheme == "" {
			v.Scheme = root.Scheme
		}
		if v.Host == "" {
			v.Host = root.Host
		}
		if *verbose {
			log.Println("Found a link on", u, "to", v)
		}
		if shouldInclude(v) {
			nlinks = append(nlinks, v)
		} else {
			if *verbose {
				log.Println(v, "outside specified domain(s); ignoring")
			}
		}
	}
	return nlinks
}

func GetLinks(r io.Reader, u *url.URL) (links []*url.URL) {
	z := html.NewTokenizer(r)
	depth := 0
	tree := make([]*tag, MaxDepth)
L:
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			err := z.Err()
			if err != io.EOF {
				log.Println("Couldn't process", u, "- Error:", err.Error())
			}
			break L
		case html.StartTagToken:
			depth++
			if depth >= MaxDepth {
				log.Println("Reached MaxDepth on", u.String()+"; skipping")
				return links
			}
			t := new(tag)
			tree[depth] = t
			tn, _ := z.TagName()
			switch string(tn) {
			default:
				t.Type = Unknown
			case "a":
				t.Type = A
			case "meta":
				t.Type = Meta
			}
			for {
				key, val, more := z.TagAttr()
				k := strings.TrimSpace(string(key)) // Trim just to be sure
				v := strings.TrimSpace(string(val))
				// vi, _ := strconv.Atoi(v)
				switch t.Type {
				case A:
					switch k {
					case "href":
						t.Href = v
					case "rel":
						t.Rel = v
					}
				case Meta:
					switch k {
					case "http-equiv":
						t.HttpEquiv = v
					case "content":
						t.Content = v
					}
				}
				if !more {
					break
				}
			}
		case html.EndTagToken:
			t := tree[depth]
			if t == nil {
				log.Println("Malformed page", u, "- cannot reliably determine start/beginning of tags; aborting")
				return nil // TODO: Remember the starting type and recover some bugs?
			}
			if t.Type == Meta && strings.Contains(t.Content, "nofollow") {
				if *noRobots {
					if *verbose {
						log.Println("Ignoring nofollow directive on", u)
					}
				} else {
					if *verbose {
						log.Println(u, "has a nofollow directive; skipping page")
					}
					return links
				}
			} else if t.Type == A && t.Href != "" {
				ignore := false
				if t.Href[0] == '#' {
					ignore = true
				}
				if strings.Contains(t.Rel, "nofollow") {
					if *noRobots {
						if *verbose {
							log.Println("Ignoring nofollow directive on link to", t.Href, "on page", u)
						}
					} else {
						if *verbose {
							log.Println("Link to", t.Href, "on page", u, "has a nofollow directive; skipping link")
						}
						ignore = true
					}
				}
				if !ignore {
					u, err := url.Parse(t.Href)
					if err != nil {
						log.Println("Error parsing URL", t.Href, "on", u.String()+": ", err.Error())
					} else {
						links = append(links, u)
					}
				}
			}
			depth--
		}
	}
	return links
}

type Url struct {
	Loc     string `xml:"loc"`
	Lastmod string `xml:"lastmod,omitempty"`
	// Changefreq string  `xml:"changefreq"`
	// Priority   float64 `xml:"priority"`
}

type Urlset struct {
	// TODO: How to add Schema links/info?
	XMLName xml.Name `xml:"urlset"`
	Url     []Url    `xml:"url"`
}

func Get(url string, ifmod *time.Time) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if ifmod != nil {
		timeStr := ifmod.Format(http.TimeFormat)
		req.Header.Add("If-Modified-Since", timeStr)
	}
	req.Header.Add("User-Agent", *useragent)
	return client.Do(req)
}

func GetUrlsFromSitemap(r io.Reader) (urlset *Urlset, err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Panic unserializing sitemap: %v", x)
		}
	}()
	err = xml.NewDecoder(r).Decode(&urlset)
	return
}

func generateSitemap(path string, urls []*url.URL) (*Urlset, error) {
	ch := make(chan *Result, 1)
	wg := new(sync.WaitGroup)
	l := len(urls)
	wg.Add(l)
	cs := make(map[string]*Crawler)
	for _, v := range urls {
		r := getRoot(v)
		roots = append(roots, r)
		c := &Crawler{r, ch, wg}
		cs[r.String()] = c
		go func() {
			sem <- true
			c.Crawl(v, r, "")
		}()
	}

	var fs io.ReadCloser
	fs, err := os.Open(path)
	if err != nil {
		log.Println("Couldn't read existing sitemap", path+":", err.Error()+"; continuing")
	} else {
		defer fs.Close()
		if strings.HasSuffix(path, ".gz") {
			if *verbose {
				log.Println("Extracting compressed data")
			}
			nfs, err := gzip.NewReader(fs)
			if err != nil {
				log.Println("Couldn't start decompression for gzipped file", path)
			} else {
				fs = nfs
				defer fs.Close()
			}
		}
		urlset, err := GetUrlsFromSitemap(fs)
		if err != nil {
			log.Println("Couldn't parse existing sitemap", path+":", err.Error()+"; continuing")
		} else {
			for _, v := range urlset.Url {
				u, err := url.Parse(v.Loc)
				if err != nil {
					log.Println("Couldn't parse URL", v.Loc, "from existing sitemap", path+":", err.Error()+"; continuing")
				} else {
					r := getRoot(u)
					s := r.String()
					oc, found := cs[s]
					if found {
						go oc.Crawl(u, r, v.Lastmod)
					} else if *verbose {
						log.Println("Skipping URL", v.Loc, "not on the same domain ("+s+")")
					}
				}
			}
		}
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	sm := new(Urlset)
	for {
		res, ok := <-ch
		if !ok {
			break
		}
		if res != nil {
			u := Url{
				Loc: res.Loc,
			}
			if *noLastmod {
				u.Lastmod = ""
			} else {
				u.Lastmod = res.Lastmod
			}
			sm.Url = append(sm.Url, u)
		}
	}
	return sm, nil
}

func getRoot(u *url.URL) *url.URL {
	r := new(url.URL)
	r.Scheme = u.Scheme
	r.Host = u.Host
	return r
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Println("Optimus Sitemap Generator", version)
		fmt.Println("http://patrickmylund.com/projects/osg/")
		fmt.Println("-----")
		flag.Usage()
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println(" ", os.Args[0], "sitemap.xml example.com")
		fmt.Println(" ", os.Args[0], "sitemap.xml site1.com site2.com https://site3.com")
		fmt.Println(" ", os.Args[0], "-c 10 sitemap.xml example.com")
		fmt.Println("")
		fmt.Println("Use OSG on an existing sitemap to crawl only pages which have changed since the last run.")
		return
	}
	sem = make(chan bool, *throttle)
	path := flag.Arg(0)
	urlStrings := flag.Args()[1:]

	var urls []*url.URL
	for _, v := range urlStrings {
		if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
			v = "http://" + v
		}
		u, err := url.Parse(v)
		if err != nil {
			log.Println("Couldn't parse URL", v+": "+err.Error())
			continue
		}
		if u.Path == "" {
			u.Path = "/"
		}
		urls = append(urls, u)
	}

	if *maxCrawl > 0 {
		oneCrawl = make(chan bool)
		go func() {
			for i := uint(0); i < *maxCrawl; i++ {
				oneCrawl <- true
			}
			if *verbose {
				log.Println("Maximum crawl limit of", *maxCrawl, "reached; not crawling any more pages")
			}
			close(oneCrawl) // when channel is closed, crawler.Crawl will not open any more pages
		}()
	}
	sm, err := generateSitemap(path, urls)
	if err != nil {
		log.Fatalln("Error generating sitemap:", err)
	}

	var f io.WriteCloser
	f, err = os.Create(path)
	if err != nil {
		log.Fatalln("Couldn't open", path, "for writing"+": "+err.Error())
	}
	defer f.Close()

	if strings.HasSuffix(path, ".gz") {
		if *verbose {
			log.Println("Enabling compression")
		}
		f, err = gzip.NewWriterLevel(f, *gzipLevel)
		if err != nil {
			log.Fatalln("Couldn't create gzip writer for", path)
		}
		defer f.Close()
	}

	fmt.Fprintf(f, `<?xml version="1.0" encoding="UTF-8"?>`)
	enc := xml.NewEncoder(f)
	if *indent {
		enc.Indent("", "\t")
	}
	err = enc.Encode(sm)
	if err != nil {
		log.Fatalln("Couldn't serialize sitemap", path+":", err)
	}
}
