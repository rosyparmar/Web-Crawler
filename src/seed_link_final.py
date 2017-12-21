import urllib2
from bs4 import BeautifulSoup
from urlparse import urlparse
from Pq import priority_dict
from urlparse import urljoin
from reppy.cache import RobotsCache
from urltools import *
from ES import ES
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


NEGATIVE_TERMS = ["football", "golf", "buy", "privacy" ,  "termsofuse", "baseball", "politics"
"category", "hockey", "privacy", "action=\"edit\"", "wikimedia", "mediawiki"
"sell", "money", "makemoney", "pronunciation", "chat", "license"
"terms_of_use", "privacy policy", "dining", "food", "realestate", "cookie_statement", "classified", "jobs", "feedback", "privacy",
"cookies", "weather", "copyright", "terms-of-sale", "careers", "feedback", "boxing", "baseball"]


class seed_link(object):
    lastSiteVisited = ""
    lastVisitedTime = ""
    urls_visited = []
    frontier = {}
    robots = RobotsCache()
    rules = {}
    pq = priority_dict()
    records = {}


    es = ES("document_info_test", "indexes")

    def __init__(self, seed):
        self.seed = seed

    def crawl_URL(self,seed):
        self.fetchURL(self.seed)
        for ele in self.frontier:
            self.pq[ele] = self.determine_score(self.frontier[ele])
        print self.pq
        while len(self.urls_visited) < 30000:
            url = self.pq.smallest()
            self.pq.pop_smallest()
            print url
            document = self.processDocument(url)
            if(document == 0):
                print "returned 0"
                continue
            self.addToES(document)

    def fetchURL(self, seed):
        for url in seed:
           url = self.canonicalizeURL("", url)
           #hostname = urlparse.urlparse(url).hostname
           #urlInfo = {"url": url, "hostname": hostname}
           level = 0
           self.frontier[url] = {"inlinks": [], "level": 0}

    def rejectURL(self, url):
        global NEGATIVE_TERMS
        for word in NEGATIVE_TERMS:
            if word in url.lower():
                return True
        return False

    def processDocument(self, url):
        if url in self.urls_visited:
            return 0
        frontierE = self.frontier[url]
        urlInfo = urlparse(url)
        hostname = urlInfo.hostname
        document = {}
        netInfo = urlInfo.netloc
        if(self.rejectURL(url)):
            print "rejected"
            return 0
        if ("wikipedia" in netInfo and not netInfo.startswith("en")):
            return 0
        if (urlInfo.path == "" or urlInfo.path == "/"):
            return 0
        #if not hostname in self.visitedDomains:
        #    self.visitedDomains.append(hostname)

        if(hostname in self.rules):
            premission = self.rules[hostname]
        else:
            premission = self.robots.allowed(url, 'my-agent')
            self.rules[hostname] = premission

        if(self.lastSiteVisited == hostname):
            timeGap = time.time() - self.lastVisitedTime 
            if(timeGap < 1):
                time.sleep(timeGap)
            self.lastVisitedTime = time.time()
        else:
            self.lastVisitedTime = time.time()
            self.lastSiteVisited = hostname


        if not premission:
            return 0

        try:
            web_page = urllib2.urlopen(url)            
        except:
            print "error while processing url"
            return 0
        try:
            soup = BeautifulSoup(web_page, "html.parser")
            for script in soup(["script", "style"]):
                script.extract()

            document["url"] = url
            document["header"] = str(web_page.info())
            document["text"] = soup.get_text()
            document["HTML"] = web_page.read()
            anchorTags = soup.find('body').findAll('a')
            document["title"] = soup.title.string
            document["outlinks"] = self.getOutLinks(url, anchorTags)
            document["inlinks"] = self.frontier[url]["inlinks"]
            
            self.urls_visited.append(url)
        except:
            return 0
        self.updatePriorityQueue(url, document["outlinks"], frontierE["level"])
        return document

    def canonicalizeURL(self,parentURL ,url):
        try:
            url = urljoin(parentURL, url)
            if "#" in url:
                urlarr = url.split("#")
                url = urlarr[0]
            canonURL =  normalize(url)

        except Exception, e:
            print e
            return  ""
        return canonURL

    def determine_score(self, ele):
        return (-1 * ((100 - ele["level"]) * 0.95 +
                    len(ele["inlinks"]) * 0.05))

    def updatePriorityQueue(self, parenturl, outLinks, level):
        for url in outLinks:
            if parenturl == url:
                continue
            if url in self.pq:
                self.frontier[url]["inlinks"].append(parenturl)
                self.frontier[url]["inlinks"] = list(set(self.frontier[url]["inlinks"]))
                self.pq[url] = self.determine_score(self.frontier[url])
            elif(url in self.urls_visited):
                self.frontier[url]["inlinks"].append(parenturl)
                self.frontier[url]["inlinks"] = list(set(self.frontier[url]["inlinks"]))
                self.updateES(url, parenturl)
            else:
                self.frontier[url] = { "inlinks": [parenturl], "level": level + 1}
                self.pq[url] = self.determine_score(self.frontier[url])

    def getOutLinks(self, parenturl, anchors):
        urlList = []
        for anchor in anchors:
            try:
                url = ""
                if(anchor.get("href") == None or
                    len(anchor.contents) == 0 or
                    anchor.get("href") == ""):
                    continue
                url = anchor["href"]
                #print url
                canurl = self.canonicalizeURL(parenturl, url)
                if(canurl == ""):
                    continue

                urlList.append(canurl)
            except:
                continue
        return urlList

    def addToES(self, document):
      action = {'index': {'_index': "document_info_test", '_type': "indexes", '_id': document["url"]}}
      index  = {"url": document["url"], "text": document["text"], "title": document["title"], "html":document["HTML"], 
        "outlinks": document["outlinks"], "inlinks":document["inlinks"], "httpheader" : document["header"], "author" : "Rosy"}
      
      self.records[document["url"]] = {"action": action, "index": index}

      if len(self.records) >= 10:
          self.es.updateElasticSearch(self.records)
          self.records = {}
      return

    def updateES(self, url, parenturl):
        print url
        print "after URL in updates"
        for record in self.records:
            self.records[record]["index"]["url"]
        if (not url in self.records):
            action = {'update': {'_index': "document_info_test", '_type': "indexes", '_id': url}}
            body = {
                "script": "ctx._source.inlinks+=new_link",
                "params": {"new_link": url}
            }
        else:
            index = self.records[url]["index"]
            index["inlinks"].append(url)
            self.records[url] = {"action": self.records[url]["action"],
                                "index": index}

if __name__ == '__main__':  
    initialSeed = ["http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant",
     "http://en.wikipedia.org/wiki/Kobe_Bryant",
     "http://www.basketball-reference.com/awards/nba_50_greatest.html",
     "http://www.basketball-reference.com/leaders/per_career.html"]
    first_crawl = seed_link(initialSeed)
    first_crawl.crawl_URL(initialSeed)














