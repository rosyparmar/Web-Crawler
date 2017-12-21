from constants import *
from urlparse import urlparse
from difflib import SequenceMatcher
 
class  html_info(object):
     
    """url is of type URLDetails class"""
    def __init__(self, id = None, urlInfo = None, httpHeader = None, 
        title = None, pageText = None, pageHTML = None, inLinks = None, 
        outLinks = None, tags = None, relevance = None):
 
        self.id = id
        self.urlInfo = urlInfo
        self.score = self.giveScore(self.urlInfo)
        self.httpHeader = httpHeader
        self.title = title 
        self.pageHTML = pageHTML
        self.pageText = pageText
         
        if(inLinks == None):
            self.inLinks = {}
        else:
            self.inLinks = inLinks
 
        if(outLinks == None):
            self.outLinks = {}
        else:
            self.outLinks = outLinks
 
        if(tags == None):
            self.tags = {}
        else:
            self.tags = tags
 
        self.relevance = relevance

    def giveScore(self, urlInfo):
            inform = urlparse(urlInfo["url"])
            domainScore = self.getMatchScore(urlInfo["domainName"], type = "domainName")
            pathScore   = self.scoreList(inform.path.split("/")[1:], "path")
            titleScore  = self.scoreList(urlInfo["title"].split(), "title")
            anchorScore = self.scoreList(urlInfo["anc_text"].split(), "anc_text")
            return domainScore + pathScore + titleScore + anchorScore
        

    def scoreList(self, pathList, type):
            score  = 0
            for path in pathList:
                    score  = score + self.getMatchScore(path, type)
            return score

    def getDomainName(self, netloc):
            print netloc
            start = netloc.index(".") + 1
            end = netloc.index( ".", start)
            domainName = netloc[start:end]
            return domainName
    
    
    def getMatchScore(self, str, type):
            score = 0;
            str = str.lower()
            k = 1
            if(type == "domainName"):
                    k = 1
            if(type == "path"):
                    k = 1
            if(type == "title" or type == "anchorText"):
                    k = 5

            for term in POSITIVETERMS:
                    score = SequenceMatcher(None, str, term).ratio()
                    if(score >= 0.6):
                            return k * score
                    score = 0
            if score <  k * 0.25:
                    for term in NEGATIVETERMS:
                            score = SequenceMatcher(None, str, term).ratio()
                            if(score >= 0.7):
                                    return -1 * k * score
            return 0
     
if __name__ == '__main__':
    sd = html_info(urlInfo = {
        "url" : "http://en.wikipedia.org/wiki/List_of_career_achievements_by_Kobe_Bryant",
        "domainName" : " ",
        "title" : "List of career achievements by Kobe Bryant - Wikipedia, the free encyclopedia",
        "anc_text" : " "})
