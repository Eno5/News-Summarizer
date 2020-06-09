from gensim.summarization.summarizer import summarize
from requests import Session
from lxml.html import fromstring as parse
from newspaper import Article
from google.modules.utils import _get_search_url, get_html
from wrappers import thread_heavy

class News(object):
    def __init__(self, pages=2, lang='en'):
        self.articles = []
        self.pages = pages
        self.lang = lang
        self.results = []
        
    @thread_heavy(max_workers=40)
    def _pull_info(self, url):
        article = Article(url)
        article.download()
        try:
            article.parse()
            try:
                summary = summarize(article.text)
            except:
                summary = article.summary

            self.articles.append([article.source_url,
                    article.title,
                    article.authors,
                    article.publish_date,
                    summary])
        except: # Could not parse article
            pass
        
    def get_article_data(self):
        return self._pull_info(self.results)

    def search(self, query, area='com', ncr=False, void=True, time_period=False, sort_by_date=True, first_page=0):
        for i in range(first_page, first_page + self.pages):
            url = _get_search_url(query, i, lang=self.lang, area=area, ncr=ncr, time_period=time_period, sort_by_date=sort_by_date)
            print(f'Search URL: {url}&tbm=nws')
            html = parse(get_html(url+"&tbm=nws"))
            links = html.xpath('//div[@id="rso"]/descendant::a/@href')

            [self.results.append(link) for link in links if link[0]!='/'] # URL leads out of Google

    
if __name__=='__main__':
    n = News(pages=1)
    n.search('Donald Trump')
    n.get_article_data()
    print(f'Number of Search Results: {len(n.results)}')
    print(f'Number of Articles: {len(n.articles)}')