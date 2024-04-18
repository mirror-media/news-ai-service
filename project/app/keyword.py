from ckip_transformers.nlp import CkipWordSegmenter
from keybert import KeyBERT
from bs4 import BeautifulSoup as bs

class KeywordExtractor():
  def __init__(self, model, vectorizer):
    self.kw_model  = KeyBERT(model=model)
    self.vectorizer = vectorizer
  def preprocessing(self, posts):
    '''
    Remove html tag and punctuations
    '''
    for idx, post in enumerate(posts):
      soup = bs(post, 'html.parser')
      for a_tag in soup.find_all('a'):
        a_tag.extract()
      posts[idx] = soup.get_text()
    return posts
  def get_keywords(self, posts):
    keywords_table = []
    posts = self.preprocessing(posts)
    for post in posts:
      keywords = self.kw_model.extract_keywords(post, vectorizer=self.vectorizer)
      keywords_table.append(keywords)
    return keywords_table