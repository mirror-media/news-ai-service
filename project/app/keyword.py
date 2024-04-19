from keybert import KeyBERT
from bs4 import BeautifulSoup as bs
import regex as re

class KeywordExtractor():
  def __init__(self, model, vectorizer, threshold: float=0.2):
    self.kw_model  = KeyBERT(model=model)
    self.vectorizer = vectorizer
    self.threshold = threshold
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
    pattern = re.compile(r'[^\u4e00-\u9fa5a-zA-Z]')
    posts = self.preprocessing(posts)
    for post in posts:
      keywords = self.kw_model.extract_keywords(post, vectorizer=self.vectorizer)
      ### 對模型預測的keywords進行處理
      filtered_keywords = []
      for keyword, score in keywords:
        keyword = re.sub(pattern, '', keyword)
        if len(keyword)>1 and score>0.2:
          filtered_keywords.append(keyword)
      ### 對於沒有顯著關鍵詞的文章，我們至少給其最高分的單詞為關鍵詞
      if len(filtered_keywords)==0 and len(keywords)>0:
        keyword, _ = keywords[0]
        filtered_keywords.append(keyword)
      keywords_table.append(filtered_keywords)
    return keywords_table