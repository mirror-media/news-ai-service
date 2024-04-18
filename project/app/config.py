from app.tool import open_file
from app.keyword import KeywordExtractor
from ckip_transformers.nlp import CkipWordSegmenter
from sklearn.feature_extraction.text import CountVectorizer

SYNCRHONIZE_SECONDS     = 180
MAX_SYNCHRONIZE_TAGS    = 100
DEFAULT_FETCH_EXTERNALS = 100

def load_vectorizer():
    ### load corpus for Tokenizer
    model = 'bert-base'
    ws  = CkipWordSegmenter(model=model)
    print('Load ws successfully')

    def ws_tokenizer(text):
        words = ws([text])
        return words[0]
    vectorizer = CountVectorizer(tokenizer=ws_tokenizer)
    
    ### create keyword extractor
    keyword_extractor = KeywordExtractor(
        model = 'distiluse-base-multilingual-cased-v1',
        vectorizer = vectorizer
    )
    print('Create keyword extractor successfully')

    return keyword_extractor

