from app.tool import open_file
from app.keyword import KeywordExtractor
from ckip_transformers.nlp import CkipWordSegmenter, CkipPosTagger, CkipNerChunker

SYNCRHONIZE_SECONDS     = 180
MAX_SYNCHRONIZE_TAGS    = 100
DEFAULT_FETCH_EXTERNALS = 100

def load_vectorizer():
    ### load corpus for Tokenizer
    model = 'bert-base'
    ws  = CkipWordSegmenter(model=model)
    print('Load ws successfully')
    pos = CkipPosTagger(model=model)
    print('Load post successfully')
    ner = CkipNerChunker(model=model)
    print('Load ner successfully')

    ### create keyword extractor
    keyword_extractor = KeywordExtractor(ws, pos, ner)
    print('Create keyword extractor successfully')

    return keyword_extractor

