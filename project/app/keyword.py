from sklearn.preprocessing import MinMaxScaler
from bs4 import BeautifulSoup as bs
import copy
import regex as re
import numpy as np

class KeywordExtractor():
    def __init__(self, ws, pos, ner):
        ### tools from ckiptagger
        self.ws, self.pos, self.ner = ws, pos, ner
        self.scaler   = MinMaxScaler()
        self.important_entity_types = {
            'NORP': 2,
            'ORG': 3,
            'LOC': 0.8,
            'FAC': 2,
            'EVENT': 2,
            'WORK_OF_ART': 3,
            'PRODUCT': 3,
            'LAW': 2,
        }
        ### we can only select the keyword which pos is valid(Most of nouns, verbs, and foreign word)
        # 名詞中有個特例是Nh為代詞，代詞不當作關鍵詞輸出(EX: 你, 你們, 他們...)
        self.valid_pos = set(
            ['Na', 'Nb', 'Nc', 'Ncd', 'Nd', 'Nep', 'Neqa', 'Neqb', 'Nes', 'Neu', 'Nf', 'Ng', 'Nv', 
             'VA', 'VB', 'VC', 'VCL', 'VD', 'VF', 'VE', 'VG', 'VH', 'VHC', 'VI', 'VJ', 'VK', 'VL', 'FW']
        )
        self.valid_noun_pos = set(
            ['Na', 'Nb', 'Nc', 'Ncd', 'Nd', 'Nep', 'Neqa', 'Neqb', 'Nes', 'Neu', 'Nf', 'Ng', 'Nv']
        )
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
    def tfidf(self, ws_posts):
        '''
        Calculate TD-IDF from many encoded_posts, return TD-IDF table and vocabulary dict
        '''
        num_posts = len(ws_posts)
        ### calculate vocabulary
        vocabs = set()
        for tokenized_post in ws_posts:
            vocabs = vocabs | set(tokenized_post)
        num_words = len(vocabs)
        ### calcualte tf(tern-frequency) table
        tf_table = []
        for post_idx, tokenized_post in enumerate(ws_posts):
            post_tf = {} ### tf of word in a post
            for token in tokenized_post:
                post_tf[token] = post_tf.get(token,0)+1
            tf_table.append(post_tf)
        ### calculate idf(inverse-frequency) table
        idf_table = {}
        for vocab in vocabs:
            num_docs_containing_word = sum(1 for post_tf in tf_table if vocab in post_tf)
            idf_table[vocab] = np.log(num_posts / (1 + num_docs_containing_word))
        ### combine tf_table and idf_table
        tfidf_table = np.zeros((num_posts, len(vocabs)))  # Store TF-IDF value for each word in each docuemnt
        vocabulary = {index: word for index, word in enumerate(vocabs)}
        for i, post_tf in enumerate(tf_table):
            for j, vocab in enumerate(vocabs):
                if vocab in post_tf:
                    tfidf_table[i, j] = post_tf[vocab] * idf_table[vocab]
        tfidf_table_scaled = self.scaler.fit_transform(tfidf_table.T).T # Normalization
        return tfidf_table_scaled, vocabulary
    def extract_keywords(self, tfidf_table, vocabulary, threshold: float=0.3, top: int=20):
        '''
        Get top-n highest score words as keyword
        '''
        keyword_table = []
        important_entity_types_keys = self.important_entity_types.keys()
        for idx in range(len(tfidf_table)):
            post_tfidf  = tfidf_table[idx]
            words_score = {idx: float(score) for idx, score in enumerate(post_tfidf) if score>0}
            ### sort the result and get keywords
            words_score_sorted = sorted(words_score.items(), key=lambda item: item[1], reverse=True)[:top]
            keywords = {}
            for pair in words_score_sorted:
                word  = vocabulary[pair[0]]
                score = pair[1]
                keywords[word] = round(score, 3)
            ### filter the keyword that the score is less than threshold
            keywords = {word: score for word, score in keywords.items() if score>threshold}
            keyword_table.append(keywords)
        return keyword_table
    def merge_adjacent_keywords(self, ws_posts, ner_posts, keyword_table, vocabulary, pos_noun_table, threshold: float=0.4, top: int=10):
        vocabulary_reverse = {word: index for index, word in vocabulary.items()}
        important_entity_types_keys = self.important_entity_types.keys()
        for post_idx in range(len(ws_posts)):
            adjacent_table = {}
            seq_post     = ws_posts[post_idx]
            seq_keywords = keyword_table[post_idx]
            keywords_set = set(seq_keywords.keys())
            ### precalculate post ner
            post_ner = ner_posts[post_idx]
            post_entity_weights = {}
            for ner in post_ner:
                entity_type, entity_word = ner[-2], ner[-1]
                if entity_type in important_entity_types_keys:
                    post_entity_weights[entity_word] = self.important_entity_types[entity_type]
            post_entity_words = post_entity_weights.keys()
            ### calculate graph
            for token_idx, token in enumerate(seq_post[:-2]):
                next_token = seq_post[token_idx+1]
                outgoing_tokens = adjacent_table.setdefault(token, {})
                outgoing_tokens[next_token] = outgoing_tokens.get(next_token, 0) + 1
            ### organize the outgoing_tokens result, and catch the edge which has highest weight
            pop_tokens_list = []
            for token, outgoing_tokens in adjacent_table.items():
                if (token not in keywords_set) or (token not in pos_noun_table):
                    continue
                total_occurences = 0
                significant_next_token = (None, 0) ### record the most significant keyword and its occurences
                for next_tokens, occurrences in outgoing_tokens.items():
                    total_occurences += occurrences
                    if (next_tokens in keywords_set) and (occurrences > significant_next_token[1]):
                        significant_next_token = (next_tokens, occurrences)
                significant_token, highest_occurences = significant_next_token
                if (significant_token != None) and (significant_token in pos_noun_table):
                    compound_token = token + significant_token
                    compound_score = (seq_keywords[token]+seq_keywords[significant_token])*(highest_occurences/total_occurences)
                    if compound_token in post_entity_words:
                        print(f'new compound word is {compound_token}, original weights is {compound_score}')
                        seq_keywords[compound_token] = round(compound_score, 3)
                        pop_tokens_list.append(token)
                        pop_tokens_list.append(significant_token)
            ### after we found compound token, we should remove the original ones
            for pop_token in pop_tokens_list:
                if pop_token in seq_keywords.keys():
                    seq_keywords.pop(pop_token)
            ### after considering merge adjacent, you should modify the score by ner
            for token, score in seq_keywords.items():
                if token in post_entity_words:
                    seq_keywords[token] *= post_entity_weights[token]
            ### sorting seq_keywords and insert back into keyword_table
            words_score_sorted = sorted(seq_keywords.items(), key=lambda item: item[1], reverse=True)[:top]
            keywords = {}
            for pair in words_score_sorted:
                word, score  = pair
                keywords[word] = round(score, 3)
            keywords = {word: score for word, score in keywords.items() if score>threshold}
            keyword_table[post_idx] = keywords
        return keyword_table
    def filter_pos(self, ws_posts, keyword_table, attention: int=3):
        valid_pos_tokens = set()
        pos_noun_table = set()
        ### collect all the keyword from the previous result
        all_keywords = set()
        for keywords in keyword_table:
            all_keywords = all_keywords | set(keywords.keys())
        ### pay attention to the sentence which has keyword
        attention_posts = []
        for ws_post in ws_posts:
            for idx, token in enumerate(ws_post):
                if token in all_keywords:
                    len_post = len(ws_post)
                    start_idx = (idx-attention) if (idx-attention)>0 else 0
                    end_idx = (idx+attention) if (idx+attention)<len_post else len_post
                    attention_posts.append(ws_post[start_idx: end_idx])
        ### calculate pos
        print('calculate pos of attention sentences...')
        pos_posts = self.pos(attention_posts)
        ### pos organization        
        for post_idx, pos_post in enumerate(pos_posts):
            for word_idx, pos in enumerate(pos_post):
                if pos in self.valid_pos:
                    token = attention_posts[post_idx][word_idx]
                    valid_pos_tokens.add(token)
                    ### noun_pos is for further using
                    if pos in self.valid_noun_pos:
                        pos_noun_table.add(token)
        ### filter some keyword which pos is not valid
        for keyword_idx, keywords in enumerate(keyword_table):
            filtered_keywords = {}
            for keyword, score in keywords.items():
                if keyword not in valid_pos_tokens:
                    print(f'remove keyword: {keyword} cause it is out of valid pos')
                    continue
                filtered_keywords[keyword] = score
            keyword_table[keyword_idx] = filtered_keywords
        return keyword_table, pos_noun_table
    def filter_keywords(self, keyword_table, threshold: float=0.7, least_threshold: float=0.4, max_keywords: int=8, min_keywords: int=3):
        ### filter out the keyword which is not a valid word
        pattern = re.compile(r'[^\u4e00-\u9fa5a-zA-Z]')
        for keyword_idx, keywords in enumerate(keyword_table):
            new_keywords = {}
            for keyword, score in keywords.items():
                keyword = re.sub(pattern, '', keyword)
                if keyword=='' or len(keyword)<2: ### invalid keyword, so skip it
                    continue
                num_keywords = len(new_keywords)
                if (num_keywords<min_keywords):
                    if score>least_threshold:
                        new_keywords[keyword] = score
                elif (num_keywords<max_keywords):
                    if score>threshold:
                        new_keywords[keyword] = score
                else:
                    break
            keyword_table[keyword_idx] = new_keywords
        return keyword_table
    def get_keywords(self, posts):
        '''
        Run keyword extraction algorithm, you should be aware that the shape of posts should be (num_posts, contents)
        The return value is tfidf_table and vocabulary for posts
        '''
        posts = self.preprocessing(copy.deepcopy(posts))
        num_posts = len(posts)
        print('execute tokenization...')
        # ws_posts = self.ws(posts, batch_sentences=1)
        ws_posts = self.ws(posts)
        filtered_ws_posts = copy.deepcopy(ws_posts)
        ### get NER(Named-entity recognition) of each post
        print('execute named-entity recognition...')
        ner_posts = self.ner(posts)
        ### filter the word that the len is smaller than 2(cannot be keyword)
        print('filter words that the length is less than 2...')
        for idx, post in enumerate(ws_posts):
            filtered_post = [word for word in post if len(word)>1]
            filtered_ws_posts[idx] = filtered_post
        ### calculate TF-IDF
        print('calculate tfidf...')
        tfidf_table, vocabulary = self.tfidf(filtered_ws_posts)
        ### get keywords from tfidf_table
        print('extract keywords...')
        keyword_table = self.extract_keywords(
            tfidf_table = tfidf_table, 
            vocabulary  = vocabulary, 
            top         = 20
        )
        ### filter keywords which pos are not valid
        print('filter pos...')
        keyword_table, pos_noun_table = self.filter_pos(
            ws_posts      = ws_posts, 
            keyword_table = keyword_table
        )
        ### merge adjacent keywords into larger keywords
        print('modify keyword table by considering adjacent...')
        keyword_table = self.merge_adjacent_keywords(
            ws_posts       = ws_posts,  # you should use original word tokenized result, not the filtered one
            ner_posts      = ner_posts,
            keyword_table  = keyword_table, 
            vocabulary     = vocabulary,
            pos_noun_table = pos_noun_table
        )
        ### filter the keyword table which has some invalid keywords
        keyword_table = self.filter_keywords(keyword_table)
        return keyword_table