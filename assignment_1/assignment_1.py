# Python 3.11.9
import csv

# this class represents single posting in the postings list.
# each posting contains a document ID (in this case, the tweet ID) and a link to the next posting.
# by linking postings like this, we create a chain of documents for each term in our inverted index.
class Posting:
    def __init__(self, document_id, next_posting=None):
        self.document_id = document_id    # stores the ID of the tweet where this term appears
        self.next_posting = next_posting  # points to the next posting, so we can chain them together

# this is our main inverted index class which holds all the data and provides methods to build and query the index.
class InvertedIndex:
    def __init__(self):
        # dictionary that holds each term with its metadata:
        # - term string itself
        # - size of the postings list (number of docs the term appears in)
        # - pointer to the start of the postings list
        self.dictionary = {}

        # separate storage for the actual postings lists, which we access via pointers
        # each pointer maps to a linked list of `Posting` objects
        self.postings_lists = {}
        
        # counter to assign unique pointers to each new postings list
        self.posting_id_counter = 0

    # noarmalize a term by making it lowercase and removing any non-alphanumeric characters.
    # this helps keep our index consistent so "Side" and "side" are treated as the same term.
    def normalize(self, term):
        term = term.lower()
        term = ''.join(char for char in term if char.isalnum())
        return term

    # main function to build the index from a CSV file of tweets.
    # it reads each tweet, tokenizes the text, and builds postings lists for each unique term.
    def index(self, filename):
        with open(filename, 'r', encoding='utf-8') as file:
            # set the delimiter to tab since each field is separated by a tab in this data
            reader = csv.reader(file, delimiter='\t')
            for row_number, row in enumerate(reader, start=1):
                if len(row) < 5:
                    # skip rows that don’t have at least 5 fields
                    # these are probably incomplete tweets
                    continue  

                # rxtract the tweet ID and text fields
                tweet_id = row[1]
                tweet_text = row[4].replace("[NEWLINE]", "\n").replace("[TAB]", "\t")
                
                # split and normalize each word in the tweet text
                tokens = [self.normalize(token) for token in tweet_text.split()]

                # for each term, add the tweet ID to the postings list for that term
                for term in tokens:
                    if term not in self.dictionary:
                        # if we havent seen this term before, create a new postings list for it
                        pointer = self.posting_id_counter
                        self.postings_lists[pointer] = None  # initialize as an empty list
                        self.dictionary[term] = (term, 0, pointer)  # store term, size, and pointer
                        self.posting_id_counter += 1
                    
                    # rtrieve the pointer and current posting data for this term
                    term_data = self.dictionary[term]
                    pointer = term_data[2]
                    current_posting = self.postings_lists[pointer]

                    # only add this tweet ID if it’s not already at the start of the list
                    if current_posting is None or current_posting.document_id != tweet_id:
                        # create a new posting that points to the current start of the list
                        new_posting = Posting(tweet_id, current_posting)
                        # uupdate the start of the list to this new posting
                        self.postings_lists[pointer] = new_posting
                        # update the term data in the dictionary to reflect the new size of the list
                        self.dictionary[term] = (term, term_data[1] + 1, pointer)
        
    # query function to search for tweets containing all the given terms
    # it returns a list of tweet IDs that contain all the specified terms
    def query(self, *terms):
        # normalize each term to match the format we used when building the index
        normalized_terms = [self.normalize(term) for term in terms]
        
        # gather the postings list for each term
        postings_lists = []
        for term in normalized_terms:
            term_data = self.dictionary.get(term)
            if not term_data:
                # if any term is missing, we know there are no matching tweets
                print(f"Term '{term}' not found in the index.")
                return []  # Return an empty list immediately
            postings_lists.append(sorted(self.postings_list_iterator(self.postings_lists[term_data[2]])))
        
        # intersect all the postings lists to find tweet IDs that contain all terms
        result = postings_lists[0]
        for postings in postings_lists[1:]:
            result = self.intersect(result, postings)

        return result

    # helper function to intersect two sorted lists of tweet IDs
    def intersect(self, list1, list2):
        iterator1, iterator2 = iter(list1), iter(list2)
        result = []
        doc_id1, doc_id2 = next(iterator1, None), next(iterator2, None)
        
        while doc_id1 is not None and doc_id2 is not None:
            if doc_id1 == doc_id2:
                # when IDs match, add to the result and advance both pointers
                result.append(doc_id1)
                doc_id1, doc_id2 = next(iterator1, None), next(iterator2, None)
            elif doc_id1 < doc_id2:
                # advance the iterator for the smaller ID to try to match it
                doc_id1 = next(iterator1, None)
            else:
                doc_id2 = next(iterator2, None)
        
        return result

    # hlper generator to yield document IDs in a postings list
    def postings_list_iterator(self, head):
        current = head
        seen_ids = set()  # keep track of IDs we’ve already yielded to avoid duplicates
        while current:
            if current.document_id not in seen_ids:
                seen_ids.add(current.document_id)
                yield current.document_id
            current = current.next_posting


index = InvertedIndex()
filename = 'twitter.csv'  
index.index(filename)

# run a query for tweets mentioning "side effects of malaria vaccines"
search_terms = ["side", "effects", "malaria"]
resulting_tweet_ids = index.query(*search_terms)
print(f"Tweet IDs for tweets mentioning '{' '.join(search_terms)}': {resulting_tweet_ids}")
