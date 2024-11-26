# Python 3.11.9
import csv

# this class represents a single posting in the postings list.
# each posting contains a document ID (in this case, the tweet ID) and a link to the next posting.
# by linking postings like this, we create a chain of documents for each term in our inverted index.
class Posting:
    def __init__(self, document_id, next_posting=None):
        self.document_id = document_id    # Stores the ID of the tweet where this term appears
        self.next_posting = next_posting  # Points to the next posting, so we can chain them together

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

        # permuterm index to handle wildcard queries
        # maps permuterm forms to the original terms
        self.permuterm_index = {}  # New addition: permuterm index for wildcard support

    # normalize a term by making it lowercase and removing any non-alphanumeric characters.
    # optionally, keep wildcards in the term for query processing.
    def normalize(self, term, keep_wildcards=False):
        term = term.lower()
        if keep_wildcards:
            # when processing query terms, we might want to keep the wildcard character '*'
            term = ''.join(char for char in term if char.isalnum() or char == '*')
        else:
            term = ''.join(char for char in term if char.isalnum())
        return term

    # generate all permuterm forms for a given term
    # this is used to build the permuterm index for wildcard queries
    def generate_permuterms(self, term):
        permuterms = []
        term = term + '$'  # append a special end symbol '$' to the term
        # generate all rotations of the term
        for i in range(len(term)):
            rotated = term[i:] + term[:i]
            permuterms.append(rotated)
        return permuterms

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

                # extract the tweet ID and text fields
                tweet_id = row[1]
                tweet_text = row[4].replace("[NEWLINE]", "\n").replace("[TAB]", "\t")
                
                # split and normalize each word in the tweet text
                tokens = [self.normalize(token) for token in tweet_text.split()]

                # for each term, add the tweet ID to the postings list for that term
                for term in tokens:
                    if term:
                        # build permuterm index for wildcard queries
                        # for each term, generate its permuterm rotations and add to the permuterm index
                        permuterms = self.generate_permuterms(term)
                        for permuterm in permuterms:
                            # map each permuterm rotation to the original term
                            if permuterm in self.permuterm_index:
                                self.permuterm_index[permuterm].add(term)
                            else:
                                self.permuterm_index[permuterm] = {term}

                        if term not in self.dictionary:
                            # if we haven't seen this term before, create a new postings list for it
                            pointer = self.posting_id_counter
                            self.postings_lists[pointer] = None  # initialize as an empty list
                            self.dictionary[term] = (term, 0, pointer)  # store term, size, and pointer
                            self.posting_id_counter += 1
                        
                        # retrieve the pointer and current posting data for this term
                        term_data = self.dictionary[term]
                        pointer = term_data[2]
                        current_posting = self.postings_lists[pointer]

                        # only add this tweet ID if it’s not already at the start of the list
                        if current_posting is None or current_posting.document_id != tweet_id:
                            # crreate a new posting that points to the current start of the list
                            new_posting = Posting(tweet_id, current_posting)
                            # pdate the start of the list to this new posting
                            self.postings_lists[pointer] = new_posting
                            # update the term data in the dictionary to reflect the new size of the list
                            self.dictionary[term] = (term, term_data[1] + 1, pointer)
        
    # helper function to expand wildcard terms using the permuterm index
    def expand_wildcard(self, term):
        # append '$' to the term to match the format in the permuterm index
        term_with_dollar = term + '$'
        index_of_star = term.find('*')
        if index_of_star == -1:
            # no wildcard in the term, return the term itself
            return [term]
        # rotate the term to move everything after '*' to the front
        rotated = term_with_dollar[index_of_star+1:] + term_with_dollar[:index_of_star+1]
        # remove '*', get the search prefix for matching permuterms
        s = rotated.rstrip('*')
        # now, find all permuterms in the index that start with this prefix
        matching_terms = set()
        for permuterm in self.permuterm_index:
            if permuterm.startswith(s):
                # add all the original terms associated with this permuterm
                matching_terms.update(self.permuterm_index[permuterm])
        return list(matching_terms)
    
    # query function to search for tweets containing all the given terms
    # it returns a list of tweet IDs that contain all the specified terms
    def query(self, *terms):
        # normalize each term to match the format we used when building the index
        # keep wildcards in the terms if present
        normalized_terms = [self.normalize(term, keep_wildcards=True) for term in terms]
        
        # expand wildcards and collect postings lists
        expanded_terms_list = []
        for term in normalized_terms:
            if '*' in term:
                # if the term contains a wildcard, expand it using the permuterm index
                expanded_terms = self.expand_wildcard(term)
                if not expanded_terms:
                    # print(f"No terms found matching wildcard '{term}'.")
                    return []  # return empty list
                expanded_terms_list.append(expanded_terms)
            else:
                # if no wildcard, use the term as is
                expanded_terms_list.append([term])
        
        # gather the postings list for each term (or group of terms if wildcard expanded)
        postings_lists = []
        for terms in expanded_terms_list:
            term_postings = set()
            for term in terms:
                term_data = self.dictionary.get(term)
                if not term_data:
                    # term not in index, skip it
                    continue
                # retrieve the postings list for the term
                postings = set(self.postings_list_iterator(self.postings_lists[term_data[2]]))
                term_postings.update(postings)
            if not term_postings:
                # no postings found for any of the expanded terms
                return []
            postings_lists.append(sorted(term_postings))
        
        # intersect all the postings lists to find tweet IDs that contain all terms
        result = postings_lists[0]
        for postings in postings_lists[1:]:
            result = self.intersect(result, postings)
            if not result:
                # early exit if intersection is empty
                break

        return result

    # helper function to intersect two sorted lists of tweet IDs
    def intersect(self, list1, list2):
        # intersect two sorted lists using two-pointer technique
        iterator1, iterator2 = iter(list1), iter(list2)
        result = []
        doc_id1, doc_id2 = next(iterator1, None), next(iterator2, None)
        
        while doc_id1 is not None and doc_id2 is not None:
            if doc_id1 == doc_id2:
                #wWhen IDs match, add to the result and advance both pointers
                result.append(doc_id1)
                doc_id1, doc_id2 = next(iterator1, None), next(iterator2, None)
            elif doc_id1 < doc_id2:
                # advance the iterator for the smaller ID
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


# initialize the index
index = InvertedIndex()
filename = 'twitter.csv'  
index.index(filename)

# define queries with wildcards
queries = [
    ('*ffect', 'vaccine'),     # (1) wildcard on the left
    ('mal*', 'disease'),       # (2) wildcard on the right
    ('s*e', 'effect'),         # (3) wildcard between other characters
    ('*ffect*', 'vaccine'),    # (4) wildcards on both sides
]

# execute queries and print results
for query_terms in queries:
    resulting_tweet_ids = index.query(*query_terms)
    print(f"Tweet IDs for query terms '{' AND '.join(query_terms)}': {resulting_tweet_ids}")
