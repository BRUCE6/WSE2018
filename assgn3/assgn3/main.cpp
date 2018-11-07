#include <iostream>
#include <sstream>
#include <fstream>
#include <cmath>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <algorithm>
#include <queue>
#include <tuple>
#include <assert.h>
#include <sqlite3.h>
#include "util.h"

const static std::string inverted_filename = "inverted_compressed"; 
const static std::string doc_filename = "docs";
const static std::string lexicon_filename = "lexicon";
std::unordered_map<std::string, Lexicon> LEXICON;
std::vector<Document> DOCS;
static float avg_len = 0;

typedef std::tuple<int, std::vector<int>, float> tuple3;

struct ListPointer {
	int did;
	int freq;
	int doc_num;
	int doc_idx;
	std::ifstream inverted;
};


struct compare {
	bool operator()(tuple3 a, tuple3 b) {
		return (std::get<2>(a)> std::get<2>(b));
	}
};


// fixed size return words
void split(std::string s, char sep, std::string *words) {
	std::string word = "";
	int idx = 0;
	for (auto c : s) {
		if (c == sep) {
			words[idx] = word;
			idx += 1;
			//std::cout << word << std::endl;
			word = "";
		}
		else
			word += c;
	}
	//std::cout << word << std::endl;
	words[idx] = word;
}


void split(std::string s, char sep, std::vector<std::string> &words) {
	std::string word = "";
	int idx = 0;
	for (auto c : s) {
		if (c == sep) {
			words.push_back(word);
			idx += 1;
			//std::cout << word << std::endl;
			word = "";
		}
		else
			word += c;
	}
	//std::cout << word << std::endl;
	words.push_back(word);
}


int decompressOneNum(ListPointer *lp) {
	char c;
	int n = 0; 
	lp->inverted.read(&c, 1);
	while (int((unsigned char)c) >= 128) {
		n = n * 128 + int((unsigned char)c) - 128;
		lp->inverted.read(&c, 1);
	}
	n = n * 128 + int(c);
	return n;
}
	

int decodeNext(ListPointer *lp) {
	// cannot go to next term
	if (lp->doc_idx == lp->doc_num)
		return -1;
	int did = decompressOneNum(lp);
	lp->freq = decompressOneNum(lp);
	//char c;
	//int did = 0
	//lp->inverted.read(&c, 1);
	//// Consider the varbyte
	//while (int((unsigned char)c) >= 128) {
	//	did = did * 128 + int((unsigned char)c) - 128;
	//	lp->inverted.read(&c, 1);
	//}
	//did = did * 128 + int(c);
	//lp->inverted.read((char *)&lp->freq, 4);
	lp->doc_idx += 1;
	return lp->did + did;
}


ListPointer* openList(std::string t) {
	ListPointer *lp = new ListPointer;
	lp->doc_idx = 0;
	lp->inverted.open(inverted_filename, std::ios_base::binary);
	//std::cout << lexicon.at(t).get_start();
	Lexicon lex = LEXICON.at(t);
	lp->inverted.seekg(lex.get_start());
	lp->doc_num = lex.get_docNum();
	lp->did = 0;
	lp->did = decodeNext(lp);
	return lp;
}


int nextGEQ(ListPointer *lp, int k) {
	while (lp->did >= 0 and lp->did < k) {
		lp->did = decodeNext(lp);
	}
	return lp->did;
}


int getFreq(ListPointer *lp) { return lp->freq; }


void closeList(ListPointer *lp) {
	lp->inverted.close();
	free(lp);
}


float BM25(std::vector<std::string> words, std::vector<int> freqs, int did) {
	float score = 0;
	float k1 = 1.2, b = 0.75;
	float K = k1 * ((1 - b) + b * DOCS[did - 1].getTokenNum() / avg_len);
	for (int i = 0; i < words.size(); i++) {
		int fdt = freqs[i];
		if (fdt == 0) continue;
		int ft = LEXICON.at(words[i]).get_docNum();
		long int N = DOCS.size();
		score += log10(((double)N - ft + 0.5) / (ft + 0.5)) * (k1 + 1.0) * (float)fdt / (float)(K + fdt);
	}
	return score;
}


// using sort for debug
void query_process(std::string query, std::vector<Document> &r_docs, std::vector<float> &scores) {
	std::vector<std::string> words;
	std::vector<ListPointer *> lps;
	split(query, ' ', words);
	// TODO: word not in lexicon
	for (int i = 0; i < words.size(); i++)
		lps.push_back(openList(words[i]));
	// TODO: sort lps

	int did = 0;
	while (true) {
		// get next post from shortest list
		did = nextGEQ(lps[0], did);
		if (did < 0) break;
		// see if this id exist in other lists
		int d = did;
		for (int i = 1; (i < words.size()) && (d = nextGEQ(lps[i], did)) == did; i++);
		if (d < 0) break;

		if (d > did) did = d;
		else {
			//std::cout << did << std::endl;
			std::vector<int> freqs;
			// docId is in intersection, get all frequencies
			for (int i = 0; i < words.size(); i++)
				freqs.push_back(getFreq(lps[i]));
			// compute BM25 and update heap
			if (did < 30000) {
				//std::cout << did << std::endl;
				r_docs.push_back(DOCS[did - 1]);
				float score = BM25(words, freqs, did);
				scores.push_back(score);
			}
			did++;
		}
	}
	for (int i = 0; i < words.size(); i++)
		closeList(lps[i]);
}


void conjunctive_query(std::vector<std::string> words, std::priority_queue<tuple3, std::vector<tuple3>, compare> &minHeap, int k) {
	std::vector<ListPointer *> lps;
	// the same url might appear more than once in the dataset
	std::unordered_set<std::string> URLs;
	// TODO: word not in lexicon
	for (int i = 0; i < words.size(); i++)
		lps.push_back(openList(words[i]));
	// TODO: sort lps
	int did = 0;
	while (true) {
		// get next post from shortest list
		did = nextGEQ(lps[0], did);
		if (did < 0) break;
		// see if this id exist in other lists
		int d = did;
		for (int i = 1; (i < words.size()) && (d = nextGEQ(lps[i], did)) == did; i++);
		if (d < 0) break;

		if (d > did) did = d;
		else {
			// check whether encountered
			if (URLs.count(DOCS[did - 1].getUrl())) {
				did++;
				continue;
			}
			else URLs.insert(DOCS[did - 1].getUrl());
			//std::cout << did << std::endl;
			std::vector<int> freqs;
			// docId is in intersection, get all frequencies
			for (int i = 0; i < words.size(); i++)
				freqs.push_back(getFreq(lps[i]));
			// compute BM25 and update heap
			//std::cout << did << std::endl;
			float score = BM25(words, freqs, did);
			minHeap.push(std::make_tuple(did, freqs, score));
			if (minHeap.size() > k) {
				minHeap.pop();
			}
			did++;
		}
	}
	for (int i = 0; i < words.size(); i++)
		closeList(lps[i]);
}


void disjunctive_query(std::vector<std::string> words, std::priority_queue<tuple3, std::vector<tuple3>, compare> &minHeap, int k) {
	std::vector<ListPointer *> lps;
	// the same url might appear more than once in the dataset
	std::unordered_set<std::string> URLs;
	// TODO: word not in lexicon
	for (int i = 0; i < words.size(); i++)
		lps.push_back(openList(words[i]));
	// TODO: sort lps
	while (true) {
		int min_did = lps[0]->did;
		for (int i = 1; i < lps.size(); i++) {
			if (lps[i]->did > 0 && (min_did < 0 || lps[i]->did < min_did)) {
				min_did = lps[i]->did;
			}
		}
		if (min_did < 0)
			break;
		std::vector<int> freqs;
		for (int i = 0; i < lps.size(); i++) {
			if (lps[i]->did == min_did) {
				freqs.push_back(getFreq(lps[i]));
				lps[i]->did = decodeNext(lps[i]);
			}
			else
				freqs.push_back(0);
		}
		// check whether encountered
		if (URLs.count(DOCS[min_did - 1].getUrl())) continue;
		else URLs.insert(DOCS[min_did - 1].getUrl());
		// compute BM25 and update heap
		//std::cout << min_did << std::endl;
		//std::cout << temp_words.size() << std::endl;
		float score = BM25(words, freqs, min_did);
		minHeap.push(std::make_tuple(min_did, freqs, score));
		if (minHeap.size() > k) {
			minHeap.pop();
		}
	}
	for (int i = 0; i < words.size(); i++)
		closeList(lps[i]);
}


void startup() {
	// load the Url Table to vector "docs"
	std::ifstream doc_file(doc_filename);
	long int total_numTokens = 0;
	if (doc_file.is_open()) {
		while (!doc_file.eof()) {
			std::string line;
			std::getline(doc_file, line);
			if (line != "") {
				/*std::istringstream s(line);
				std::vector<std::string> words;
				std::string word;
				while (std::getline(s, word, ' ')) {
					words.push_back(word);
				}*/
				std::string words[2];
				split(line, ' ', words);
				int token_number = stoi(words[1]);
				total_numTokens += token_number;
				DOCS.push_back(Document(words[0], token_number));
			}
		}
		doc_file.close();
	}
	avg_len = (float)total_numTokens / (float)DOCS.size();

	// load the Lexicon table to map "lexicon"
	std::ifstream lexicon_file(lexicon_filename);
	std::string line;
	std::string word;
	if (lexicon_file.is_open()) {
		while (!lexicon_file.eof()) {
			std::getline(lexicon_file, line);
			//std::cout << line << std::endl;
			if (line != "") {
				// TODO: work on this
				//std::istringstream s(line);
				////std::vector<std::string> words;
				//while (std::getline(s, word, ' ')) {
				//	//words.push_back(word);
				//}
				std::string words[4];
				split(line, ' ', words);
				LEXICON.insert(std::make_pair(words[0], Lexicon(stoi(words[1]), stoi(words[2]), stoi(words[3]))));
			}
		}
		lexicon_file.close();
	}
}

void test() {
	std::string words[3];
	split("I love you", ' ', words);
	std::vector<std::string> words1;
	split("I love you", ' ', words1);
	assert(words[0] == "I");
	assert(words[1] == "love");
	assert(words[2] == "you");
	assert(words1[0] == "I");
	assert(words1[1] == "love");
	assert(words1[2] == "you");
}


bool sortbysec(const std::pair<Document, float> &a, const std::pair<Document, float> &b) {
	return (a.second > b.second);
}


static int callback(void *data, int argc, char **argv, char **azColName) {
	int i;
	fprintf(stderr, "%s", (const char*)data);
	std::string *text = static_cast<std::string*>(data);
	text->assign(argv[1]);
	/*for (i = 0; i < argc; i++) {
		printf("%s = %s", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");*/
	return 0;
}


int show_snippet(int idx, std::vector<std::string> words) {
	//int idx = 1;
	//std::vector<std::string> words;
	//words.push_back("Atom");
	//std::vector<int> freqs;
	sqlite3 *db;
	char *zErrMsg = 0;
	int rc;
	std::string sql;
	//const char* data = "Callback function called";

	// Open database
	rc = sqlite3_open("I:/sqlite_db/Docs.db", &db);
	if (rc) {
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		return 0;
	}
	else {
		//fprintf(stderr, "Opened database successfully\n");
	}

	// Create select SQL statement
	sql = "SELECT * FROM docs WHERE id = " + std::to_string(idx) + ";";

	// Execute SQL statement
	std::string text;
	rc = sqlite3_exec(db, sql.c_str(), callback, &text, &zErrMsg);

	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	else {
		//fprintf(stdout, "Operation done successfully\n");
	}

	// Get snippet
	std::vector<std::string> lines;
	split(text, '\n', lines);
	int line_num = 0;
	for (auto line : lines) {
		if (line_num > 10) break;
		for (auto word : words) {
			size_t found = line.find(word);
			if (found != std::string::npos) {
				line_num += 1;
				// pay attention that size_t is unsigned, cannot use subtract
				size_t start = found >= 50 ? found - 50 : 0;
				size_t len = start + 100 <= line.size() ? 100 : line.size() - start;
				//std::cout << found << ' ' << start << ' ' << len << ' ' << line.size() << ' ' << std::endl;
				std::cout << "..." << line.substr(start, len);
			}
		}
	}
	if (line_num > 0) std::cout << "..." << std::endl;
	sqlite3_close(db);
	return 0;
}


int main() {
	//test();
	startup();
	std::string query;
	int type;
	int k = 10;
	while (true) {
		std::cout << "Query: ";
		std::getline(std::cin, query);
		std::vector<std::string> words;
		split(query, ' ', words);
		std::priority_queue<tuple3, std::vector<tuple3>, compare> minHeap;
		std::cout << "Type (0 for conjunctive, 1 for disjunctive): ";
		std::cin >> type;
		switch (type)
		{
		case 0:
			conjunctive_query(words, minHeap, k);
			break;
		case 1:
			disjunctive_query(words, minHeap, k);
			break; 
		default:
			break;
		}
		int result_idx = 1;
		while (!minHeap.empty()) {
			// show some info
			auto p = minHeap.top();
			std::cout << result_idx <<  ". ";
			int idx = std::get<0>(p) - 1;
			// url
			std::cout << DOCS[idx].getUrl() << std::endl;
			// each term frequency
			auto freqs = std::get<1>(p);
			for (int i = 0; i < freqs.size(); i++) {
				std::cout << words[i] << ": " << freqs[i] << std::endl;
			}
			std::cout << "Score: " << std::get<2>(p) << std::endl;
			// snippet text 200 characters around the term
			std::cout << "Snippet: " << std::endl;
			show_snippet(std::get<0>(p), words);
			std::cout << std::endl;
			minHeap.pop();
			result_idx += 1;
		}
		//conjunctive_query("horror movies", minHeap, 20);
		//disjunctive_query("horror movies", minHeap, 20);
		std::cout << "Enter q to quit:" << std::endl;
		char c;
		std::cin >> c;
		if (c == 'q') break;
		std::cout << std::endl;
		std::cin.ignore(1, '\n');
	}
	// test using sort
	/*std::vector<Document> r_docs;
	std::vector<float> scores;
	query_process("horror movies", r_docs, scores);
	std::vector<std::pair<Document, float>> vec;
	for (int i = 0; i < r_docs.size(); i++)
		vec.push_back(std::make_pair(r_docs[i], scores[i]));
	std::sort(vec.begin(), vec.end(), sortbysec);*/
	system("pause");
	return 0;	
}