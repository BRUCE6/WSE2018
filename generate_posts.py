import gzip
import time
import re
import sys
import struct
import pickle
import argparse
from urllib.request import urlretrieve


def parseWet(content):
    # parse the wet file
    docs = content.split('WARC-Type: conversion\r\n')
    docs = docs[1:]
    return docs


def int2b(n):
    # convert python int to 4 bytes binary data
    return (n).to_bytes(4, byteorder='little')


def isEnglish(word):
    if not word:
        return False
    for c in word:
        if not ((ord(c) >= ord('a') and ord(c) <= ord('z')) or 
                (ord(c) >= ord('A') and ord(c) <= ord('Z')) or 
                (ord(c) >= ord('0') and ord(c) <= ord('9'))):
            return False
    return True


def appendPosts(start_idx, docs, word2idx, pf, df):
    for idx, doc in enumerate(docs):
        num_terms = 0
        if not idx % 10000:
            print('doc', idx)
        tmp_post = {}
        lines = doc.split('\n')
        lines = [line.strip() for line in lines]
        for i, line in enumerate(lines):
            if line[:len('Content-Length')] == 'Content-Length':
                break
        for line_idx, line in enumerate(lines[i + 2:]):
            # consider the word boundary
            words = re.split('[,. :-=]', line)
            for pos, word in enumerate(words):
                # make sure each term is a english word
                if not isEnglish(word):
                    continue
                num_terms += 1                    
                if word not in word2idx:
                    word2idx[word] = len(word2idx)
                if word2idx[word] not in tmp_post:
                    tmp_post[word2idx[word]] = []
                tmp_post[word2idx[word]].append((line_idx, pos))
        if num_terms:
            df.write(lines[0][len('WARC-Target-URI: '):] + ' ')
            start_idx += 1
            df.write(str(num_terms) + '\n')
            # write postings for this file
            for k, v in tmp_post.items():
                pf.write(int2b(k))
                pf.write(int2b(start_idx))
                pf.write(int2b(len(v)))
    return start_idx


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate the postings")
    parser.add_argument('n', help="number of files")
    parser.add_argument('gzfolder', help="path of the folder containing all .gz files")
    args = parser.parse_args()

    start = time.time()
    word2idx = {}
    postsfile = 'posts'
    docfile = 'docs'
    start_idx = 0
    with open(postsfile, 'wb') as pf, open(docfile, 'w') as df:
        for i in range(int(args.n)):
            filename = args.gzfolder + '/' + str(i) +'.gz'
            with open(filename, 'rb') as f:
                print("Parsing", filename)
                content = gzip.decompress(f.read())
                content = content.decode('utf-8')       
                docs = parseWet(content)
                start_idx = appendPosts(start_idx, docs, word2idx, pf, df)
    with open('word2idx.pickle', 'wb') as f:
        pickle.dump(word2idx, f, protocol=pickle.HIGHEST_PROTOCOL)
    end = time.time()
    print("Total number of web pages is", start_idx)
    print("Generating postings takes ", end - start, " seconds")
