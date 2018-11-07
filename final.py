import struct
import sys
import argparse
import pickle
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Final stage of building inverted indx")
    parser.add_argument('sorted', help = "completedly sorted file after sort and merge")
    args = parser.parse_args()

    start_t = time.time()
    lexicon = {}
    rec_size = 12
    post_size = 8

    word_idx, start, end, num = 0, 0, 0, 0
    with open(args.sorted, 'rb') as f, open("inverted", 'wb') as outf:
        byte = f.read(rec_size)
        cur_idx = struct.unpack('l',byte[:4])[0]
        end += post_size
        num += 1
        outf.write(byte[4:8])
        outf.write(byte[8:])
        while byte != b'':
            byte = f.read(rec_size)
            if byte == b'':
                break
            outf.write(byte[4:8])
            outf.write(byte[8:])
            cur_idx = struct.unpack('l',byte[:4])[0]
            # if new idx, record previous in lexicon
            if cur_idx != word_idx:
                lexicon[word_idx] = (start, end, num)
                word_idx = cur_idx
                start = end + post_size
                num = 0
            end += post_size
            num += 1
        lexicon[word_idx] = (start, end, num)


    with open('word2idx.pickle', 'rb') as f:
        word2idx = pickle.load(f)

    for k in word2idx:
        word2idx[k] = lexicon[word2idx[k]]

    with open('lexicon.pickle', 'wb') as outf:
        pickle.dump(word2idx, outf, protocol=pickle.HIGHEST_PROTOCOL)

    end_t = time.time()
    print("Final stage time ", end_t - start_t, " seconds")
