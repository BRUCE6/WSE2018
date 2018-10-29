import argparse
import os
from urllib.request import urlretrieve

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download the compressed files")
    parser.add_argument('wet', help = "the path for WET.paths file from commoncrawler")
    parser.add_argument('n', help = "the number of compressed files to download")
    args = parser.parse_args()
    with open(args.wet, 'r') as f:
        content = f.read()
        lines = content.split('\n')
    lines = lines[:-1]

    if not os.path.exists('gzs'):
        os.mkdir('gzs')
    pre = 'https://commoncrawl.s3.amazonaws.com/'
    for i in range(int(args.n)):
        print("Downloading file", i)
        local, headers = urlretrieve(pre + lines[1], filename = 'gzs/' + str(i) +'.gz')