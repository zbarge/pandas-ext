from pandas_ext.ios import read_csv, read_text, read_file
import os

if __name__ == '__main__':
    counter = 1
    def count():
        global counter
        t = (1,2,3)
        for i, v in enumerate(t):
            counter += i + v
    count()
    print(counter)

