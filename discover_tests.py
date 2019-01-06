#!/usr/bin/env python3

import sys

def main():
    prefix = ''
    for line in sys.stdin:
        if line.startswith(' '):
            try:
                line = line[:line.find('#')]
            except ValueError:
                pass
            print(prefix + line.strip())
        else:
            prefix = line.strip()


if __name__ == '__main__':
    main()
