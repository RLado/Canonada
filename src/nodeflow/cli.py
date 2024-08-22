import sys
from . import example

def main():
    args = sys.argv[1:]
    example.greet(args[0] if args else "World")

if __name__ == "__main__":
    main()