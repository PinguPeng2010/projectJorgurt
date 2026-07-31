import argparse

parser = argparse.ArgumentParser()

parser.add_argument('arg', type=str, help='Argument')
parser.add_argument('--argu','-a', type=str, help='Argument')


args = parser.parse_args()
print(args.arg)
print(args.argu)