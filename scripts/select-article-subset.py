import random
import argparse
import os
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-filename",   required=True,  help="Filename of the source file")
    parser.add_argument("-o", "--output-filename",  required=True,  help="Filename of the output file")
    parser.add_argument("-n", "--num-articles",     required=True,  help="Number of article ids to select from the original file", type=int)
    parser.add_argument("-s", "--seed",             required=False, help="Random seed for selection", type=int)
    args = parser.parse_args()

    # if applicable, set seed
    if args.seed is not None: random.seed(args.seed)

    # load input file and split it
    try:
        with open(args.input_filename) as f:
            input_lines = f.read().splitlines()
    except FileExistsError:
        sys.exit('File "{0}" not found'.format(args.input_filename))

    # select a random subset of the original file
    output_lines = random.sample(input_lines, args.num_articles)

    # open output file and write to it
    try:
        with open(args.output_filename, 'w') as f:
            f.write('\n'.join(output_lines))
    except:
        sys.exit('Unable to open file "{0}"'.format(args.output_filename))