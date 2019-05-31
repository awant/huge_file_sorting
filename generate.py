import argparse
import logging
from generator import FileGenerator


logging.basicConfig(level=logging.INFO)


def configure_args():
    parser = argparse.ArgumentParser(description='Generating a huge file')
    parser.add_argument('-out', '--out', type=str, default='huge_file.txt', help='path to generated file')
    parser.add_argument('-lines_count', '--lines_count', type=int, required=True, help='number of lines to generate')
    parser.add_argument('-maxlen', '--maxlen', type=int, required=True, help='max len of every line')
    parser.add_argument('-force', '--force', action='store_true', help='replace out file even if it exists')
    return parser.parse_args()


if __name__ == '__main__':
    args = configure_args()
    file_generator = FileGenerator(**vars(args))
    file_generator.generate()
