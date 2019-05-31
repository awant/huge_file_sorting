import argparse
import psutil
from file_sorter import FileSorter


def configure_args():
    parser = argparse.ArgumentParser(description='Sorting a huge file')
    parser.add_argument('-inp', '--inp', type=str, default='huge_file.txt', help='path to a file for sorting')
    parser.add_argument('-out', '--out', type=str, help='path to a sorted file')
    # system limits
    parser.add_argument('-max_mem', '--max_mem', type=int, help='set limit for mem usage in bytes')
    # TODO: fd_count should get from os. Remember stdin, out, err
    parser.add_argument('-fd_count', '--fd_count', type=int, default=1024, help='max number of opened fds')
    return parser.parse_args()


def update_args(args):
    # take only a half of available mem
    get_mem_usage = lambda: psutil.virtual_memory().available // 2
    args.out = args.inp + '.sorted' if args.out is None else args.out
    args.max_mem = get_mem_usage() if args.max_mem is None else args.max_mem


if __name__ == '__main__':
    args = configure_args()
    update_args(args)
    file_sorter = FileSorter(**vars(args))
    file_sorter.sort()
