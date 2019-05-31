import os
import sys
import errno
import logging
from tempfile import TemporaryDirectory
from operator import lt as lt_comparator
from functools import cmp_to_key


logging.basicConfig(level=logging.DEBUG)

MB = 1024 * 1024


def iget_line(filepath):
    with open(filepath, 'r') as f:
        for line in f:
            yield line.rstrip()


def iget_batch(filepath, max_mem):
    batch, size = [], 0
    for line in iget_line(filepath):
        line_size = sys.getsizeof(line)
        size += line_size
        if size > max_mem:
            yield batch
            batch, size = [line], line_size
        else:
            batch.append(line)
    if batch:
        yield batch


def read_line_by_pos(filepath, pos):
    with open(filepath) as f:
        f.seek(pos, os.SEEK_SET)
        line = f.readline().rstrip()
        new_pos = f.tell()
    if new_pos == pos:
        raise StopIteration()
    return line, new_pos


class LineGetter(object):
    def __init__(self, fp, always_open):
        self._fp = fp
        self._f = None
        # need to track pos when file is closed
        self._current_pos = None
        self._always_open = always_open
        self.line = None
        self.ended = True
        self._init()

    def _init(self):
        self.ended = False
        if self._always_open:
            self._f = open(self._fp)
        else:
            self._current_pos = 0
        self.read_next()

    def read_next(self):
        try:
            if self._always_open:
                self.line = next(self._f).rstrip()
            else:
                self.line, self._current_pos = read_line_by_pos(self._fp, self._current_pos)
        except StopIteration:
            self.ended = True
            self.line = None
            if self._always_open:
                self._f.close()


class Merger(object):
    def __init__(self, filepaths, out, open_all, comparator=lt_comparator, encoding=None):
        self._line_getters = [LineGetter(filepath, open_all) for filepath in filepaths]
        self._out = out
        self._comparator = comparator
        self._encoding = encoding

    def _get_next_min_line(self):
        min_line, next_line_idx = '', -1
        for idx, line_getter in enumerate(self._line_getters):
            if line_getter.ended:
                continue
            if next_line_idx == -1:
                min_line, next_line_idx = line_getter.line, idx
            elif self._comparator(line_getter.line, min_line):
                min_line = line_getter.line
                next_line_idx = idx
        if next_line_idx >= 0:
            self._line_getters[next_line_idx].read_next()
            return min_line
        return None

    def _iget_next_min_line(self):
        last_line = self._get_next_min_line()
        while last_line is not None:
            yield last_line
            last_line = self._get_next_min_line()

    def merge(self):
        with open(self._out, 'w', encoding=self._encoding) as out_f:
            for line in self._iget_next_min_line():
                out_f.write(line+'\n')


class FileSorter(object):
    def __init__(self, inp, out, max_mem=1024*MB, fd_count=1024, comparator=lt_comparator, encoding=None):
        self._inp, self._out = inp, out
        self._max_mem = max_mem
        self._comparator = comparator
        self._fd_count = fd_count
        self._encoding = encoding
        self._tmpdir = TemporaryDirectory()
        self._check_arguments()

    def _check_arguments(self):
        if not os.path.exists(self._inp):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self._inp)

    def _form_filepath(self, idx):
        return os.path.join(self._tmpdir.name, '{}.part'.format(idx))

    def _sort_batch(self, batch):
        comparator_int = lambda x, y: -int(self._comparator(x, y))*2+1
        batch.sort(key=cmp_to_key(comparator_int))

    def _dump(self, batch, fp):
        with open(fp, 'w', encoding=self._encoding) as f:
            for sent in batch:
                f.write(sent+'\n')

    def sort(self):
        # sort and dump batches
        filepaths = []
        for idx, batch in enumerate(iget_batch(self._inp, self._max_mem)):
            fp = self._form_filepath(idx)
            self._sort_batch(batch)
            self._dump(batch, fp)
            filepaths.append(fp)
        logging.debug('A file was separated into {} files'.format(len(filepaths)))
        # merge batches
        # check if can open all files at the same time. One for out file
        open_all = len(filepaths) <= self._fd_count - 1
        merger = Merger(filepaths, self._out, open_all, comparator=self._comparator, encoding=self._encoding)
        merger.merge()
        logging.info('Sorted filepath: {}'.format(self._out))


class LongLinesFileSorter(object):
    def __init__(self, inp, out, max_mem=1024*MB, comparator=lt_comparator, encoding=None):
        self._inp, self._out = inp, out
        self._max_mem = max_mem
        self._comparator = comparator
        self._encoding = encoding
        self._lines_positions = []
        self._set_line_positions()

    def _set_line_positions(self):
        lines_separator = '\n'
        current_pos = 0
        with open(self._inp, encoding=self._encoding) as f:
            while True:
                symbol = f.read(1)
                if not symbol:
                    break
                if symbol == lines_separator:  # next line
                    lines_positions.append(current_pos)
                current_pos += 1

    def sort(self):
        n_lines = len(self._lines_positions)
        # max size of line to read
        chunk_size = self._max_mem // n_lines - sys.getsizeof('')
        # TODO: ... (run out of 4 hours)

