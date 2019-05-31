import random
import string
import os
import logging


def get_file_size(path):  # in Mb
    return os.path.getsize(path) // 1024 // 1024


class FileGenerator(object):
    def __init__(self, lines_count, maxlen, out, force=False):
        self._nlines = lines_count
        self._maxlen = maxlen
        self._default_out = out
        self._check_params(force)

    def _check_params(self, force):
        assert self._nlines >= 0
        assert self._maxlen > 0
        if not force:
            assert not os.path.exists(self._default_out)

    def _igen_symbol(self, size):
        for _ in range(size):
            yield random.choice(string.ascii_letters)

    def generate(self):
        out = self._default_out
        with open(out, 'w') as f:
            for _ in range(self._nlines):
                line_length = random.randint(1, self._maxlen)
                for symbol in self._igen_symbol(line_length):
                    f.write(symbol)
                f.write('\n')

        attribs = {
            'fp': out,
            'lc': self._nlines,
            'ml': self._maxlen,
            'size': get_file_size(out)
        }
        attribs['size'] = attribs['size'] if attribs['size'] > 0 else '<0'
        logging.info('File {fp} was generated\n    '
                     'lines_count = {lc}, maxlen = {ml}, size = {size} Mb'.format(**attribs))
