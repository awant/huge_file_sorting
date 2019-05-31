import pytest
import os
from generator import FileGenerator
from file_sorter import FileSorter
from tempfile import TemporaryDirectory


def read_lines(filepath):
    with open(filepath) as f:
        return [line.rstrip() for line in f]


def test_wrong_path():
    with pytest.raises(FileNotFoundError):
        file_sorter = FileSorter(inp='inp.txt', out='out.txt')


def test_empty_file():
    tmp_dir = TemporaryDirectory()
    inp_file = os.path.join(tmp_dir.name, 'input.txt')
    out_file = os.path.join(tmp_dir.name, 'output.txt')

    file_generator = FileGenerator(lines_count=0, maxlen=1, out=inp_file, force=True)
    file_generator.generate()
    FileSorter(inp=inp_file, out=out_file).sort()


def test_one_batch_sort():
    generated_lines = []
    sorted_lines_reference = []
    sorted_lines = []

    tmp_dir = TemporaryDirectory()
    inp_file = os.path.join(tmp_dir.name, 'input.txt')
    out_file = os.path.join(tmp_dir.name, 'output.txt')

    file_generator = FileGenerator(lines_count=1000, maxlen=10, out=inp_file, force=True)
    file_generator.generate()
    file_sorter = FileSorter(inp=inp_file, out=out_file)
    file_sorter.sort()

    sorted_lines_reference = sorted(read_lines(inp_file))
    sorted_lines = read_lines(out_file)

    assert sorted_lines == sorted_lines_reference


def test_several_batches_sort():
    generated_lines = []
    sorted_lines_reference = []
    sorted_lines = []

    tmp_dir = TemporaryDirectory()
    inp_file = os.path.join(tmp_dir.name, 'input.txt')
    out_file = os.path.join(tmp_dir.name, 'output.txt')

    file_generator = FileGenerator(lines_count=1000, maxlen=100, out=inp_file, force=True)
    file_generator.generate()
    file_sorter = FileSorter(inp=inp_file, out=out_file, max_mem=1024)
    file_sorter.sort()

    sorted_lines_reference = sorted(read_lines(inp_file))
    sorted_lines = read_lines(out_file)

    assert sorted_lines == sorted_lines_reference


def test_several_fds():
    generated_lines = []
    sorted_lines_reference = []
    sorted_lines = []

    tmp_dir = TemporaryDirectory()
    inp_file = os.path.join(tmp_dir.name, 'input.txt')
    out_file = os.path.join(tmp_dir.name, 'output.txt')

    file_generator = FileGenerator(lines_count=1000, maxlen=100, out=inp_file, force=True)
    file_generator.generate()
    file_sorter = FileSorter(inp=inp_file, out=out_file, max_mem=1024, fd_count=3)
    file_sorter.sort()

    sorted_lines = read_lines(out_file)
    assert sorted_lines == sorted(read_lines(inp_file))


def test_huge_file():
    generated_lines = []
    sorted_lines_reference = []
    sorted_lines = []

    tmp_dir = TemporaryDirectory()
    inp_file = os.path.join(tmp_dir.name, 'input.txt')
    out_file = os.path.join(tmp_dir.name, 'output.txt')

    # About 9 GB
    file_generator = FileGenerator(lines_count=2000000, maxlen=10000, out=inp_file, force=True)
    file_generator.generate()
    # Sort 9 GB file
    file_sorter = FileSorter(inp=inp_file, out=out_file)
    file_sorter.sort()

    sorted_lines = read_lines(out_file)
    assert sorted_lines == sorted(read_lines(inp_file))
