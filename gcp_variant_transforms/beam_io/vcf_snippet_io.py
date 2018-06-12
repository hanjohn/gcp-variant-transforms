# Copyright 2018 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A source for reading VCF file headers."""

from __future__ import absolute_import

from typing import Iterable, Tuple

from apache_beam.io import filebasedsource
from apache_beam.io import range_trackers  # pylint: disable=unused-import
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import iobase
from apache_beam import transforms

from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.beam_io import vcfio


class _VcfSnippetSource(filebasedsource.FileBasedSource):
  """A source for reading a limited number of variants from a set of VCF files.

  Lines that are malformed are skipped.

  Parses VCF files (version 4) using PyVCF library.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(self,
               file_pattern,
               snippet_size,
               compression_type=filesystem.CompressionTypes.AUTO,
               validate=True):
    # type: (str, int, str, bool) -> None
    super(_VcfSnippetSource, self).__init__(file_pattern,
                                            compression_type=compression_type,
                                            validate=validate,
                                            splittable=False)
    self._compression_type = compression_type
    self._snippet_size = snippet_size

  def read_records(
      self,
      file_name,  # type: str
      range_tracker  # type: range_trackers.UnsplittableRangeTracker
      ):
    # type: (...) -> Iterable[Tuple[str, str, vcfio.Variant]]
    """Iterator to emit lines encoded as `Variant` objects."""
    record_iterator = vcf_parser.PyVcfParser(
        file_name,
        range_tracker,
        self._pattern,
        self._compression_type,
        allow_malformed_records=True,
        representative_header_lines=None,
        buffer_size=self.DEFAULT_VCF_READ_BUFFER_SIZE,
        skip_header_lines=0)

    # Open distinct channel to read lines as raw bytestrings.
    with filesystems.FileSystems.open(file_name,
                                      self._compression_type) as raw_reader:
      line = raw_reader.readline()
      while line and line.startswith('#'):
        # Skip headers, assume header size is negligible.
        line = raw_reader.readline()

      count = 0
      for encoded_record in record_iterator:
        raw_record = raw_reader.readline()

        if count >= self._snippet_size:
          break
        if not isinstance(encoded_record, vcfio.Variant):
          continue

        count += 1
        yield file_name, raw_record, encoded_record


class ReadVcfSnippet(transforms.PTransform):
  """A PTransform for reading a limited number of lines from a set of VCF files.

  Output will be a PTable mapping from `file names -> Tuple[(line, Variant)]`
  objects. The list contains the first `snippet_size` number of lines that are
  not malformed, first as a raw string and then encoded as a `Variant` class.

  Parses VCF files (version 4) using PyVCF library.
  """

  def __init__(
      self,
      file_pattern,  # type: str
      snippet_size,  # type: int
      compression_type=filesystem.CompressionTypes.AUTO,  # type: str
      validate=True,  # type: bool
      **kwargs  # type: **str
  ):
    # type: (...) -> None
    """Initialize the :class:`ReadVcfHeaders` transform.

    Args:
      file_pattern: The file path to read from either as a single file or a glob
        pattern.
      snippet_size: The number of lines that should be read from the file.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate: Flag to verify that the files exist during the pipeline creation
        time.
    """
    super(ReadVcfSnippet, self).__init__(**kwargs)
    self._source = _VcfSnippetSource(
        file_pattern, snippet_size, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | iobase.Read(self._source)
