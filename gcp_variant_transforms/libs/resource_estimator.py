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

"""Helper functions for estimating the resources that a vcf_to_bq will require.

Currently, the resource estimator only estimates the disk usage that a Dataflow
pipeline will take along with the `MergeVariants` step, since this can cause
expensive pipeline failures late in the run.
"""
from typing import Any, Dict, List, Tuple
import logging

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import filesystem
from apache_beam.io import filesystems

from gcp_variant_transforms.beam_io import vcfio


# TODO(hanjohn): Add unit tests.

def _convert_variant_snippets_to_bytesize(variant):
  # type: (vcfio.Variant) -> int
  return coders.registry.get_coder(vcfio.Variant).estimate_size(variant)


class _VariantsSizeInfo(object):
  def __init__(self,
               raw_snippet_size,  # type: int
               encoded_snippet_size,  # type: int
              ):
    # type: (...) -> None
    self.raw_size = raw_snippet_size
    self.encoded_size = encoded_snippet_size


class FileSizeInfo(object):
  def __init__(self, raw_file_size, encoded_file_size=None):
    # type: (int, int) -> None
    self.raw_size = raw_file_size
    self.encoded_size = encoded_file_size

  def calculate_encoded_file_size(self, snippet_size_info):
    # type: (_VariantsSizeInfo) -> None
    """Estimates a VCF file's encoded size based on snippet analysis.

    Given the raw_file_size and measurements of several VCF lines from the file,
    estimate how much disk the file will take after expansion due to encoding
    lines as `vcfio.Variant` objects. The encoded_snippet_size will be set as
    `self.encoded`.

    This is a simple ratio problem, solving for encoded_snippet_size which is
    the only unknown:
    encoded_snippet_size / raw_snippet_size = encoded_file_size / raw_file_size
    """
    if snippet_size_info.raw_size == 0:
      # Propagate in-band error state to avoid divide-by-zero.
      self.encoded_size = 0
      self.raw_size = 0
    else:
      self.encoded = (self.raw_size * snippet_size_info.encoded_size /
                      snippet_size_info.raw_size)


def measure_variant_size((file_path, raw_variant, encoded_variant,)):
  # type: (Tuple[str, str, vcfio.Variant]) -> Tuple[str, _VariantsSizeInfo]
  """Measures the lengths of the raw and encoded representations of a Variant.

  Given a PTable mapping file_paths to the raw (bytestring) and vcfio.Variant-
  encoded representations of a Variant line, have the output PTable instead map
  from the file_paths to a Tuple with the (raw, encoded) representation sizes.

  The file_path keys are not expected to be unique.
  """
  encoded_variant_size = _convert_variant_snippets_to_bytesize(encoded_variant)
  raw_variant_size = len(raw_variant)
  return file_path, _VariantsSizeInfo(raw_variant_size, encoded_variant_size)


def estimate_file_encoded_size((file_name, metrics,)):
  # type: (Tuple[str, Dict[str, Any]]) -> Tuple[str, FileSizeInfo]
  file_size_info = metrics['whole_file_raw_size'][0]  # type: FileSizeInfo
  snippet_size_info = metrics['snippet_stats'][0]  # type: _VariantsSizeInfo

  # Assume that the ratio of encoded size to raw disk size is roughly the same
  # throughout the file compared to the first several lines.
  file_size_info.calculate_encoded_file_size(snippet_size_info)
  if file_size_info.raw_size == 0:
    logging.error("VCF file %s reported with 0 well-formed variant lines; "
                  "its contribution to disk resource usage will be "
                  "ignored.", file_name)

  logging.debug("File %s has raw file size %d, raw snippet size %d, encoded "
                "size %d. Estimated encoded file size: %d",
                file_name, file_size_info.raw_size, snippet_size_info.raw_size,
                snippet_size_info.encoded_size, file_size_info.encoded_size)
  return file_name, file_size_info

def get_file_sizes(input_pattern):
  # type: (str) -> List[FileSizeInfo]
  match_results = filesystems.FileSystems.match([input_pattern])
  file_sizes = []
  for match in match_results:
    for file_metadata in match.metadata_list:
      compression_type = filesystem.CompressionTypes.detect_compression_type(
          file_metadata.path)
      if compression_type != filesystem.CompressionTypes.UNCOMPRESSED:
        logging.error("VCF file %s is compressed; disk requirement estimator "
                      "will not be accurate.", file_metadata.path)

      file_sizes.append((file_metadata.path,
                         FileSizeInfo(file_metadata.size_in_bytes),))
  return file_sizes


class _VariantsSizeInfoSumFn(beam.CombineFn):
  """Combiner Function to sum up the size fields of _VariantsSizeInfos.

  Example: [_VariantsSizeInfo(a, b), _VariantsSizeInfo(c, d)] ->
            _VariantsSizeInfo(a+c, b+d)
  """
  def create_accumulator(self):
    # type: (None) -> Tuple[int, int]
    return (0, 0)  # (raw, encoded) sums

  def add_input(self, sums, snippet_size_info):
    # type: (Tuple[int, int], _VariantsSizeInfo) -> Tuple[int, int]
    return (sums[0] + snippet_size_info.raw_size,
            sums[1] + snippet_size_info.encoded_size)

  def merge_accumulators(self, accumulators):
    # type: (Iterable[Tuple[int, int]]) -> Tuple[int, int]
    first, second = zip(*accumulators)
    return sum(first), sum(second)

  def extract_output(self, sums):
    # type: (Tuple[int, int]) -> _VariantsSizeInfo
    return _VariantsSizeInfo(*sums)


class FileSizeInfoSumFn(beam.CombineFn):
  """Combiner Function to sum up the size fields of Tuple[str, FileSizeInfo]s.

  Unlike _VariantsSizeInfoSumFn, the input is a PTable mapping str to
  FileSizeInfo, so the input is a tuple with the FileSizeInfos as the second
  field. The output strips out the str key which represents the file path.

  Example: [('/path/a', FileSizeInfo(a, b)), ('/path/b;, FileSizeInfo(c, d))] ->
            FileSizeInfo(a+c, b+d)
  """
  def create_accumulator(self):
    # type: (None) -> Tuple[int, int]
    return (0, 0)  # (raw, encoded) sums

  def add_input(self, raw_encoded, path_and_file_size_info):
    # type: (Tuple[int, int], Tuple[str, FileSizeInfo]) -> Tuple[int, int]
    raw, encoded = raw_encoded
    _, file_size_info = path_and_file_size_info
    return raw + file_size_info.raw_size, encoded + file_size_info.encoded_size

  def merge_accumulators(self, accumulators):
    # type: (Iterable[Tuple[int, int]]) -> Tuple[int, int]
    raw, encoded = zip(*accumulators)
    return sum(raw), sum(encoded)

  def extract_output(self, raw_encoded):
    # type: (Tuple[int, int]) -> FileSizeInfo
    raw, encoded = raw_encoded
    return FileSizeInfo(raw, encoded)
