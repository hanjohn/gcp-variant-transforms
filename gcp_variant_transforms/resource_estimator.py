"""TODO(hanjohn): DO NOT SUBMIT without one-line documentation for resource_estimator.

TODO(hanjohn): DO NOT SUBMIT without a detailed description of resource_estimator.
TODO(hanjohn): move this into gcp_variant_transforms
"""

from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.options import variant_transform_options
# TODO(hanjohn) Refactor deps on vcf_to_bq module to a lib.
from gcp_variant_transforms import vcf_to_bq

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
]


class WriteAndLogBytesize(WriteToText):
  """Thin wrapper around apache_beam.io.WriteToText for logging."""

  def __init__(self, file_path_prefix):
    super(WriteAndLogBytesize, self).__init__(
        file_path_prefix=file_path_prefix,
        num_shards=1,
        shard_name_template='')

  def expand(self, pcoll):
    # Log size of bytes so that the file with the bytesize doesn't have to be
    # opened in a separate step.
    pcoll | beam.ParDo(
        lambda x : logging.info ("PCollections' data size: %d bytes", x))
    return super(WriteAndLogBytesize, self).expand(pcoll)


def _getSizeOfInputFiles(input_patterns):
  match_results = FileSystems.match(input_patterns)
  logging.warning("annyeong: %s", str(match_results[0].metadata_list))
  # for match in match_results:
    # for file_metadata in match.metadata_list:
      # logging.warning("hello: %s %fGB", file_metadata.path, file_metadata.size_in_bytes / 1e9)


def run(argv=None):
  """Runs resource estimator for VCF to BigQuery pipeline."""
  known_args, pipeline_args = vcf_to_bq.parse_and_validate_args(
      argv, _COMMAND_LINE_OPTIONS)

  _getSizeOfInputFiles([known_args.input_pattern])

  ## file_metadata = match_results[0].metadata_list
  #file_pattern = match_results[0].pattern
  ## logging.warning("hello: " + str(vars(file_metadata[0])))
  #logging.warning("hello: " + str(file_pattern))
  ## for i in 

  pipeline_options = PipelineOptions(pipeline_args)
  # with beam.Pipeline(options=pipeline_options) as p:
    # variants = vcf_to_bq.read_variants(p, known_args)
    # _ = (variants
         # | 'GetElementSizes' >> beam.Map(vcfio.getElementSizeFn)
         # | 'SumAllElementSizes' >> beam.CombineGlobally(sum)
         # | 'PrintBytesize' >> WriteAndLogBytesize('pipeline_output.txt'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
