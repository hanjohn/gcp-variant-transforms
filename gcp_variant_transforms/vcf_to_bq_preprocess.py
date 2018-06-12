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

r"""Pipeline for preprocessing the VCF files.

This pipeline is aimed to help the user to easily identify and further import
the malformed/incompatible VCF files to BigQuery. It generates two files as the
output:
- Conflicts report: A file that lists the incompatible headers, undefined header
  fields, the suggested resolutions and eventually malformed records.
- Resolved headers file: A VCF file that contains the resolved fields
  definitions.

The report is generated in the ``report_path``, while the resolved headers file
is generated in ``resolved_headers_path`` if provided.

Run locally:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)> \
  --report_path <local path to the report file> \
  --resolved_headers_path <local path to the resolved headers file> \
  --report_all True

Run on Dataflow:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)>
  --report_path <cloud path to the report file> \
  --resolved_headers_path <cloud path to the resolved headers file> \
  --report_all True \
  --project gcp-variant-transforms-test \
  --job_name preprocess \
  --staging_location "gs://integration_test_runs/staging" \
  --temp_location "gs://integration_test_runs/temp" \
  --runner DataflowRunner \
  --setup_file ./setup.py
"""

import functools
import math
import logging
import sys

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.filesystems import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options import pipeline_options

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import conflicts_reporter
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_header_definitions

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.FilterOptions,
    variant_transform_options.PreprocessOptions,
    variant_transform_options.VcfReadOptions
]


# SNIPPET_SIZE = 0  # DO NOT SUBMIT
# SNIPPET_SIZE = 1  # DO NOT SUBMIT
# SNIPPET_SIZE = 10  # DO NOT SUBMIT
SNIPPET_SIZE = 100  # DO NOT SUBMIT
# SNIPPET_SIZE = 10000000  # DO NOT SUBMIT


def _get_inferred_headers(pipeline,  # type: beam.Pipeline
                          known_args,  # type: argparse.Namespace
                          merged_header  # type: pvalue.PCollection
                         ):
  # type: (...) -> (pvalue.PCollection, pvalue.PCollection)
  inferred_headers = vcf_to_bq_common.get_inferred_headers(pipeline, known_args,
                                                           merged_header)
  merged_header = (
      (inferred_headers, merged_header)
      | beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          allow_incompatible_records=True))
  return inferred_headers, merged_header


def _get_file_sizes(input_pattern):
  # type: (str) -> (List[Tuple(str, int)])
  match_results = FileSystems.match([input_pattern])
  count = 0
  total_size = 0
  file_sizes = []
  for match in match_results:
    for file_metadata in match.metadata_list:
      file_sizes.append((file_metadata.path, file_metadata.size_in_bytes,))
      count += 1
      total_size += file_metadata.size_in_bytes
      if count % 1000 == 0:
        logging.warning("done reading %d files", count)
  return file_sizes


# TODO(hanjohn): Add unit tests.
def getVariantSize(val, element_type):
  return coders.registry.get_coder(element_type).estimate_size(val)

def encodeVariant(val):
  encoded_variant = coders.registry.get_coder(vcfio.Variant).encode(val)
  return encoded_variant

def _convert_variant_snippets_to_bytesize(element, element_type):
  file_name, variant_record = element
  logging.info("From file %s: %s with size: %d",
               file_name, variant_record, getVariantSize(variant_record, element_type))
  return file_name, getVariantSize(variant_record, element_type)


def _add_snippet_raw_vcf_sizes(element):
  # type: (Tuple[str, int]) -> (Tuple[str, Tuple[int, int]])
  file_name, snippet_encoded_size = element
  compression_type = CompressionTypes.detect_compression_type(file_name)
  if compression_type != CompressionTypes.UNCOMPRESSED:
    logging.error("VCF file {} is compressed; disk requirement estimator will not be accurate.",
                  file_name)  # Fail louder?

  total_snippet_size = 0
  with FileSystems.open(file_name, compression_type) as vcf:
    for i in range(SNIPPET_SIZE): # DO NOT SUBMIT
      line = vcf.readline()
      while line and line.startswith('#'):
        # TODO: Account for header size
        line = vcf.readline()
      if not line:
        break
      logging.info("Reading raw line with len %d: %s", len(line), line[:100])
      total_snippet_size += len(line)  # TODO: May need to add byte for newline char
  if total_snippet_size == 0:
    logging.error("Something went wrong when reading the VCF, so disk requirement estimator will not be accurate.")
    # Fail more loudly? Maybe excessive

  return file_name, (snippet_encoded_size, total_snippet_size)


def _estimate_file_encoded_size(element):
  # type: (Tuple[str, Dict[str, List]]) -> (Tuple[str, Dict[str, int]])
  file_name, metrics = element
  raw_file_size = metrics['whole_file_raw_size'][0]
  encoded_snippet_size, raw_snippet_size = metrics['snippet_stats'][0]

  # Assume that ratio of encoded size to raw disk size is roughly the same throughout the file.
  encoded_file_size = raw_file_size * encoded_snippet_size / raw_snippet_size

  logging.info("File %s has raw file size %d, raw snippet size %d, encoded size %d. Estimated encoded file size: %d",
               file_name, raw_file_size, raw_snippet_size, encoded_snippet_size, encoded_file_size)
  return file_name, {'raw': raw_file_size, 'encoded_estimate': encoded_file_size}


class FileSizeSumFn(beam.CombineFn):
  def create_accumulator(self):
    return (0, 0)  # (raw, encoded) sums

  def add_input(self, raw_encoded, input):
    raw, encoded = raw_encoded
    return raw + input['raw'], encoded + input['encoded_estimate']

  def merge_accumulators(self, accumulators):
    raw, encoded = zip(*accumulators)
    return sum(raw), sum(encoded)

  def extract_output(self, raw_encoded):
    raw, encoded = raw_encoded
    return {'raw': raw, 'encoded_estimate': encoded}


def run(argv=None):
  # type: (List[str]) -> (str, str)
  """Runs preprocess pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  options = pipeline_options.PipelineOptions(pipeline_args)
  pipeline_mode = vcf_to_bq_common.get_pipeline_mode(known_args)

  logging.info("************" + str(_get_file_sizes(known_args.input_pattern)))

  with beam.Pipeline(options=options) as p:
    headers = vcf_to_bq_common.read_headers(p, pipeline_mode, known_args)
    merged_headers = vcf_to_bq_common.get_merged_headers(headers)
    merged_definitions = (headers
                          | 'MergeDefinitions' >>
                          merge_header_definitions.MergeDefinitions())
    inferred_headers_side_input = None

    # Resource estimator
    # TODO: Put this in a function
    files_to_raw_size = (p
                      | 'InputFilePattern' >> beam.Create([known_args.input_pattern])
                      | 'GetRawFileSizes' >> beam.FlatMap(lambda pattern: _get_file_sizes(pattern)))
    vcf_snippets = vcf_to_bq_common.read_variants(p, known_args,
                                                  snippet_size_limit=SNIPPET_SIZE, key_output_by_file=True)
    # DO NOT SUBMIT
    # Dumps encoded variants to plaintext file for debugging
    # vcf_snippets | 'WriteToText' >> beam.io.WriteToText('localvcf.out')
    # (vcf_snippets | 'Encode' >> beam.Map(encodeVariant)
    #               # | 'WriteEncodedToText' >> beam.io.WriteToText('localvcfencoded.out')
    #               | 'WriteEncodedToText' >> beam.io.WriteToText('gs://my-bucket-hanjohn-vt-test/outputs/vcf_to_bq_printoutputestimator')
    # )

    # files_to_snippet_sizes: (file_name<str>, {'encoded_estimate': <int>, 'raw': <int>})
    files_to_snippet_sizes = (vcf_snippets
                             | 'ConvertSnippetLinesToBytesize' >> beam.Map(functools.partial(_convert_variant_snippets_to_bytesize, element_type=vcf_snippets.element_type))
                             | 'AggregateSnippetSizePerFile' >> beam.CombinePerKey(sum)
                             # TODO: `ReadRawSnippetFileSizes` a ParDo probably slows down the pipeline, it should be a
                             # new input source
                             | 'ReadRawSnippetFileSizes' >> beam.Map(_add_snippet_raw_vcf_sizes)
                             )

    result = ({'whole_file_raw_size': files_to_raw_size, 'snippet_stats': files_to_snippet_sizes}
                         | 'ConsolidateFileStats' >> beam.CoGroupByKey()
                         | 'EstimateWholeFileEncodedSizes' >> beam.Map(_estimate_file_encoded_size)
                         | 'RemoveFileNamesFromData' >> beam.Map(lambda name_size: name_size[1])
                         | 'SumAllSizeEstimates' >> beam.CombineGlobally(FileSizeSumFn()))
    # result | 'Print' >> beam.Map(lambda x: logging.info("FINAL RECOMMENDATION: %dGB", math.ceil(x / 1e9)))
    result | 'PrintEstimate' >> beam.Map(lambda x: logging.info("Final estimate of encoded size: %s bytes", str(x)))
    # End resource estimator


    if known_args.report_all:
      inferred_headers, merged_headers = _get_inferred_headers(
          p, known_args, merged_headers)
      inferred_headers_side_input = beam.pvalue.AsSingleton(inferred_headers)

    _ = (merged_definitions
         | 'GenerateConflictsReport' >>
         beam.ParDo(conflicts_reporter.generate_conflicts_report,
                    known_args.report_path,
                    beam.pvalue.AsSingleton(merged_headers),
                    inferred_headers_side_input))
    if known_args.resolved_headers_path:
      vcf_to_bq_common.write_headers(merged_headers,
                                     known_args.resolved_headers_path)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
