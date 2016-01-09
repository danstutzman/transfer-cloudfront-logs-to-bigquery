from apiclient.discovery import build
from apiclient.errors import HttpError
import argparse
from oauth2client.client import GoogleCredentials
import re
import subprocess
import sys
import uuid

def main():
  credentials = GoogleCredentials.get_application_default()
  bigquery_service = build('bigquery', 'v2', credentials=credentials)

  if True:
    try:
      table = bigquery_service.tables().insert(
        body={
          'tableReference': {
            'tableId': 'cloudfront_logs',
            'datasetId': 'cloudfront_logs',
            'projectId': 'speech-danstutzman'
          },
          'schema': {
            'fields': [
              {'name': 'datetime',                    'type': 'timestamp'},
              {'name': 'x_edge_location',             'type': 'string'},
              {'name': 'sc_bytes',                    'type': 'integer'},
              {'name': 'c_ip',                        'type': 'string'},
              {'name': 'cs_method',                   'type': 'string'},
              {'name': 'cs_host',                     'type': 'string'},
              {'name': 'cs_uri_stem',                 'type': 'string'},
              {'name': 'sc_status',                   'type': 'string'},
              {'name': 'cs_referer',                  'type': 'string'},
              {'name': 'cs_user_agent',               'type': 'string'},
              {'name': 'cs_uri_query',                'type': 'string'},
              {'name': 'cs_cookie',                   'type': 'string'},
              {'name': 'x_edge_result_type',          'type': 'string'},
              {'name': 'x_edge_request_id',           'type': 'string'},
              {'name': 'x_host_header',               'type': 'string'},
              {'name': 'cs_protocol',                 'type': 'string'},
              {'name': 'cs_bytes',                    'type': 'integer'},
              {'name': 'time_taken',                  'type': 'float'},
              {'name': 'x_forwarded_for',             'type': 'string'},
              {'name': 'ssl_protocol',                'type': 'string'},
              {'name': 'ssl_cipher',                  'type': 'string'},
              {'name': 'x_edge_response_result_type', 'type': 'string'},
            ],
          },
        },
        datasetId='cloudfront_logs',
        projectId='speech-danstutzman').execute()
    except HttpError as e:
      if e.resp and e.resp['status'] == '409': # if already exists
        pass
      else:
        raise

  command = ['/usr/local/bin/aws', 's3', 'ls', 's3://cloudfront-logs-danstutzman']
  sys.stderr.write(' '.join(command) + "\n")
  ls_output = subprocess.check_output(command)
  for line in ls_output.split("\n"):
    if line != '' and not ' PRE ' in line:
      date, time, size, filename_gz = re.split('\s+', line)
      if filename_gz.endswith('.gz'):
        filename = filename_gz[:-3]

        command = ['rm', '-f', filename_gz, filename]
        sys.stderr.write(' '.join(command) + "\n")
        subprocess.check_output(command)

        command = ['/usr/local/bin/aws', 's3', 'cp',
          's3://cloudfront-logs-danstutzman/%s' % filename_gz, '.']
        sys.stderr.write(' '.join(command) + "\n")
        subprocess.check_output(command)

        command = ['gunzip', filename_gz]
        sys.stderr.write(' '.join(command) + "\n")
        subprocess.check_output(command)

        with open(filename) as infile:
          for line2 in infile:
            if line2.startswith('#'):
              if line2 == '#Version: 1.0\n':
                pass
              elif line2 == '#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type\n':
                pass
              else:
                raise Exception("Don't understand line: %s" % line2)
            else:
              date, time, x_edge_location, sc_bytes, c_ip, cs_method, cs_host, cs_uri_stem, sc_status, cs_referer, cs_user_agent, cs_uri_query, cs_cookie, x_edge_result_type, x_edge_request_id, x_host_header, cs_protocol, cs_bytes, time_taken, x_forwarded_for, ssl_protocol, ssl_cipher, x_edge_response_result_type = line2.split("\t")
              insert_all_data = {
                'rows': [{
                  'json': {
                    'datetime': '%s %s' % (date, time),
                    'x_edge_location': x_edge_location,
                    'sc_bytes': sc_bytes,
                    'c_ip': c_ip,
                    'cs_method': cs_method,
                    'cs_host': cs_host,
                    'cs_uri_stem': cs_uri_stem,
                    'sc_status': sc_status,
                    'cs_referer': cs_referer,
                    'cs_user_agent': cs_user_agent,
                    'cs_uri_query': cs_uri_query,
                    'cs_cookie': cs_cookie,
                    'x_edge_result_type': x_edge_result_type,
                    'x_edge_request_id': x_edge_request_id,
                    'x_host_header': x_host_header,
                    'cs_protocol': cs_protocol,
                    'cs_bytes': cs_bytes,
                    'time_taken': time_taken,
                    'x_forwarded_for': x_forwarded_for,
                    'ssl_protocol': ssl_protocol,
                    'ssl_cipher': ssl_cipher,
                    'x_edge_response_result_type': x_edge_response_result_type,
                  },
                  # Generate a unique id for each row so retries don't accidentally
                  # duplicate insert
                  'insertId': str(uuid.uuid4()),
                }]
              }
              bigquery_service.tabledata().insertAll(
                projectId='speech-danstutzman',
                datasetId='cloudfront_logs',
                tableId='cloudfront_logs',
                body=insert_all_data).execute(num_retries=1)

        command = ['/usr/local/bin/aws', 's3', 'rm',
          's3://cloudfront-logs-danstutzman/%s' % filename_gz]
        sys.stderr.write(' '.join(command) + "\n")
        subprocess.check_output(command)

if __name__ == '__main__':
   #parser = argparse.ArgumentParser()
   #parser.add_argument('project_id', help='Your Google Cloud Project ID.')
   #args = parser.parse_args()
   main() #args.project_id)
