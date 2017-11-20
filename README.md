# Bigquerycsv file input plugin for Embulk

development version. 0.0.1

## Overview

* **Plugin type**: file input
* **Resume supported**: no
* **Cleanup supported**: yes

### Detail

Google Cloud Bigquery's table or query result export.

Maybe solution for very big data in bigquery.

If you set  **table** config without **query** config, 
then just extract table to Google Cloud Storage.

If you set **query** config,
then query result save to temp table and then extracted that temp table to Google Cloud Storage uri.
see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.extract
   
## Usage

### Install plugin

```bash
embulk gem install embulk-input-bigquery_extract_files
```

* rubygem url : https://rubygems.org/profiles/jo8937


## Configuration

- **project**: Google Cloud Platform (gcp) project id (string, required)
- **json_keyfile**: gcp service account's private key with json (string, required)
- **gcs_uri**: bigquery result saved uri. bucket and path names parsed from this uri.  (string, required)
- **temp_local_path**: extract files download directory in local machine (string, required)

- **dataset**: target datasource dataset (string, default: `null`)
- **table**: target datasource table. either query or table are required. (string, default: `null`)
- **query**: target datasource query. either query or table are required. (string, default: `null`)

- **temp_dataset**: if you use **query** param, query result saved here  (string, default: `null`)
- **temp_table**: if you use **query** param, query result saved here. if not set, plugin generate temp name (string, default: `null`)
- **use_legacy_sql**: if you use **query** param, see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.useLegacySql (string, default: `false`)
- **cache**: if you use **query** param, see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.useQueryCache (string, default: `true`)
- **create_disposition**: if you use **query** param, see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.createDisposition (string, default: `CREATE_IF_NEEDED`)
- **write_disposition**: if you use **query** param, see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.writeDisposition (string, default: `WRITE_APPEND`)

- **file_format**: Table extract file format. see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.extract.destinationFormat (string, default: `CSV`)
- **compression**: Table extract file compression setting. see : https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.extract.compression (string, default: `GZIP`)

- **decoders**: embulk java-file-input plugin's default attribute. see : http://www.embulk.org/docs/built-in.html#gzip-decoder-plugin
- **parser**: embulk java-file-input plugin's default .attribute see : http://www.embulk.org/docs/built-in.html#csv-parser-plugin

## Example

```yaml
in:
  type: bigquery_extract_files
  project: googlecloudplatformproject
  json_keyfile: gcp-service-account-private-key.json
  dataset: target_dataset
  #table: target_table
  query: 'select a,b,c from target_table'
  gcs_uri: gs://bucket/subdir
  temp_dataset: temp_dataset
  temp_local_path: C:\Temp
  file_format: 'NEWLINE_DELIMITED_JSON'
  compression: 'GZIP'
  decoders:
  - {type: gzip}  
  parser:
    type: json
out: 
  type: stdout
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
