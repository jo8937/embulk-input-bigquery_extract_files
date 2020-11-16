# Google Cloud Bigquery extract file input plugin for Embulk 

embulk file input plugin. 

- embulk : http://www.embulk.org/docs/

- embulk plugins : http://www.embulk.org/plugins/

Read files stored in Google Cloud Storage that extracted from Google Cloud Bigquery's table or query result.

## Overview

* **Plugin type**: file input
* **Resume supported**: no
* **Cleanup supported**: yes

### Detail

Read files stored in Google Cloud Storage, that exported from Google Cloud Bigquery's table or query result.

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

### Update plugin (latest version : 0.0.13)

```bash
embulk gem update embulk-input-bigquery_extract_files
```

* rubygem url : https://rubygems.org/profiles/jo8937


## Configuration

- **project**: Google Cloud Platform (gcp) project id (string, required)
- **json_keyfile**: gcp service account's private key with json (string, required)
- **location**: location of bigquery dataset and temp_dataset. see : https://cloud.google.com/bigquery/docs/locations (Optional) (string, default: `US`)
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

- **temp_schema_file_path**: bigquery result schema file for parser. (Optional) (string, default: `null`)

- **bigquery_job_wait_second**: bigquery job waiting second. (Optional) (string, default: `600`)

- **throw_bigquery_job_wait_timeout**: throw exception when bigquery job waiting second timeout. (Optional) (string, default: `false`)

- **cleanup_gcs_before_executing**: delete all file in gcs temp path before process start (Optional) (string, default: `true`)

- **cleanup_gcs_files**: delete all file in gcs temp path after process end (Optional) (string, default: `false`)

- **cleanup_temp_table**: delete query result table after process end (Optional) (string, default: `true`)

- **cleanup_local_temp_files**: delete all file in local temp dir (Optional) (string, default: `true`)

- **direct_download_enabled**: gcs download option. see : https://developers.google.com/api-client-library/java/google-api-java-client/reference/1.19.1/com/google/api/client/googleapis/media/MediaHttpDownloader#setDirectDownloadEnabled(boolean)  (Optional) (string, default: `false`)

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

### Advenced Example 

#### bigquery to mysql with auto-schema 

I have to batch bigquery table to mysql every day for my job.
then, I wan'to get auto-schema for this file input plugin.

- see also 
 - https://github.com/jo8937/embulk-parser-csv_with_schema_file
 - https://github.com/embulk/embulk-output-jdbc/tree/master/embulk-output-mysql

this is my best practive for bigquery to mysql batch config. 

```yaml
in:
  type: bigquery_extract_files
  project: my-google-project
  json_keyfile: /tmp/embulk/google_service_account.json
  query: 'select * from dataset.t_nitocris'
  temp_dataset: temp_dataset
  gcs_uri: gs://bucket/embulktemp/t_nitocris_*
  temp_local_path: /tmp/embulk/data
  file_format: 'CSV'
  compression: 'GZIP'
  temp_schema_file_path: /tmp/embulk/schema/csv_schema_nitocris.json
  decoders:
  - {type: gzip}
  parser:
    type: csv_with_schema_file
    default_timestamp_format: '%Y-%m-%d %H:%M:%S %z'
    schema_path: /tmp/embulk/schema/csv_schema_nitocris.json
out:
  type: mysql
  host: host
  user: user
  password: password
  port: 3306
  database: MY_DATABASE
  table: 
  options: {connectTimeout: 0, waitTimeout: 0, enableQueryTimeouts: false, autoReconnect: true}
  mode: insert_direct
  retry_limit: 60
  retry_wait: 3000
  batch_size: 4096000
```

* bigquery's TIMESTAMP value format is not exactly one format in one CSV export. you can define optional 'columns' param in 'csv_with_schema_file' parser for another format like below.

```yml
...
  parser:
    type: csv_with_schema_file
    default_timestamp_format: '%Y-%m-%d %H:%M:%S %z'
    schema_path: /tmp/embulk/schema/csv_schema_nitocris.json
    columns:
      - {name: Date2, type: timestamp, format: '%Y-%m-%d %H:%M:%S.%N %z'}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```


## Plugin maintenance 

for old version user

### Remove plugin specific version 

```bash
embulk gem uninstall embulk-input-bigquery_extract_files --version 0.0.13
```

* rubygem url : https://rubygems.org/profiles/jo8937


# Another choice

This plugin useful for file-input type. but maybe so complicated to use.

For portable use, see embulk-input-bigquery : https://github.com/medjed/embulk-input-bigquery
