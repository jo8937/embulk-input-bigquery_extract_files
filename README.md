# Bigquerycsv file input plugin for Embulk

development version. 0.0.1

## Overview

* **Plugin type**: file input
* **Resume supported**: no
* **Cleanup supported**: yes

### Detail

Google Cloud Bigquery's table or query result export.

If you set  **table** config without **query** config, 
then just extract table to Google Cloud Storage.

If you set **query** config,
then query result save to temp table and then extracted that temp table to Google Cloud Storage uri.
   
## Usage

### Install plugin

```bash
embulk gem install embulk-input-bigquery_extract_files
```

* rubygem url : https://rubygems.org/profiles/jo8937


## Configuration

- **project**: Google Cloud Platform (gcp) project id (string, required)
- **json_keyfile**: gcp service account's private key with json (string, required)
- **dataset**: target datasource dataset (string, default: `null`)
- **table**: target datasource table. either query or table are required  (string, default: `null`)
- **query**: target datasource query. either query or table are required  (string, default: `null`)
- **gcs_uri**: bigquery result saved uri. bucket and path names parsed from this uri.  (string, required)
- **temp_dataset**: if you use **query** param, then query result saved here  (string, required)
- **temp_table**: if you use **query** param, then query result saved here. if not set, plugin generate temp name (string, default: `null`)
- **file_format**: Table extract file format. see :  (string, default: `CSV`)
- **compression**: Table extract file compression setting. see :  (string, default: `CSV`)
- **temp_local_path**: extract files download directory in local machine (string, required)
- **decoders**: embulk file-input-plugin's default see : 
- **parser**: embulk file-input-plugin's default see :

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
