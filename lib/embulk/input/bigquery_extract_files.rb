Embulk::JavaPlugin.register_input(
  "bigquery_extract_files", "org.embulk.input.bigquery_export_gcs.BigqueryExportGcsFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
