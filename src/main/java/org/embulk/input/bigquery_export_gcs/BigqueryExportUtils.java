package org.embulk.input.bigquery_export_gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.embulk.input.bigquery_export_gcs.BigqueryExportGcsFileInputPlugin.PluginTask;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.Bigquery.Tabledata;
import com.google.api.services.bigquery.Bigquery.Tables.Delete;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.airlift.slice.RuntimeIOException;

/**
 * 
 * 
 * 
 * #reference : 
 * 
 * # https://github.com/embulk/embulk
 * # https://github.com/embulk/embulk-input-s3
 * # https://github.com/embulk/embulk-input-gcs
 * # https://github.com/embulk/embulk-input-jdbc
 * # https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/storage/json-api/src/main/java/StorageSample.java
 * 
 * 
 * @author george 2017. 11. 16.
 *
 */
public class BigqueryExportUtils 
{
	private static final Logger log = Exec.getLogger(BigqueryExportUtils.class);


    public static String parseQueryToBaseTableName(String query){
    	if( query == null){
    		return null;
    	}
    	
    	Pattern p = Pattern.compile(" from [\\[]?([^ \\$\\[\\]]+)[\\]]?", Pattern.CASE_INSENSITIVE);
    	Matcher m = p.matcher(query);
    	if(m.find() && m.groupCount() > 0){
    		return Strings.nullToEmpty(m.group(1)).replaceAll(".*\\.","").replaceAll("[^\\w\\s]","");
    	}else{
    		return null;
    	}
    }
    
    public static String generateTempTableName(String query){
    	return generateTempTableName(query, null);
    }
    
    public static String generateTempTableName(String query, String tablename){
    	
    	String tname  = tablename;
    	
    	if (tname == null){
    		tname = parseQueryToBaseTableName(query);
    		if(tname == null){
    			tname = "temp";	
    		}
    	}
    	
    	return "embulk_" + tname + "_" + FastDateFormat.getInstance("yyyyMMdd_HHmmss").format(new Date()) + "_" + UUID.randomUUID().toString().replaceAll("-", "");
    }
    

    public static void executeQueryToDestinationWorkTable(Bigquery bigquery, PluginTask task) throws IOException, InterruptedException {

    	log.info("execute Query to Table ");
    	log.info("# Query # {}",task.getQuery().get());
		log.info("# Table # {}.{} ",task.getWorkDataset(), task.getWorkTable());
		
	    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
	    queryConfig.setQuery(task.getQuery().get());
	    queryConfig.setDestinationTable(new TableReference()
	                                    .setProjectId(task.getProject())
	                                    .setDatasetId(task.getWorkDataset())
	                                    .setTableId(task.getWorkTable()));
	    queryConfig.setUseLegacySql(task.getUseLegacySql());
	    queryConfig.setCreateDisposition(task.getCreateDisposition());
	    queryConfig.setWriteDisposition(task.getWriteDisposition());
	    queryConfig.setUseQueryCache(task.getQueryCache());
	    queryConfig.setAllowLargeResults(true);
	    
	    com.google.api.services.bigquery.Bigquery.Jobs.Insert insert = bigquery.jobs().insert(task.getProject(), 
	                                                                                          new Job().setConfiguration(new JobConfiguration().setQuery(queryConfig))
	                                                                                          );
	    Job jobRes = insert.execute(); // ~~~~~~~~~~~~~~~~~~~~~ API CALL
	    
	    JobReference jobRef = jobRes.getJobReference();
		String jobId = jobRef.getJobId();

		log.info("query to Table jobId : {} : waiting for job end...",jobId);
		
		Job lastJob = waitForJob(bigquery, task.getProject(), jobId, task.getBigqueryJobWaitingSecond().get());
		
		log.debug("waiting for job end....... {}", lastJob.toPrettyString());
	}
    
    public static void parseGcsUri(PluginTask task){

        if(StringUtils.isEmpty(task.getGcsUri()) || false == task.getGcsUri().matches("gs://[^/]+/.+") ){
        	throw new RuntimeException("gcs_uri not found : " + task.getGcsUri());
        }
        
        task.setGcsBucket(task.getGcsUri().replaceAll("gs://([^/]+)/.+", "$1"));	
        task.setGcsBlobNamePrefix(task.getGcsUri().replaceAll("gs://[^/]+/(.+)", "$1").replaceAll("[\\*]*$", ""));
        
    }
    
    
    /***
     * 
     * google cloud sdk 
     * 
     * @param task
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    public static Bigquery newBigqueryClient(PluginTask task){
    	log.debug("# Starting Google BigQuery API ... ");
		try {
			GoogleCredentialSet set = googleCredential(task);
			return new Bigquery.Builder(set.transport, set.jsonFactory, set.googleCredential).setApplicationName("embulk-input-bigquey-export-gcs").build();
		} catch (Exception e) {
			throw new RuntimeException("bigquery connect fail",e);
		}
		
    }

    public static Storage newGcsClient(PluginTask task) throws FileNotFoundException, IOException{
    	log.debug("# Starting Google Cloud Storage ... ");
    	GoogleCredentialSet set = googleCredential(task);
		return new Storage.Builder(set.transport, set.jsonFactory, set.googleCredential).setApplicationName("embulk-input-bigquey-export-gcs").build();
    }

    
    public static class GoogleCredentialSet {	
    	public GoogleCredential googleCredential = null;
    	public HttpTransport transport = new NetHttpTransport();
    	public JsonFactory jsonFactory = new JacksonFactory();
    }
    
	public static GoogleCredentialSet googleCredential(PluginTask task) throws IOException {
		GoogleCredentialSet ret = new GoogleCredentialSet(); 
		
		log.debug("### init googleCredentialFile : {} ",task.getJsonKeyfile());
		
		ret.transport = new NetHttpTransport();
		ret.jsonFactory = new JacksonFactory();
		
		GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream( task.getJsonKeyfile() ), ret.transport, ret.jsonFactory);
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(BigqueryScopes.all()).createScoped(StorageScopes.all());
		}
		ret.googleCredential = credential;
		return ret;
	}
	
    
    public static List<String> getFileListFromGcs(PluginTask task) throws FileNotFoundException, IOException{
    	Storage gcs = newGcsClient(task);
    	return getFileListFromGcs(gcs, task.getGcsBucket(), task.getGcsBlobNamePrefix());
    }
    
    public static List<String> getFileListFromGcs(Storage gcs, String bucket, String blobName) throws IOException{
    	ImmutableList.Builder<String> builder = ImmutableList.builder();
    	Storage.Objects.List listRequest  = gcs.objects().list(bucket).setPrefix(blobName);
    	Objects objects;
    	
        do {
          objects = listRequest.execute();
          if(objects.getItems() == null){
        	  log.error("file not found in gs://{}/{}",bucket,blobName);
        	  return builder.build();
          }
          for(StorageObject obj : objects.getItems()){
        	  builder.add(obj.getName());  
          }
          listRequest.setPageToken(objects.getNextPageToken());
        } while (null != objects.getNextPageToken());

		return builder.build().asList();
    }

	public static final String TYPE_INTEGER = "INTEGER";
	public static final String TYPE_STRING = "STRING";
	public static final String TYPE_FLOAT = "FLOAT";
	public static final String TYPE_TIMESTAMP = "TIMESTAMP";
	
	public static SchemaConfig getSchemaWithGuess(Bigquery bigquery, PluginTask task, Table table, Schema schema) throws IOException{
		List<ColumnConfig> columns = Lists.newArrayList();
		
		com.google.api.services.bigquery.Bigquery.Tabledata.List req = bigquery.tabledata().list(task.getProject(), task.getDataset().get(), table.getTableReference().getTableId());
		
		req = req.setMaxResults(new Long(1));
				
		TableDataList list = req.execute();
		
		for(TableRow row : list.getRows()){
			//row.get(name)
		}
		return new SchemaConfig(columns);
	}
	
    public static Schema convertTableSchemaToEmbulkSchema(Table table){
    	Schema.Builder builder = Schema.builder();
    	TableSchema ts = table.getSchema();
    	for( TableFieldSchema field : ts.getFields() ){
    		String name = field.getName();
    		org.embulk.spi.type.Type type = Types.JSON;
    		switch(field.getType()){
    			case "INTEGER":
    				builder.add(name, Types.LONG);
    				break;
    			case "FLOAT": 
    				builder.add(name, Types.DOUBLE);
    				break;
    			case "TIMESTAMP": 
    				builder.add(name, Types.TIMESTAMP);
    				break;
    			default: 
    				builder.add(name, Types.STRING);
    				break;
    		}
    	}
    	return builder.build();
    }
    
    public static PHASE initTask(PluginTask task) {

		if(task.getQuery().isPresent()){
			task.setWorkId(generateTempTableName(task.getQuery().get()));
			
			if(task.getTempTable().isPresent() == false){
				task.setTempTable(Optional.of(task.getWorkId()));
			}
			if(task.getTempDataset().isPresent() == false && task.getDataset().isPresent()){
				task.setTempDataset(Optional.of(task.getDataset().get()));
			}
				
			// actual target table setting
			task.setWorkDataset(task.getTempDataset().get());
			task.setWorkTable(task.getTempTable().get());
			
			return PHASE.QUERY;
		}else if(task.getTable().isPresent() && task.getDataset().isPresent()){
			task.setWorkId(generateTempTableName(null, task.getTable().get()));
			// actual target table setting			
			task.setWorkDataset(task.getDataset().get());
			task.setWorkTable(task.getTable().get());
			
			return PHASE.TABLE;
			
		}else{
			throw new RuntimeException("please set config file [dataset]+[table] or [query]");
		}
    }
    
    public static Schema extractWorkTable(Bigquery bigquery, PluginTask task) throws FileNotFoundException, IOException, InterruptedException{
    	
		Table table = bigquery.tables().get(task.getProject(), task.getWorkDataset(), task.getWorkTable()).execute();
		
		Schema embulkSchema = convertTableSchemaToEmbulkSchema(table);
		
		
		//task.setSchame(embulkSchema);
		
		log.debug("Table Schema : {}", table.getSchema());
		
		//Tabledata. req = bigquery.tabledata().list(projectId, dataset, table);
		
		log.info("start table extract [{}.{}] to {} ...", task.getWorkDataset(), task.getWorkTable(), task.getGcsUri());
		
		Job jobReq = new Job();
	    JobConfigurationExtract extract = new JobConfigurationExtract();
	    extract.setDestinationFormat(task.getFileFormat().get());
	    extract.setCompression(task.getCompression().get());
	    extract.setDestinationUris(Lists.newArrayList(task.getGcsUri()));
	    extract.setSourceTable(table.getTableReference());
	    jobReq.setConfiguration(new JobConfiguration().setExtract(extract));

	    Insert jobInsert = bigquery.jobs().insert(task.getProject(), jobReq);
	    Job res = jobInsert.execute();
	    
	    JobReference jobRef = res.getJobReference();
		String jobId = jobRef.getJobId();
		log.info("extract jobId : {}",jobId);
		log.debug("waiting for job end....... ");
		
		Job lastJob = waitForJob(bigquery, task.getProject(), jobId, task.getBigqueryJobWaitingSecond().get());
		
		log.info("table extract result : {}",lastJob.toPrettyString());
		
		return embulkSchema;
    }

    public static Job waitForJob(Bigquery bigquery, String project, String jobId, int bigqueryJobWaitingSecond) throws IOException, InterruptedException{
    	int maxAttempts = bigqueryJobWaitingSecond;
		int initialRetryDelay = 1000; // ms
		Job pollingJob = null;	
		log.info("waiting for job end : {}",jobId);
		int tryCnt = 0;
        for (tryCnt=0; tryCnt < maxAttempts; tryCnt++){
            pollingJob = bigquery.jobs().get(project, jobId).execute();
            String state = pollingJob.getStatus().getState();
            log.debug("Job Status {} : {}",jobId, state);
            
            if (pollingJob.getStatus().getState().equals("DONE")) {
                break;
            }
            log.info("waiting {} ... ",tryCnt);
            Thread.sleep(initialRetryDelay);
        }
        if(tryCnt + 1 == maxAttempts){
        	log.error("Bigquery Job Waiting exceed : over {} second...", bigqueryJobWaitingSecond);
        }
        
        return pollingJob;
    }
    
    public static Schema predictSchema(Bigquery bigquery){
    	Schema schema = Schema.builder().add("", org.embulk.spi.type.Types.LONG).build();
    	return schema;
    }

    /**
     * 
     * https://github.com/google/google-api-java-client-samples/blob/master/storage-cmdline-sample/src/main/java/com/google/api/services/samples/storage/examples/ObjectsDownloadExample.java
     * 
     */
    public static InputStream openInputStream(PluginTask task, String file)
    {
        try {
        	
        	
        	Storage gcs = newGcsClient(task); 
			
        	
    		Path fullLocalFilePath = getFullPath(task, file);
    		
        	log.info("Start download : gs://{}/{} ...to ... {} ",task.getGcsBucket(), file, task.getTempLocalPath());
        	
    	    Storage.Objects.Get getObject = gcs.objects().get(task.getGcsBucket(), file);
    	    getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true);
    	     
    	    // return getObject.executeMediaAsInputStream() // direct InputStream ?? I Think this is faster then temp file. but ...
    	    
    		try(FileOutputStream s = new FileOutputStream(fullLocalFilePath.toFile())){
    			getObject.executeMediaAndDownloadTo(s);	
    		}
    		return new FileInputStream(fullLocalFilePath.toFile());
        	
		} catch (FileNotFoundException e) {
			log.error("gcs file not found error",e);
			return null;
		} catch(IOException e){
			log.error("gcs file read error",e);
			return null;
		}
    }
    

    public static Path getFullPath(PluginTask task, String file){
    	String baseName = file.replaceFirst(".*/", "");
    	Path fullLocalFilePath = FileSystems.getDefault().getPath(task.getTempLocalPath(), baseName);
    	return fullLocalFilePath ;
    }
    
    public enum SCHEMA_TYPE{
    		EMBULK,
    		AVRO
    }
    
    public static Schema decnodeSchemaJson(String json) {
	    	ObjectMapper mapper = new ObjectMapper();
		try {
			Schema schema = mapper.readValue(json, Schema.class);
			return schema;
		} catch (Exception e) {
			log.error("error when parse schema object : " + json,e);
			return null;
		}
    }
    
	public static void writeSchemaFile(Schema schema, String schemaType, File file) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.writeValue(file, schema);
		} catch (Exception e) {
			log.error("error when create schema json {}",file);
			throw new RuntimeException(e);
		}
	}
	
    public static String generateSchemaJson(Schema schema, String schemaType) {
    		SCHEMA_TYPE tp = SCHEMA_TYPE.EMBULK;
    		if(schemaType != null) {
    			tp.valueOf(schemaType);
    		}
    		
    		ObjectMapper mapper = new ObjectMapper();
    		try {
			String jsonString = mapper.writeValueAsString(schema);
			return jsonString;
		} catch (JsonProcessingException e) {
			log.error("error when create schema json",e);
			return null;
		}
    		//for(Column col : schema.getColumns()) {
    }
    
    public static String toPrettyString(Object obj){
    	try {
    		ObjectMapper mapper = new ObjectMapper();
			String str = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
			return str;
		} catch (Exception e) {
			log.error("JSON format error",e);
			return java.util.Objects.toString(obj);
		}
    }
    
    /**
     * 
     * @param task
     */
    public static void removeTempTable(PluginTask task){
		try {
			log.info("Remove temp table {}.{}",task.getTempDataset().get(), task.getTempTable().get());
			Bigquery bigquery = newBigqueryClient(task);
			Delete del = bigquery.tables().delete(task.getProject(), task.getTempDataset().get(), task.getTempTable().get());
			del.execute();
		} catch (Exception e) {
			log.error("# Remove temp table FAIL : " + task.getTempDataset().orNull() +  "." + task.getTempTable().orNull(),e);
		}
    }
    
    public static void removeGcsFilesBeforeExecuting(PluginTask task){
		try {
			log.info("start cleanup gs://{}/{} ... ",task.getGcsBucket(), task.getGcsBlobNamePrefix());
			Storage gcs = BigqueryExportUtils.newGcsClient(task);
			List<String> fileList = getFileListFromGcs(gcs, task.getGcsBucket(), task.getGcsBlobNamePrefix());
			for(String f : fileList){
				log.info("cleanup gs://{}/{} ... ",task.getGcsBucket(), f);
				gcs.objects().delete(task.getGcsBucket(), f).execute();	
			}
		} catch (GoogleJsonResponseException e) {
			if(e.getStatusCode() == 404){
				log.info("file not found in gs://{}/{} :: it's ok ",task.getGcsBucket(), task.getGcsBlobNamePrefix());
			}else{
				throw new RuntimeException("# Remove GCS files gs://" + task.getGcsBucket() + "/" + task.getGcsBlobNamePrefix(),e);	
			}
		} catch (Exception e) {
			throw new RuntimeException("# Remove GCS files gs://" + task.getGcsBucket() + "/" + task.getGcsBlobNamePrefix(),e);
		}
    }
    
    public static void removeTempGcsFiles(PluginTask task, String file){
		try {
			Storage gcs = BigqueryExportUtils.newGcsClient(task);
			log.info("delete finish file gs://{}/{}", task.getGcsBucket(), file);
			gcs.objects().delete(task.getGcsBucket(), file).execute();
		} catch (Exception e) {
			log.error("# Remove temp gcs file FAIL : " + file,e);
		}
    }
}
