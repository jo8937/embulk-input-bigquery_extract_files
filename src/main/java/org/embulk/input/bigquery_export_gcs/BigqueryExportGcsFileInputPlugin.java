package org.embulk.input.bigquery_export_gcs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import org.codehaus.plexus.util.StringUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.ConfigurableGuessInputPlugin;
import org.embulk.exec.GuessExecutor;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.slf4j.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.common.base.Optional;

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
public class BigqueryExportGcsFileInputPlugin
        implements FileInputPlugin, ConfigurableGuessInputPlugin
{
	private static final Logger log = Exec.getLogger(BigqueryExportGcsFileInputPlugin.class);
	
    public interface PluginTask
            extends Task
    {
        @Config("project")
        public String getProject();

        @Config("json_keyfile")
        public String getJsonKeyfile();
        
        @Config("dataset")
        @ConfigDefault("null")
        public Optional<String> getDataset();
        
        @Config("table")
        @ConfigDefault("null")
        public Optional<String> getTable();
        
        @Config("query")
        @ConfigDefault("null")
        public Optional<String> getQuery();
        
        @Config("file_format")
        @ConfigDefault("\"CSV\"")
        public Optional<String> getFileFormat();
        
        @Config("compression")
        @ConfigDefault("\"GZIP\"")
        public Optional<String> getCompression();

        @Config("gcs_uri")
        public String getGcsUri();

        @Config("temp_dataset")
        @ConfigDefault("null")
        public Optional<String> getTempDataset();
        public void setTempDataset(Optional<String> tempDataset);
        
        @Config("temp_table")
        @ConfigDefault("null")
        public Optional<String> getTempTable();
        public void setTempTable(Optional<String> tempTable);

        @Config("cache")
        @ConfigDefault("true")
        public boolean getQueryCache();

        @Config("use_legacy_sql")
        @ConfigDefault("false")
        public boolean getUseLegacySql(); 

        @Config("create_disposition")
        @ConfigDefault("\"CREATE_IF_NEEDED\"")
        public String getCreateDisposition();
        
        @Config("write_disposition")
        @ConfigDefault("\"WRITE_APPEND\"")
        public String getWriteDisposition();

        @Config("temp_local_path")
        public String getTempLocalPath();
        
        @Config("temp_schema_file_path")
        @ConfigDefault("null")
        public Optional<String> getTempSchemaFilePath();
        
        @Config("temp_schema_file_type")
        @ConfigDefault("null")
        public Optional<String> getTempSchemaFileType();
        
        @Config("bigquery_job_wait_second")
        @ConfigDefault("600")
        public Optional<Integer> getBigqueryJobWaitingSecond();
        
        @Config("cleanup_gcs_files")
        @ConfigDefault("false")
        public boolean getCleanupGcsTempFile(); 
        
        @Config("cleanup_temp_table")
        @ConfigDefault("true")
        public boolean getCleanupTempTable(); 
        
        public List<String> getFiles();
        public void setFiles(List<String> files);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
        
        public String getGcsBucket();
        public void setGcsBucket(String bucket);
        
        public String getGcsBlobNamePrefix();
        public void setGcsBlobNamePrefix(String blobName);

        public String getWorkDataset();
        public void setWorkDataset(String dataset);
        
        public String getWorkTable();
        public void setWorkTable(String table);

        public String getWorkId();
        public void setWorkId(String temp);
        
        //public Schema getSchemaConfig();
        //public void setSchameConfig(SchemaConfig schema);
    }
    
    @Override
	public ConfigDiff guess(ConfigSource execConfig, ConfigSource inputConfig) {

        GuessExecutor guessExecutor = Exec.getInjector().getInstance(GuessExecutor.class);
        return guessExecutor.guessParserConfig(null, inputConfig, execConfig);
	}

	@Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        checkLocalPath(task);
        
        executeBigqueryApi(task);
                
        int taskCount = task.getFiles().size();

        return resume(task.dump(), taskCount, control);
    }
	
	public void checkLocalPath(PluginTask task){
        File localPath = new File(task.getTempLocalPath());
        if(localPath.exists() == false){
        	log.error("local download path not exists : {}",localPath);
        	log.info("create local downlaod path : {}", localPath);
        	boolean ok = localPath.mkdirs();
        	if(!ok){
        		throw new RuntimeIOException(new IOException("local path create fail : " + localPath));
        	}
        }
	}
	
	public void executeBigqueryApi(PluginTask task) {

        BigqueryExportUtils.parseGcsUri(task);

        Schema schema = extractBigqueryToGcs(task);
        
        log.info("Schema : {}",schema.toString());
        
        writeSchemaFileIfSpecified(schema, task);
        
        List<String> files = listFilesOfGcs(task);
        
        task.setFiles(files);
        
	}
	
	public void writeSchemaFileIfSpecified(Schema schema, PluginTask task) {
		if(task.getTempSchemaFilePath().isPresent()) {
			log.info("generate temp {} schema file to ... {}", task.getTempSchemaFileType().or(""), task.getTempSchemaFilePath().orNull());
			BigqueryExportUtils.writeSchemaFile(schema, task.getTempSchemaFileType().orNull(), new File(task.getTempSchemaFilePath().get()));
        }
	}
	
    public Schema extractBigqueryToGcs(PluginTask task){
    	try {
    		Bigquery bigquery = BigqueryExportUtils.newBigqueryClient(task);
    		
    		// query init or execute query
    		BigqueryExportUtils.initWorkTableWithExecuteQuery(bigquery,task);
    		
    		// extract table and get schema
    		Schema schema = BigqueryExportUtils.extractWorkTable(bigquery, task);
    		
    		return schema;
		} catch (IOException e) {
			log.error("bigquery io error",e);
			throw new RuntimeIOException(e);
		} catch (InterruptedException e) {
			log.error("bigquery job error",e);
			throw new RuntimeException(e);
		}
    }
    // usually, you have an method to create list of files
    List<String> listFilesOfGcs(PluginTask task)
    {
    	log.info("get file list in to gcs of ... {}.{} -> gs://{}/{}", task.getDataset(), task.getWorkTable(),task.getGcsBucket(),task.getGcsBlobNamePrefix());
    	
    	try {
			return BigqueryExportUtils.getFileListFromGcs(task);
		} catch (IOException e) {
			log.error("GCS api call error");
			throw new RuntimeIOException(e);
		}
		
    }
    

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();
        //configDiff.has(attrName)
        
        
        // usually, yo uset last_path
        //if (task.getFiles().isEmpty()) {
        //    if (task.getLastPath().isPresent()) {
        //        configDiff.set("last_path", task.getLastPath().get());
        //    }
        //} else {
        //    List<String> files = new ArrayList<String>(task.getFiles());
        //    Collections.sort(files);
        //    configDiff.set("last_path", files.get(files.size() - 1));
        //}

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
    	final PluginTask task = taskSource.loadTask(PluginTask.class);
    	
    	// remove query temp table when exists 
    	if(task.getCleanupTempTable() && 
    			task.getTempTable().isPresent() && 
    			task.getQuery().isPresent() && 
    			task.getTempDataset().isPresent()){
    		BigqueryExportUtils.removeTempTable(task);
    	}
    	
    	for(int i=0; i < successTaskReports.size(); i++){
    		TaskReport report = successTaskReports.get(i);
    		if( report.isEmpty() ){
    			String file = task.getFiles().get(i);
    	    	
    			Path p = BigqueryExportUtils.getFullPath(task,file);
    			
    			log.info("delete temp file...{}",p);
    			p.toFile().delete();
    	    	
    			if(task.getCleanupGcsTempFile()){
    				//TODO : delete temp file in gcs
    				log.info("delete temp gcs file... {} ... not now... ", file);
    			}
    			
    			//		
    		}else{
    			log.error("datasource not empty : {}", report);
    		}
    	}
    	
    }
    

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        // Write your code here :)
        //throw new UnsupportedOperationException("BigquerycsvFileInputPlugin.open method is not implemented yet");

        // if you expect InputStream, you can use this code:
        
        InputStream input = BigqueryExportUtils.openInputStream(task, task.getFiles().get(taskIndex));
        
        return new InputStreamTransactionalFileInput(task.getBufferAllocator(), input) {
            @Override
            public void abort()
            { }
        
            @Override
            public TaskReport commit()
            {
                return Exec.newTaskReport();
            }
        };
    }
    
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
}
