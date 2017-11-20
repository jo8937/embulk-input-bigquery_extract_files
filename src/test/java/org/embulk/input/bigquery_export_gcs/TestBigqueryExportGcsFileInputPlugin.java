package org.embulk.input.bigquery_export_gcs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBigqueryExportGcsFileInputPlugin
{
	private static final Logger log = LoggerFactory.getLogger(TestBigqueryExportGcsFileInputPlugin.class);
		
    private static String GCP_PROJECT;
    private static String GCP_JSON_FILE_PATH;
    private static String BQ_DATASET;
    private static String BQ_TABLE;
    private static String BQ_QUERY;
    private static String GCS_URI;
    private static String LOCAL_PATH;
    
    /*
     * This test case requires environment variables
     *   GCP_EMAIL
     *   GCP_P12_KEYFILE
     *   GCP_JSON_KEYFILE
     *   GCP_BUCKET
     */
    @BeforeClass
    public static void initializeConstantVariables()
    {
    	GCP_PROJECT = System.getenv("GCP_PROJECT");
    	GCP_JSON_FILE_PATH = System.getenv("GCP_JSON_FILE_PATH");
    	BQ_DATASET = System.getenv("BQ_DATASET");
    	BQ_TABLE = System.getenv("BQ_TABLE");
    	BQ_QUERY = System.getenv("BQ_QUERY");
    	GCS_URI = System.getenv("GCS_URI");
    	LOCAL_PATH = System.getenv("LOCAL_PATH");
    }
    
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;
    private BigqueryExportGcsFileInputPlugin plugin;
    private FileInputRunner runner;
    private MockPageOutput output;
    
    /**
  project: ddd
  json_keyfile: d
  dataset: xxx
  table: sxxx
  query: dddd
  gcs_bucket: ddd
  temp_dataset: xxxx
  local_temp_path: ddd

     * 
     */
    @Before
    public void createResources() 
    {
        config = Exec.newConfigSource()
                .set("project", GCP_PROJECT)
                .set("json_keyfile", GCP_JSON_FILE_PATH)
                .set("dataset", BQ_DATASET)
                .set("table", BQ_TABLE)
                .set("query", BQ_QUERY)
                .set("gcs_uri", GCS_URI)
        		.set("temp_dataset", BQ_DATASET)
        		.set("temp_local_path", LOCAL_PATH);
        
        plugin = new BigqueryExportGcsFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(BigqueryExportGcsFileInputPlugin.class));
        output = new MockPageOutput();
        
        assumeNotNull(GCP_PROJECT, GCP_JSON_FILE_PATH, BQ_DATASET, GCS_URI, LOCAL_PATH);
    }

    @Test
    public void regexTest(){
    	testTableName("select * from aaa.bbb","bbb");
    	testTableName("select * from aaa.bbb.ccc","ccc");
    	testTableName("select * from [aaa.bbb]","bbb");
    	testTableName("select * from aaa.bbb$20171123","bbb");
    	testTableName("select * from aaa.t_b_b_b","t_b_b_b");
    }
    
    public void testTableName(String query, String expect){
    	String word = BigqueryExportUtils.parseQueryToBaseTableName(query);
    	log.info("{}", word );
    	assertEquals(word, expect);
    }

    @Test
    public void envTest(){
    	log.info("{}",System.getenv("GCP_PROJECT"));
    }

    @Test
    public void testParseGcsUrl(){
    	ConfigSource c = config.deepCopy();
    	c.set("gcs_uri", "gs://aaa/bbb/ccc_*");
    	
    	BigqueryExportGcsFileInputPlugin.PluginTask task = c.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
    	
    	BigqueryExportUtils.parseGcsUri(task);
    	
    	assertEquals("", "aaa", task.getGcsBucket());
    	assertEquals("", "bbb/ccc_", task.getGcsBlobNamePrefix());
    }

    @Test
    public void testGcsInputStreamOpen() throws FileNotFoundException, IOException
    {
    	BigqueryExportGcsFileInputPlugin.PluginTask task = config.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
    	
        plugin.processTransactionAndSetTask(task);
        
    	InputStream ins = BigqueryExportUtils.openInputStream(task, task.getFiles().get(0));
    	
    	log.info("file size : {}",org.apache.commons.compress.utils.IOUtils.toByteArray(ins).length);
    }
}
