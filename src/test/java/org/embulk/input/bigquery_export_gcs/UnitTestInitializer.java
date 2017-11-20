package org.embulk.input.bigquery_export_gcs;

import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.io.IOException;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSourceImpl;
import org.embulk.input.bigquery_export_gcs.BigqueryExportGcsFileInputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnitTestInitializer
{
	private static final Logger log = LoggerFactory.getLogger(UnitTestInitializer.class);
		
    private static String TEST_EMBULK_CUSTOM_CONFIG_PATH;
    
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
    	TEST_EMBULK_CUSTOM_CONFIG_PATH = "local_config.yml"; //System.getenv("TEST_EMBULK_CUSTOM_CONFIG_PATH");
    }
    
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    protected ConfigSource config;
    protected BigqueryExportGcsFileInputPlugin plugin;
    protected FileInputRunner runner;
    protected MockPageOutput output;
    
    /**
     * @throws IOException 
     * 
     */
    @Before
    public void createResources() throws IOException 
    {
    	File f =new File(TEST_EMBULK_CUSTOM_CONFIG_PATH);
    	if(f.exists()){
    		log.info("Test env load from file : {}",f);
    		config = new ConfigLoader(Exec.session().getModelManager()).fromYamlFile(f).getNested("in");	
    		log.info("values : {}",config.toString());
    	}else{
    		log.error("test config file NOT FOUND ! :: {}",f);
    		log.info("load test env from system env ... ");
    		config = Exec.newConfigSource();
    		config.set("project", System.getenv("embulk_config_project"));
    		config.set("json_keyfile", System.getenv("embulk_config_json_keyfile"));
    		config.set("dataset", System.getenv("embulk_config_dataset"));
    		config.set("query", System.getenv("embulk_config_query"));
    		config.set("gcs_uri", System.getenv("embulk_config_gcs_uri"));
    		config.set("temp_dataset", System.getenv("embulk_config_temp_dataset"));
    		config.set("temp_local_path", System.getenv("embulk_config_temp_local_path"));
    	}
    	        
        plugin = new BigqueryExportGcsFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(BigqueryExportGcsFileInputPlugin.class));
        output = new MockPageOutput();

        assumeNotNull(
                      config.get(String.class, "project"),
                      config.get(String.class, "json_keyfile"),
                      config.get(String.class, "gcs_uri"),
                      config.get(String.class, "temp_local_path")
                      );
    }


}
