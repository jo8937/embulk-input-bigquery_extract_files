package org.embulk.input.bigquery_export_gcs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGoogleCloudAccessData  extends UnitTestInitializer
{
	private static final Logger log = LoggerFactory.getLogger(TestGoogleCloudAccessData.class);

    @Test
    public void envTest(){
    	log.info("{}",System.getenv("GCP_PROJECT"));
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
