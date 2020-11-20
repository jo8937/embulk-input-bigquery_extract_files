package org.embulk.input.bigquery_export_gcs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Optional;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.AssertTrue;

public class TestGoogleCloudAccessData  extends UnitTestInitializer
{
	private static final Logger log = LoggerFactory.getLogger(TestGoogleCloudAccessData.class);

    @Test
    public void testGcsInputStreamOpen() throws FileNotFoundException, IOException
    {
    	BigqueryExportGcsFileInputPlugin.PluginTask task = config.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
    	
        plugin.executeBigqueryApi(task);
        
    	InputStream ins = BigqueryExportUtils.openInputStream(task, task.getFiles().get(0));
    	
    	log.info("file size : {}",org.apache.commons.compress.utils.IOUtils.toByteArray(ins).length);
    }


    @Test(expected=Exception.class)
    public void testJobDoneButError() throws FileNotFoundException, IOException
    {
        BigqueryExportGcsFileInputPlugin.PluginTask task = config.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
        task.setThrowBigqueryJobWaitTimeout(true);
        task.setThrowBigqueryJobIncludesError(true);
        task.setQuery(Optional.of("select a from b"));
        plugin.executeBigqueryApi(task);

        InputStream ins = BigqueryExportUtils.openInputStream(task, task.getFiles().get(0));
        log.info("file size : {}",org.apache.commons.compress.utils.IOUtils.toByteArray(ins).length);

    }


    @Test(expected=Exception.class)
    public void testJobWaitTimeout() throws FileNotFoundException, IOException
    {
        BigqueryExportGcsFileInputPlugin.PluginTask task = config.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
        task.setThrowBigqueryJobWaitTimeout(true);
        task.setBigqueryJobWaitingSecond(Optional.of(1));
        plugin.executeBigqueryApi(task);

        InputStream ins = BigqueryExportUtils.openInputStream(task, task.getFiles().get(0));
        log.info("file size : {}",org.apache.commons.compress.utils.IOUtils.toByteArray(ins).length);

    }

}
