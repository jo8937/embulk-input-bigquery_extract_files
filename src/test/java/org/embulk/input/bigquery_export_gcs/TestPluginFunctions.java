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

public class TestPluginFunctions extends UnitTestInitializer
{
	private static final Logger log = LoggerFactory.getLogger(TestPluginFunctions.class);
		
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
    public void testParseGcsUrl(){
    	ConfigSource c = config.deepCopy();
    	c.set("gcs_uri", "gs://aaa/bbb/ccc_*");
    	
    	BigqueryExportGcsFileInputPlugin.PluginTask task = c.loadConfig(BigqueryExportGcsFileInputPlugin.PluginTask.class );
    	
    	BigqueryExportUtils.parseGcsUri(task);
    	
    	assertEquals("", "aaa", task.getGcsBucket());
    	assertEquals("", "bbb/ccc_", task.getGcsBlobNamePrefix());
    }


}
