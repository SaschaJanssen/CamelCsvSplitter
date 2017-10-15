package de.rdk.camel;

import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * This test example uses mock endpoints for verification instead of real files.
 *
 * Next would be to use the ProducerTemplate instead of a real file.
 *
 * To Be Continued ....
 */
public class HeroCsvFilterRouteTest extends CamelTestSupport {

    private String inputDir = "out/input/";
    private String outputDir = "out/result/";

    @Before
    public void mockEndpoints() throws Exception {

        // rerouting the output to a mock endpoint

        AdviceWithRouteBuilder mockFile = new AdviceWithRouteBuilder() {

            @Override
            public void configure() throws Exception {
                // mock the for testing
                interceptSendToEndpoint("file://" + outputDir + "?fileName=MaleHeroes-${date:now:yyyyMMddHHmmssSSS}.csv")
                        .skipSendToOriginalEndpoint()
                        .to("mock:catchMaleHeroOutputMessages");
            }
        };

        context.getRouteDefinition("heroGrouping-route").adviceWith(context, mockFile);
    }

    @Override
    public boolean isUseAdviceWith() {
        //Tell CamelTestSupport to manually start/stop camel
        return true;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new HeroCsvFilterRouteBuilder(inputDir, outputDir);
    }

    @Test
    public void should_handle_input_file_and_group_characters_by_gender() throws Exception {
        MockEndpoint mockEndpoint = getMockEndpoint("mock:catchMaleHeroOutputMessages");

        context.start();
        mockEndpoint.expectedMessageCount(1);

        copyTestFileToInbox();

        mockEndpoint.await(20, TimeUnit.SECONDS);
        mockEndpoint.assertIsSatisfied();
        context.stop();

    }

    private void copyTestFileToInbox() throws IOException {
        InputStream resource = this.getClass().getClassLoader().getResourceAsStream("data/dc-wikia-data.csv");
        Files.copy(resource, Paths.get(inputDir + "dc-wikia-data.csv"));
    }


}
