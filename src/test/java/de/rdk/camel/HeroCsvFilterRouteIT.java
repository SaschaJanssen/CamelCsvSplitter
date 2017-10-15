package de.rdk.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;


public class HeroCsvFilterRouteIT extends CamelTestSupport {

    private String inputDir = "out/input/";
    private String outputDir = "out/result/";

    @Before
    public void setUp() throws Exception {
        purgeInputDirectory();
        purgeOutputDirectory();
        super.setUp();
    }

    @Test
    public void should_handle_input_file_and_group_characters_by_gender() throws Exception {
        //MockEndpoint mockEndpoint = getMockEndpoint("mock:catchMaleHeroOutputMessages");

        context.start();
        //mockEndpoint.expectedMessageCount(1);

        copyTestFileToInbox();

        sleepUntilFilesAreProcessed();

        //mockEndpoint.assertIsSatisfied();
        context.stop();

        assertMaleCharacterOutput();
        assertFemaleCharacterOutput();
    }

    private void assertFemaleCharacterOutput() throws Exception {
        List<String> content = getOutputContentFor("FemaleHeroes");

        assertThat(content.size(), equalTo(4));
        assertThat(content.get(0), equalTo(csvHeader()));
        assertThat(content.get(1), containsString("Wonder Woman"));
        assertThat(content.get(2), containsString("Barbara Gordon"));
        assertThat(content.get(3), containsString("Catwoman"));
    }

    private void assertMaleCharacterOutput() throws Exception {
        List<String> content = getOutputContentFor("MaleHeroes");

        assertThat(content.size(), equalTo(4));
        assertThat(content.get(0), equalTo(csvHeader()));
        assertThat(content.get(1), containsString("Batman"));
        assertThat(content.get(2), containsString("Superman"));
        assertThat(content.get(3), containsString("Aquaman"));
    }

    private List<String> getOutputContentFor(String prefix) throws IOException {
        Path rootPath = Paths.get(outputDir);
        Optional<File> heroes = Files.walk(rootPath)
                .map(Path::toFile)
                .filter(file -> file.getName().startsWith(prefix))
                .findFirst();

        assertTrue(heroes.isPresent());
        return Files.readAllLines(heroes.get().toPath());
    }

    private String csvHeader() {
        return String.join(";", HeroCsvFilterRouteBuilder.HEADER);
    }

    private void sleepUntilFilesAreProcessed() {
        await().atMost(20, TimeUnit.SECONDS)
                .until(resultFilesWritten());
    }

    private Callable<Boolean> resultFilesWritten() {
        return () -> Files.list(Paths.get(outputDir)).count() == 2;
    }


    private void copyTestFileToInbox() throws IOException {
        InputStream resource = this.getClass().getClassLoader().getResourceAsStream("data/dc-wikia-data.csv");
        Files.copy(resource, Paths.get(inputDir + "dc-wikia-data.csv"));
    }


    private void purgeOutputDirectory() throws Exception {
        purgeDirectory(outputDir);
    }

    private void purgeInputDirectory() throws Exception {
        purgeDirectory(inputDir);
    }

    private void purgeDirectory(String paths) throws IOException {
        Path rootPath = Paths.get(paths);
        if (Files.notExists(rootPath)) {
            Files.createDirectories(rootPath);
            return;
        }

        Files.walk(rootPath)
                .map(Path::toFile)
                .forEach(File::delete);

    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new HeroCsvFilterRouteBuilder(inputDir, outputDir);
    }

    @Override
    public boolean isUseAdviceWith() {
        //Tell CamelTestSupport to manually start/stop camel
        return true;
    }


}