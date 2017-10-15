package de.rdk.camel;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.spi.DataFormat;

import java.util.List;

/**
 * Simple route, that reads a csv input file, groups the super hero characters by gender and writes the
 * resulting buckets to separate output files.
 */
public class HeroCsvFilterRouteBuilder extends RouteBuilder {

    public static final String[] HEADER = {"PAGE_ID", "NAME", "URL_SLUG", "ID", "ALIGN", "EYE", "HAIR", "SEX", "GSM", "ALIVE", "APPEARANCES", "FIRST APPEARANCE", "YEAR" };
    private static final int CHARACTER_GENDER_FIELD_NO = 7;

    private final String inputDir;
    private final String outputDir;

    protected HeroCsvFilterRouteBuilder(String inputDir, String outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    public HeroCsvFilterRouteBuilder() {
        this.inputDir = "input/";
        this.outputDir = "result/";
    }

    @Override
    public void configure() throws Exception {

        from("file://" + inputDir)
                .routeId("heroGrouping-route")
                .unmarshal(csv())
                .split(body())
                .choice()
                .when(this::isMaleCharacter)
                .to("direct:handleMaleHeroes")
                .otherwise()
                .to("direct:handleFemaleHeroes");


        from("direct:handleMaleHeroes")
                .aggregate(header(Exchange.FILE_NAME_ONLY), new HeroAggregationStrategy()).completionTimeout(10 * 1000L)
                .marshal(csvWithHeader())
                .to("file://" + outputDir + "?fileName=MaleHeroes-${date:now:yyyyMMddHHmmssSSS}.csv");

        from("direct:handleFemaleHeroes")
                .aggregate(header(Exchange.FILE_NAME_ONLY), new HeroAggregationStrategy()).completionTimeout(10 * 1000L)
                .marshal(csvWithHeader())
                .to("file://" + outputDir + "?fileName=FemaleHeroes-${date:now:yyyyMMddHHmmssSSS}.csv");


    }

    private DataFormat csvWithHeader() {
        CsvDataFormat csv = new CsvDataFormat();
        return csv.setDelimiter(';')
                .setHeader(HEADER);
    }

    private DataFormat csv() {
        CsvDataFormat csv = new CsvDataFormat();
        return csv.setDelimiter(',')
                .setSkipHeaderRecord(true);
    }

    private boolean isMaleCharacter(Exchange exchange) {
        List<String> superhero = (List<String>) exchange.getIn().getBody();
        return superhero.get(CHARACTER_GENDER_FIELD_NO).contains("Male");
    }


}
