package de.rdk.camel;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation strategy that collects the different superheroes.
 */
public class HeroAggregationStrategy implements AggregationStrategy {

    @SuppressWarnings("unchecked")
    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        // ugly list<list<string>> data structure is needed by CSV marshaller to create proper csv files
        List<List<String>> heroBuffer;

        if (oldExchange == null) {
            heroBuffer = new ArrayList<>();
        } else {
            heroBuffer = (List<List<String>>) oldExchange.getIn().getBody();
        }

        heroBuffer.add(newExchange.getIn().getBody(List.class));
        newExchange.getIn().setBody(heroBuffer);

        return newExchange;
    }
}
