package de.rdk.camel;

import org.apache.camel.main.Main;


public class CsvFilterApplication {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.addRouteBuilder(new HeroCsvFilterRouteBuilder());
        main.run(args);
    }




}
