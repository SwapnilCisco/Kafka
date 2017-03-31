package com.kafka.bootStrap.bootStrap.Server;

import CumsumerClasses.AmpConsumer;
//import CumsumerClasses.ConsumerOffset;
//import CumsumerClasses.Consumer_ccw_orders_sava;
//import CumsumerClasses.SimpleConsumer2;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Calling Consumer Main" );        
        //ConsumerOffset.main(args);
        AmpConsumer.main(args);
    }
}
