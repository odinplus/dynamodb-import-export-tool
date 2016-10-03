/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.beust.jcommander.Parameter;

/**
 * This class contains the parameters to input when executing the program from
 * command line.
 */
public class CommandLineArgsDynamoToSqlite {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "Endpoint of the source table", required = true)
    private String sourceEndpoint;

    public String getSourceEndpoint() {
        return sourceEndpoint;
    }

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public static final String DESTINATION_DATABASE = "--destinationDatabase";
    @Parameter(names = DESTINATION_DATABASE, description = "Destination Sqlite database", required = true)
    private String destinationDatabase;

    public String getDestinationDatabase() {
        return destinationDatabase;
    }

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public String getDestinationTable() {
        return destinationTable;
    }

    public static final String READ_THROUGHPUT_RATIO = "--readThroughputRatio";
    @Parameter(names = READ_THROUGHPUT_RATIO, description = "Percentage of total read throughput to scan the source table", required = true)
    private double readThroughputRatio;

    public double getReadThroughputRatio() {
        return readThroughputRatio;
    }

    public static final String TOTAL_SECTIONS = "--totalSections";
    @Parameter(names = TOTAL_SECTIONS, description = "Total number of sections to divide the scan into", required = false)
    private int totalSections = 1;

    public int getTotalSections() {
        return totalSections;
    }

    public static final String SECTION = "--section";
    @Parameter(names = SECTION, description = "Section number to scan when running multiple programs concurrently [0, 1... totalSections-1]", required = false)
    private int section = 0;

    public int getSection() {
        return section;
    }
    
    public static final String CONSISTENT_SCAN = "--consistentScan";
    @Parameter(names = CONSISTENT_SCAN, description = "Use this flag to use strongly consistent scan. If the flag is not used it will default to eventually consistent scan")
    private boolean consistentScan = false;

    public boolean getConsistentScan() {
        return consistentScan;
    }

    public static final String PROJECTION_EXPRESSION = "--projectionExpression";
    @Parameter(names = PROJECTION_EXPRESSION, description = "Projection expression to use with scan. If the flag is not used it will default to project all documents attributes")
    private String projectionExpression = null;

    public String getProjectionExpression() {
        return projectionExpression;
    }

    public static final String FILTER_EXPRESSION = "--filterExpression";
    @Parameter(names = FILTER_EXPRESSION, description = "Filter expression to use with scan. If the flag is not used it will default to empty filter")
    private String filterExpression = null;

    public String getFilterExpression() {
        return filterExpression;
    }

    public static final String EXPRESSION_ATTRIBUTE_VALUES = "--expressionAttributeValues";
    @Parameter(names = EXPRESSION_ATTRIBUTE_VALUES, description = "Filter expression to use with scan. If the flag is not used it will default to empty filter")
    private String expressionAttributeValues = null;

    public String getExpressionAttributeValues() {
        return expressionAttributeValues;
    }

    public static final String NUMBER_OF_SEGMENTS = "--numberOfSegments";
    @Parameter(names = NUMBER_OF_SEGMENTS, description = "Total number of segments", required = false)
    private int numberOfSegments = 1;

    public int getNumberOfSegments() {
        return numberOfSegments;
    }

}
