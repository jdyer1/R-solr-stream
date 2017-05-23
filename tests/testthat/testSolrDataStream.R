test_that("we can stream data from a mock solr to R", {	
	mock_solr_client <- .jnew("rsolrstream/testthat/MockSolrClient")
	solr_client <- .jcast(mock_solr_client, "org/apache/solr/client/solrj/SolrClient")
	mock_solr_collection <- "collection_that_runs_Stuff"
	mock_streaming_expression <- "expression_name(collection_with_data, expession_details_go_here)"
	data <- solr_stream(solr_client, mock_solr_collection, mock_streaming_expression)
	
	for(i in seq(1:2)) {
		expect_equal(default_string_transform(mock_solr_client$getString(as.integer(i))), data[i, grep("^string$", colnames(data))]);
		expect_equal(default_double_transform(mock_solr_client$getDouble(as.integer(i))), data[i, grep("^double$", colnames(data))]);
		expect_equal(default_long_transform(mock_solr_client$getLong(as.integer(i))), data[i, grep("^long$", colnames(data))]);
		expect_equal(default_boolean_transform(mock_solr_client$getBoolean(as.integer(i))), data[i, grep("^boolean$", colnames(data))]);	
		expect_equal(default_string_transform(mock_solr_client$getStringMulti(as.integer(i))), default_string_transform(data[i, grep("^string_multi$", colnames(data))]))
		expect_equal(default_double_transform(mock_solr_client$getDoubleMulti(as.integer(i))), default_double_transform(data[i, grep("^double_multi$", colnames(data))]))
		expect_equal(default_long_transform(mock_solr_client$getLongMulti(as.integer(i))), default_long_transform(data[i, grep("^long_multi$", colnames(data))]))
		expect_equal(default_boolean_transform(mock_solr_client$getBooleanMulti(as.integer(i))), default_boolean_transform(data[i, grep("^boolean_multi$", colnames(data))]))
	}	
})

test_that("we can stream partial column data from a mock solr to R", {	
	mock_solr_client <- .jnew("rsolrstream/testthat/MockSolrClient")
	solr_client <- .jcast(mock_solr_client, "org/apache/solr/client/solrj/SolrClient")
	mock_solr_collection <- "collection_that_runs_Stuff"
	mock_streaming_expression <- "expression_name(collection_with_data, expession_details_go_here)"
	columns_to_return <- c("long", "boolean", "string_multi")
	data <- solr_stream(solr_client, mock_solr_collection, mock_streaming_expression, columns_to_return)
	
	for(i in seq(1:2)) {
		expect_equal(length(columns_to_return), length(data[i,]))
		expect_equal(default_long_transform(mock_solr_client$getLong(as.integer(i))), data[i, grep("^long$", colnames(data))]);
		expect_equal(default_boolean_transform(mock_solr_client$getBoolean(as.integer(i))), data[i, grep("^boolean$", colnames(data))]);	
		expect_equal(default_string_transform(mock_solr_client$getStringMulti(as.integer(i))), default_string_transform(data[i, grep("^string_multi$", colnames(data))]))
	}			
})

test_that("we can ignore invalid column names, populating them with empty strings", {	
	mock_solr_client <- .jnew("rsolrstream/testthat/MockSolrClient")
	solr_client <- .jcast(mock_solr_client, "org/apache/solr/client/solrj/SolrClient")
	mock_solr_collection <- "collection_that_runs_Stuff"
	mock_streaming_expression <- "expression_name(collection_with_data, expession_details_go_here)"
	columns_to_return <- c("long", "invalid_column_name")
	data <- solr_stream(solr_client, mock_solr_collection, mock_streaming_expression, columns_to_return)
	
	for(i in seq(1:2)) {
		expect_equal(length(columns_to_return), length(data[i,]))
		expect_equal(default_long_transform(mock_solr_client$getLong(as.integer(i))), data[i, grep("^long$", colnames(data))]);
		expect_equal("", data[i, grep("^invalid_column_name$", colnames(data))]);
	}			
})

