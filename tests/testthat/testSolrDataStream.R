test_that("we can stream data from a mock solr to R", {	
	mock_solr_client <- .jnew("rsolrstream/testthat/MockSolrClient")
	mock_solr_collection <- "collection_that_runs_Stuff"
	mock_streaming_expression <- "expression_name(collection_with_data, expession_details_go_here)"
	
	data <- solr_stream(mock_solr_client, mock_solr_collection, mock_streaming_expression)
	print(data)
})