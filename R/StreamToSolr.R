to_solr_stream <- function(r_obj, cloud_solr_client, solr_collection, col_names=NULL, rowname_col=NULL, sort_column=NULL, 
		sort_expression=NULL, timeout_in_milliseconds=1000, outer_expression_generator_function=update_solr_collection) {
	if(is.null(col_names)) {
		col_names <- colnames(r_obj)
	}	
	if(!is.null(rowname_col)) {
		col_names <- c(rowname_col, col_names)
	}
	if(is.null(sort_expression)) {
		if(is.null(sort_column)) {
			stop("must supply either 'sort_expression' or 'sort_column'.")				
		}
		sort_expression <- paste(sort_column, "asc")
	}
	sort_column_counter <- NULL
	if(!is.null(sort_column)) {
		sort_column_counter <- id_column_counter()
		col_names <- c(col_names, sort_column)
	}
	
	if(!.jinstanceof(cloud_solr_client, "org/apache/solr/client/solrj/impl/CloudSolrClient")) {
		stop("Must create a Cloud Solr Client from zookeeper hosts")
	}
	cloud_solr_client_cast <- .jcast(cloud_solr_client, "org/apache/solr/client/solrj/impl/CloudSolrClient")
	zk_host <- .jcall(cloud_solr_client_cast, "Ljava/lang/String;", "getZkHost")	
	
	dim_length <- length(dim(r_obj))
	if(dim_length>2) {
		stop("r_obj must be a 1- or 2-dimensional object such as a vector or data.frame")
	}
	
	queue <- .jnew("java/util/concurrent/SynchronousQueue")
	blocking_queue <- .jcast(queue, "java/util/concurrent/BlockingQueue")
	queue_name <- paste("RStream", as.numeric(Sys.time()), sep="-")
	.jcall("rsolrstream/RStream", "V", "registerQueue", queue_name, blocking_queue)
	
	col_names_comma_delimited <- paste(col_names, collapse=",")	
	expression <- outer_expression_generator_function(zk_host, solr_collection, paste("R(sort=\"", sort_expression, "\", readTimeoutMillis=", 
					timeout_in_milliseconds, ", columnNames=\"", col_names_comma_delimited, "\", queueName=\"", 
					queue_name, "\")", sep=""))	
	bse <- .jnew("rsolrstream/BackgroundStreamingExpression", expression)
	.jcall(bse, "V", "submit")
			
	if(dim_length<2) {		
		col_types <- rep(class(r_obj), length(r_obj))
		to_solr_row(r_obj, col_types, queue, sort_column_counter)
	} else {
		col_types <- sapply(r_obj, class)
		if(is.null(rowname_col)) {
			apply(r_obj, 1, to_solr_row, col_types, queue, sort_column_counter)
		} else {
			col_types <- c("header", col_types)
			for(i in 1:dim(r_obj)[1]) {
				to_solr_row(cbind(rownames(r_obj)[i], r_obj[i,]), col_types, queue, sort_column_counter)
			}
		}
	} 
	
	#signals EOF
	to_solr_row(list(), list(), queue, NULL)
	NULL
}

update_solr_collection <- function(zk_host, solr_collection, inner_expression) {
	paste("update(", solr_collection, ", batchSize=10, ", inner_expression, ")", sep="")
}

to_solr_row <- function(row, col_types, queue, sort_column_counter) {
	l <- length(row)
	if(!is.null(sort_column_counter)) {
		l <- l + 1
	}
	r_input_row <- .jnew("rsolrstream/RInputRow", as.integer(l))		
	i <- 1
	for(x in row) {
		if(col_types[i]=="logical") {
			.jcall(r_input_row, "V", "setBoolean", as.integer(i-1), x)
		} else if(col_types[i]=="integer") {
			.jcall(r_input_row, "V", "setLong", as.integer(i-1), x)
		} else if(col_types[i]=="numeric") {
			.jcall(r_input_row, "V", "setDouble", as.integer(i-1), x)
		} else if(col_types[i]=="character") {
			.jcall(r_input_row, "V", "setString", as.integer(i-1), x)
		} else {
			.jcall(r_input_row, "V", "setString", as.integer(i-1), toString(x))
		}
		i <- i + 1
	}
	if(!is.null(sort_column_counter)) {
		.jcall(r_input_row, "V", "setLong", as.integer(i-1), .jlong(sort_column_counter$value()))	
		sort_column_counter$increment()
	}
	.jcall(queue, "V", "put", .jcast(r_input_row, "java/lang/Object"))
}

# based on: https://www.r-bloggers.com/a-little-r-counter/
# accessed May 31, 2017.
id_column_counter <- function() {
	curr_count <- 1
	list(
		increment = function() {
			curr_count <<- curr_count + 1
		},
		value = function() {
			return(curr_count)
		}
	)
}