solr_stream <- function(solr_client, solr_collection, streaming_expression, col_names=NULL, col_transform_functions=NULL) {
	iterator <- .jcall("rsolrstream/RStreamingExpressions", "Lrsolrstream/RStreamingExpressionIterator;", 
			"executeStreamingExpression", solr_client, solr_collection, streaming_expression, col_names)
	if(is.null(col_names)) {
		col_names <- .jcall(iterator, "[Ljava/lang/String;", "columnNames")	
	}
	if(is.null(col_transform_functions)) {
		col_transform_functions <- array(dim=length(col_names))
	}
	boolean_indexes <- .jcall(iterator, "[I", "booleanIndexes")
	long_indexes <- .jcall(iterator, "[I", "longIndexes")
	double_indexes <- .jcall(iterator, "[I", "doubleIndexes")
	string_indexes <- .jcall(iterator, "[I", "stringIndexes")
	
	data <- expanding_list()
	
	while(.jcall(iterator, "Z", "hasNext")) {
		data_obj <- .jcall(iterator, "Ljava/lang/Object;", "next")	
		data_row <- .jcast(data_obj, "rsolrstream/RStreamingExpressionRow")
		values <- vector(mode="list", length=length(col_names))
		i <- 0
		for(j in boolean_indexes) {
			boolean_values <- .jcall(data_row, "[Z", "getBooleans", as.integer(i))
			if(is.na(col_transform_functions[[j+1]])) {
				values[[(j+1)]] <- default_boolean_transform(boolean_values) 
			} else {
				values[[(j+1)]] <- col_transform_functions[[j+1]](boolean_values)
			}
			i <- i + 1
		}
		i <- 0
		for(j in long_indexes) {
			long_values <- .jcall(data_row, "[J", "getLongs", as.integer(i))
			if(is.na(col_transform_functions[[j+1]])) {
				values[[(j+1)]] <- default_long_transform(long_values) 
			} else {
				values[[(j+1)]] <- col_transform_functions[[j+1]](long_values)
			}
			i <- i + 1
		}
		i <- 0
		for(j in double_indexes) {
			double_values <- .jcall(data_row, "[D", "getDoubles", as.integer(i))
			if(is.na(col_transform_functions[[j+1]])) {
				values[[(j+1)]] <- default_double_transform(double_values) 
			} else {
				values[[(j+1)]] <- col_transform_functions[[j+1]](double_values)
			}
			i <- i + 1
		}
		i <- 0
		for(j in string_indexes) {
			string_values <- .jcall(data_row, "[Ljava/lang/String;", "getStrings", as.integer(i))
			if(is.na(col_transform_functions[[j+1]])) {
				values[[(j+1)]] <- default_string_transform(string_values) 
			} else {
				values[[(j+1)]] <- col_transform_functions[[j+1]](string_values)
			}
			i <- i + 1
		}
		data$add(values)
	}
	data <- rbindlist(data$as.list())	
	colnames(data) <- col_names
	data
}

default_string_transform <- function(value) {
	paste(value, collapse = ' ')
}
default_boolean_transform <- function(value) {
	all(value)
}
default_long_transform <- function(value) {
	median(value)
}
default_double_transform <- function(value) {
	median(value)
}
first_value_transform <- function(value) {
	value[[1]]
}

#Taken directly from: http://stackoverflow.com/questions/2436688/append-an-object-to-a-list-in-r-in-amortized-constant-time-o1/32870310#32870310
# accessed on May 16, 2017.
expanding_list <- function(capacity = 1000) {
	buffer <- vector('list', capacity)
	length <- 0	
	methods <- list()	
	methods$double.size <- function() {
		buffer <<- c(buffer, vector('list', capacity))
		capacity <<- capacity * 2
	}	
	methods$add <- function(val) {
		if(length == capacity) {
			methods$double.size()
		}		
		length <<- length + 1
		buffer[[length]] <<- val
	}	
	methods$as.list <- function() {
		b <- buffer[0:length]
		return(b)
	}	
	methods
}
