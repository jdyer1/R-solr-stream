to_solr_stream <- function(r_obj, col_names=NULL, rowname_col=NULL) {
	if(is.null(col_names)) {
		col_names <- colnames(r_obj)
	}	
	if(!is.null(rowname_col)) {
		col_names <- c(rowname_col, col_names)
	}
	dim_length <- length(dim(r_obj))
	
	queue <- .jnew("java/util/concurrent/SynchronousQueue")
		
	if(dim_length<2) {		
		col_types <- rep(class(r_obj), length(r_obj))
		to_solr_row(r_obj, col_types, queue)
	} else if(dim_length==2) {
		col_types <- sapply(r_obj, class)
		if(is.null(rowname_col)) {
			apply(r_obj, 1, to_solr_row, col_types)
		} else {
			col_types <- c("header", col_types)
			for(i in 1:dim(r_obj)[1]) {
				to_solr_row(cbind(rownames(r_obj)[i], r_obj[i,]), col_types, queue)
			}
		}
	} else {
		stop("r_obj must be a 1- or 2-dimensional object such as a vector or data.frame")
	}
}

to_solr_row <- function(row, col_types, queue) {
	r_input_row <- .jnew("rsolrstream/RInputRow", length(row))		
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
	.jcall(queue, "V", "put", r_input_row)
}