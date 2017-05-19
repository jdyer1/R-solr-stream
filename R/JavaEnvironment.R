set_java_system_properties <- function(vector_alternating_key_and_value) {
	.jcall("rsolrstream/JavaEnvironment", "V", "setSystemProperties", vector_alternating_key_and_value)
}

get_java_system_properties <- function() {
	props <- .jcall("rsolrstream/JavaEnvironment", "[Ljava/lang/String;", "systemProperties")
	props
}

print_java_system_properties <- function() {
	.jcall("rsolrstream/JavaEnvironment", "V", "logSystemProperties")
}