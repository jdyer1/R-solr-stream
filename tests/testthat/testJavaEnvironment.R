test_that("java system properties can be set and get", {	
	set_java_system_properties(c('PORK', 'BEEF', 'CHICKEN', 'FISH'))
	element_after_pork <- 'Z'
	element_after_chicken <- 'Z'
	for(e in get_java_system_properties()) {
		if(e=='PORK' || element_after_pork=='PORK') {
			element_after_pork <- e
		}
		if(e=='CHICKEN' || element_after_chicken=='CHICKEN') {
			element_after_chicken <- e
		}
	}
	expect_equal('BEEF', element_after_pork);
	expect_equal('FISH', element_after_chicken);
})