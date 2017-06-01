# R-solr-stream
An interface between R and Apache Solr's Streaming Expressions

## Solr Server Requirements
* Streaming expression support is for "cloud" installations
* This package uses solrj version 6.5.1. Other 6.x and 7.x-snapshot releases may possibly work as well.

## Installation
* `git clone` the R-solr-stream repository
* Build: `mvn clean install package`
* R installation: `install.packages("/path/to/R-solr-stream", repos=NULL)`

## Running the R unit tests
* `setwd("/path/to/R-solr-stream")`
* `devtools::test()`

## Use
* This example assumes a solr collection named "stream" with the following schema.xml

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<schema name="stream" version="1.6">
	<field name="_version_" type="long" indexed="true" stored="true" docValues="true" />
	<field name="_root_" type="string" indexed="true" stored="false" />
	<field name="id" type="string" indexed="true" stored="true" docValues="true" required="true" multiValued="false" />

	<dynamicField name="s_*" type="string" indexed="true" stored="true" docValues="true" multiValued="false" />
	<dynamicField name="i_*" type="int" indexed="true" stored="true" docValues="true" multiValued="false" />
	<dynamicField name="f_*" type="float" indexed="true" stored="true" docValues="true" multiValued="false" />

	<uniqueKey>id</uniqueKey>

	<fieldType name="string" class="solr.StrField" sortMissingLast="true" />
	<fieldType name="int" class="solr.TrieIntField" precisionStep="8" positionIncrementGap="0"/>
	<fieldType name="float" class="solr.TrieFloatField" precisionStep="8" positionIncrementGap="0"/>  
</schema>
```

* build and install the package as shown above.
* load the package and its dependencies: `library(rSolrStream)`
* (optional), set up java environment variables as needed if using SSL, zookeeper credentials, etc:
```
p <- c("zkCredentialsProvider", "org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider",
"zkDigestUsername","solr",
"zkDigestPassword","solrrocks",
"javax.net.ssl.keyStore", "/path/to/keystore",
"javax.net.ssl.keyStorePassword", "password",
"javax.net.ssl.trustStore", "/path/to/truststore",
"javax.net.ssl.trustStorePassword", "password")
set_java_system_properties(p)
```
* obtain a Cloud Solr Client:
```
solr_client <- solr_client_from_zookeeper_hosts("myhost1:2181,myhost2:2181,myhost3:2181")
```
* stream the built-in "mtcars" data.frame to solr:
```
to_solr_stream(mtcars, solr_client, "stream", sort_column="i_seq", rowname_col="id", col_names="f_mpg,i_cyl,f_disp,i_hp,f_drat,f_wt,f_qsec,i_vs,i_am,i_gear,i_carb")
```
* stream the data back from solr into a new data.frame:
```
streaming_expression <- "search(stream, q=\"*:*\", fl=\"i_seq,id,f_mpg,i_cyl,f_disp,i_hp,f_drat,f_wt,f_qsec,i_vs,i_am,i_gear,i_carb\", sort=\"i_seq asc\", qt=\"/export\")"
new_df <- solr_stream(solr_client, "stream", streaming_expression)
```

## Difference from the existing "rsolr" package
* This package is concerned with solr streaming expressions whereas "rsolr" handles solr's other features.
* This package is a wrapper around solrj, with a dependency on "rjava", whereas "rsolr" is a complete client in of itself.
