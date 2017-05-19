package rsolrstream;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaEnvironment {
    private static final Logger log = LoggerFactory.getLogger(JavaEnvironment.class);

    private JavaEnvironment() {
    }

    public static void setSystemProperties(String[] props) {
        for (int i = 0; i < props.length - 1; i += 2) {
            System.setProperty(props[i], props[i + 1]);
            log.debug("set system property k/v: {}/{}", props[i], props[i + 1]);
        }
    }

    public static String[] systemProperties() {
        Map<Object, Object> p = System.getProperties();
        String[] ps = new String[p.size() * 2];
        int i = 0;
        for (Map.Entry<Object, Object> entry : p.entrySet()) {
            ps[i++] = entry.getKey().toString();
            ps[i++] = entry.getValue().toString();
        }
        return ps;
    }

    public static void logSystemProperties() {
        Map<Object, Object> p = System.getProperties();
        SortedMap<String,String> sortedProperties = new TreeMap<>();
        for(Map.Entry<Object,Object> entry : p.entrySet()) {
            sortedProperties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        for(Map.Entry<String,String> entry : sortedProperties.entrySet()) {
            log.info("{}={}",entry.getKey(),entry.getValue());
        }
        
    }

}
