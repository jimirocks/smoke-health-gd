import com.splunk.Event
import com.splunk.JobExportArgs
import com.splunk.MultiResultsReaderXml
import com.splunk.SearchResults
import com.splunk.Service
import com.splunk.ServiceArgs

class SplunkConnector {
    String host
    String user
    String password

    def export(String from, String to, String query, Closure<Event> eventClosure) {
        // Create a map of arguments and add login parameters
        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername(user);
        loginArgs.setPassword(password);
        loginArgs.setHost(host);

        // Create a Service instance and log in with the argument map
        Service service = Service.connect(loginArgs);


        // Create an argument map for the export arguments
        JobExportArgs exportArgs = new JobExportArgs();
        exportArgs.setEarliestTime(from);
        exportArgs.setLatestTime(to);
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);


        InputStream exportSearch = service.export(query, exportArgs);

        // Display results using the SDK's multi-results reader for XML
        MultiResultsReaderXml multiResultsReader = new MultiResultsReaderXml(exportSearch);

        multiResultsReader.each {it.each {event-> eventClosure(event)}}

        multiResultsReader.close()
    }
}
