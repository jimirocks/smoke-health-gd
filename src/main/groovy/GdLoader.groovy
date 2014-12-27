import com.gooddata.GoodData
import groovy.sql.Sql
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.QuoteMode

import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import java.text.SimpleDateFormat

/**
 * TODO
 */
class GdLoader {
    String user
    String password
    String project

    private Sql sql
    private ScriptEngine pyEngine
    private Date lastSuiteDate

    private CSVPrinter suitesCsv
    private File suitesFile
    private CSVPrinter testsCsv

    private File testsFile
    public static final isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    private static final splunkFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z")
    private static final JDBC_URI = 'jdbc:gdc:datawarehouse://secure.gooddata.com/gdc/datawarehouse/instances/'

    GdLoader(String user, String password, String project, String ads) {
        this.user = user
        this.password = password
        this.project = project
        sql = Sql.newInstance("$JDBC_URI$ads", user, password)
        lastSuiteDate = sql.firstRow("select max(checkdate) from suites").get('max') as Date
        ScriptEngineManager mgr = new ScriptEngineManager();
        pyEngine = mgr.getEngineByName("python")

    }

    def addEvent(Map<String,String> event) {
        if (suitesCsv == null) {
            suitesFile = File.createTempFile('suites', '.csv')
            suitesCsv = createCsvPrinter(suitesFile)
            println "creating temp csv $suitesFile.absolutePath"
        }
        if (testsCsv == null) {
            testsFile = File.createTempFile('tests', '.csv')
            testsCsv = createCsvPrinter(testsFile)
            println "creating temp csv $testsFile.absolutePath"
        }

        def timestamp = splunkFormat.parse(event['_time'])
        if (lastSuiteDate == null || timestamp.compareTo(lastSuiteDate) > 0) {

            def host = event['host'].tokenize('.').first()

            def smoke
            try {
                smoke = pyEngine.eval(event['smoke'])
            } catch (ScriptException ex) {
                println "cant' parse smoke output: ${event['smoke']}"
                return
            }


            def checkdate = isoFormat.format(timestamp)
            suitesCsv.printRecord(host, checkdate, smoke['status'])

            smoke['componentResults'].each {
                component, compResult -> compResult['messages'].each {
                    severity, testResults -> testResults.each { testResult ->
                        def m = testResult =~ /PID ([\d]+): (.*)/
                        testsCsv.printRecord(host, checkdate, component, severity, m[0][1], m[0][2], 0)
                    }
                }
            }
        }
    }

    private static CSVPrinter createCsvPrinter(File file) {
        new CSVPrinter(new OutputStreamWriter(new FileOutputStream(file)), CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL))
    }

    def loadToAds() {
        suitesCsv.close()
        testsCsv.close()

        println "loading suites into ADS"
        sql.execute("COPY suites (host, checkdate, status) from local '$suitesFile.absolutePath' DELIMITER ',' ENCLOSED BY '\"' ABORT ON ERROR;")
        println "loading tests into ADS"
        sql.execute("COPY healthchecks (host, checkdate, test, severity, process, message, dummy) from local '$testsFile.absolutePath' DELIMITER ',' ENCLOSED BY '\"' ABORT ON ERROR;")

    }

    def fullLoad() {
        def gd = new GoodData(user, password)
        def project = gd.projectService.getProjectById(project)

        def suites = gd.datasetService.getDatasetManifest(project, 'dataset.suites')
        suites.setMapping('id', 'label.suites.id')
        suites.setMapping('host', 'label.suites.host')
        suites.setMapping('timestamp', 'label.suites.timestamp')
        suites.setMapping('status', 'label.suites.status')
        suites.setMapping('checkdate', 'checkdate.date.mdyy')

        File suitesFile = File.createTempFile('suitesLoad', '.csv')
        def suitesLoadCsv = createCsvPrinter(suitesFile)
        suitesLoadCsv.printRecord('id', 'host', 'timestamp', 'status', 'checkdate')
        sql.eachRow("select host, checkdate as ts, status, to_char(checkdate, 'YYYY-MM-DD') as checkdate from suites") {
            suitesLoadCsv.printRecord("${it['host']}#${it['ts']}", it['host'], it['ts'], it['status'], it['checkdate'])
        }
        suitesLoadCsv.close()
        println "loading $suites.dataSet from $suitesFile.absolutePath"
        gd.datasetService.loadDataset(project, suites, new FileInputStream(suitesFile)).get()


        def healthchecks = gd.datasetService.getDatasetManifest(project, 'dataset.healthchecks')
        healthchecks.setMapping('suiteid', 'label.suites.id')
        healthchecks.setMapping('test', 'label.healthchecks.test')
        healthchecks.setMapping('severity', 'label.healthchecks.severity')
        healthchecks.setMapping('process', 'label.healthchecks.process')
        healthchecks.setMapping('message', 'label.healthchecks.message')
        healthchecks.setMapping('dummy', 'fact.healthchecks.dummy')

        File testsFile = File.createTempFile('testsLoad', '.csv')
        def testsLoadCsv = createCsvPrinter(testsFile)
        testsLoadCsv.printRecord('suiteid', 'test', 'severity', 'process', 'message', 'dummy')
        sql.eachRow("select host, checkdate as ts, test, severity, process, message, dummy from healthchecks") {
            testsLoadCsv.printRecord("${it['host']}#${it['ts']}", it['test'], it['severity'], it['process'], it['message'], it['dummy'])
        }
        testsLoadCsv.close()
        println "loading $healthchecks.dataSet from $testsFile.absolutePath"
        gd.datasetService.loadDataset(project, healthchecks, new FileInputStream(testsFile)).get()

        gd.logout()
    }

    def close() {
        sql.close()
    }

    String getLastLoad() {
        return sql.firstRow("select val from conf where key='lastload'").get('val')
    }

    void setLastLoad(String lastload) {
        sql.executeUpdate("update conf set val='$lastload' where key='lastload'")
    }
}

