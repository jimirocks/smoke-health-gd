def splunkUser = args[0]
def splunkPass = args[1]
def gdUser = args[2]
def gdPass = args[3]
def gdProject = args[4]
def ads = args[5]

def splunk = new SplunkConnector(host: 'splunk.intgdc.com', user: splunkUser, password: splunkPass)
def gdLoader = new GdLoader(gdUser, gdPass, gdProject, ads)
def now = GdLoader.isoFormat.format(new Date())
def search = """search host=\"*msf*\"  \" Plugin java.cloverexecute-health result*\" source=\"/mnt/log/gdc-python\"
    | rex \"^.*java\\.cloverexecute-health result: (?<smoke>{.*}).*\$\" | table _time, smoke, host"""

splunk.export(gdLoader.lastLoad, now, search) {
    gdLoader.addEvent(it)
}

gdLoader.loadToAds()
gdLoader.fullLoad()
gdLoader.lastLoad = now

gdLoader.close()