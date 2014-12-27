create table conf (key varchar, val varchar);
insert into conf values('lastload', '2014-12-26T00:00:00.000+01:00')

create table suites (
  host varchar(255),
  checkdate timestamptz,
  status varchar(10),
  PRIMARY KEY(host, checkdate)
)

create table healthchecks (
  checkdate timestamptz,
  host varchar(255),
  test varchar(255),
  severity varchar(15),
  process varchar(5),
  message varchar(1024),
  dummy integer,
  FOREIGN KEY (host, checkdate) REFERENCES suites(host, checkdate) );