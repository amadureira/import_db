import pymssql

class SQL():
    orgsrv = "10.128.132.166:1166"
    orgusr =  "usr_des_web"
    orgpwd = "12qwRE$#56tydesweb"
    orgdb  = "DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"
    orgconn = "";

    destsrv = "10.128.132.167:1167"
    destusr =  "usr_hom_web"
    destpwd = "12qwRE$#56tyhomweb"
    destdb  = "DB_MADUREIRA"
    destconn = "";

    def __init__(self):
        self.orgconn = pymssql.connect(host=self.orgsrv, user=self.orgusr, password=self.orgpwd,database=self.orgdb)
        self.destconn = pymssql.connect(host=self.destsrv, user=self.destusr, password=self.destpwd,database=self.destdb,autocommit=True)

    def getTables(self,DB_NAME):
        cursorTab = self.orgconn.cursor();
        cursorTab.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
	return  cursorTab.fetchall();

    def createTable(self,tables):
        cursorCol  = self.orgconn.cursor();
        cursorDest = self.destconn.cursor();
        #cursorDest.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
        #print cursorDest.fetchall();
        for row in tables:
            sqlQuery = ""
            sqlQuery = "%s select CONCAT(CASE WHEN UPPER(COLUMN_NAME) IN('IN','OR','ORDER','AND','DATABASE')" % sqlQuery
            sqlQuery = "%s THEN CONCAT('[',COLUMN_NAME,']') ELSE COLUMN_NAME END,' '," % sqlQuery
            sqlQuery = "%s DATA_TYPE," % sqlQuery
            sqlQuery = "%s CASE DATA_TYPE" % sqlQuery
            sqlQuery = "%s WHEN 'text' THEN ''" % sqlQuery
            sqlQuery = "%s WHEN 'image' THEN ''" % sqlQuery
            sqlQuery = "%s ELSE CASE isnull(CHARACTER_MAXIMUM_LENGTH,'')" % sqlQuery
            sqlQuery = "%s WHEN 0 THEN ''" % sqlQuery
            sqlQuery = "%s WHEN -1 THEN ''" % sqlQuery
            sqlQuery = "%s ELSE CONCAT('(', CHARACTER_MAXIMUM_LENGTH,')') END END," % sqlQuery
            sqlQuery = "%s CASE IS_NULLABLE when 'YES' THEN '' ELSE ' NOT NULL'  END )" % sqlQuery 
            sqlQuery = "%s from INFORMATION_SCHEMA.COLUMNS where TABLE_name='%s' " % (sqlQuery,row[0])
            sqlQuery = "%s  order by ORDINAL_POSITION" % sqlQuery
#           print sqlQuery
#           break
            cursorCol.execute(sqlQuery);
	    createSQL= "create table %s (" % row[0];
            comma=''
            for col in cursorCol:
               createSQL= "%s %s %s " % (createSQL,comma, col[0]);
               comma=','
            createSQL='%s);' %  createSQL
            print createSQL;
            #print "Create table %s" % row[0]
            cursorDest.execute( createSQL );
            
#           break;
