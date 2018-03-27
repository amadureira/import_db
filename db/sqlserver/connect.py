import pymssql

class SQL():
    orgsrv = "10.128.132.166:1166"
    orgusr =  "usr_des_web"
    orgpwd = "12qwRE$#56tydesweb"
    orgdb  = "DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"
    orgconn = "";
#   js = {} ;
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
            cursorCol.execute(sqlQuery);
            createSQL= "create table %s (" % row[0];
            comma=''
            for col in cursorCol:
               createSQL= "%s %s %s " % (createSQL,comma, col[0]);
               comma=','
            createSQL='%s);' %  createSQL
            cursorDest.execute( createSQL );
    def getRelation(self,dbName):
        cursor = [['a','b'],['b','c'],['b','d'],['b','e']]
        cursor  = self.orgconn.cursor();
        js= {} 
        sql = ""
        sql= "%s SELECT  obj.name AS FK_NAME,sch.name AS [schema_name],tab1.name AS [table], " % sql ;
        sql = "%s col1.name AS [column],tab2.name AS [referenced_table],col2.name AS [referenced_column] " % sql;
        sql = "%s FROM sys.foreign_key_columns fkc INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id " % sql
        sql = "%s INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id " % sql
        sql = "%s INNER JOIN sys.schemas sch ON tab1.schema_id = sch.schema_id INNER JOIN sys.columns col1 " % sql
        sql = "%s ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id "  % sql
        sql = "%s INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id " % sql 
        sql = "%s INNER JOIN sys.columns col2 ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id " % sql
        cursor.execute(sql);
        js = {} ;
        for row in cursor:
#            print row;
            if row[2] in js.keys():
	      tmp = js[row[2]] ;
              tmp[ row[4] ] =  {'name':row[0],'colun_orign':row[3],'column_dest':row[4]}
              js[row[2]]= tmp;
            else:
              js[row[2]]= {row[4]: {'name':row[0],'colun_orign':row[3],'column_dest':row[4]} };
        print(js);

