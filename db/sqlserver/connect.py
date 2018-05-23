import pymssql

class SQL():
    orgsrv = "10.128.132.166:1166"
    orgsrv = "10.128.132.167:1167"
    orgusr =  "usr_des_web"
    orgusr =  "usr_hom_web"
    orgpwd = "12qwRE$#56tydesweb"
    orgpwd = "12qwRE$#56tyhomweb"
#   orgdb  = "DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"
    orgdb  = "DB_MADUREIRA"
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
            if row[2].upper() in js.keys():
             tmp = js[row[2].upper()];
             if row[4].upper() in tmp.keys():
               #print("Add elementt %s " % row[4].upper());
               #print(tmp);
               tmp[ row[4].upper() ].append( {'name':row[0],'colun_orign':row[3],'column_dest':row[5]} );
             else:
              #print("Create array")
              #print(tmp)
               tmp[ row[4].upper() ] = [ {'name':row[0],'colun_orign':row[3],'column_dest':row[5]}]
             js[row[2].upper()]= tmp;
            else:
             js[row[2].upper()]= {row[4].upper():[ {'name':row[0],'colun_orign':row[3],'column_dest':row[5]}] };
        return(js);
    def searchREll(self,mainTable,jsConfig):
       for k in jsConfig.keys():
         if mainTable in  jsConfig[k].keys():
            return(k) ;
       return None;
    def createQuery(self,mainTable,jsConfig):
       tmpTable = mainTable;
       renkey = "";
       while True:
         if (len(jsConfig[tmpTable].keys()))==0:
#            print("search rel to table %s " % tmpTable);
             tmpTable = self.searchREll(tmpTable,jsConfig);
#            print(jsConfig[tmpTable])
#            break;
             if tmpTable is None:
               break;
#            print("found rel to table %s " % tmpTable);
             continue;
          
         renkey = jsConfig[tmpTable].keys()[0];
         sql = ""
         andCond = "on";
         sql =  "select %s.* from %s inner join %s " % (tmpTable,tmpTable, renkey );
         # on %s = %s "% (tmpTable,tmpTable, renkey, jsConfig[tmpTable][renkey]['colun_orign'], jsConfig[tmpTable][renkey]['column_dest']);
         for row in jsConfig[tmpTable][renkey]:
          sql = " %s %s %s = %s " % ( sql , andCond ,'.'.join([tmpTable,row['colun_orign']]),'.'.join([renkey,row['column_dest']]));
          andCond =  "and"
         print(sql);
         del jsConfig[tmpTable][renkey];
         if renkey in jsConfig.keys():
            tmpTable = renkey;
