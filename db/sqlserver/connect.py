import pymssql
import  sys

class SQL():
    orgsrv = "10.128.132.166:1166"
#   orgsrv = "10.128.132.167:1167"
    orgusr =  "usr_des_web"
#   orgusr =  "usr_hom_web"
    orgpwd = "12qwRE$#56tydesweb"
#   orgpwd = "12qwRE$#56tyhomweb"
    orgdb  = "DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"
#   orgdb  = "DB_MADUREIRA"
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
    def logs(self,msg,LEVEL=0):
        print(msg);
    def getTables(self,DB_NAME):
        self.logs("Search for tables",1);
        cursorTab = self.orgconn.cursor();
        cursorTab.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
        return  cursorTab.fetchall();

    def createTable(self,tables):
        cursorCol  = self.orgconn.cursor();
        cursorDest = self.destconn.cursor();
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
               tmp[ row[4].upper() ].append( {'name':row[0],'colun_orign':row[3],'column_dest':row[5]} );
             else:
               tmp[ row[4].upper() ] = [ {'name':row[0],'colun_orign':row[3],'column_dest':row[5]}]
             js[row[2].upper()]= tmp;
            else:
             js[row[2].upper()]= {row[4].upper():[ {'name':row[0],'colun_orign':row[3],'column_dest':row[5]}] };
        return(js);
    def searchREll(self,mainTable,jsConfig):
       self.logs(("Search for relation for table %s" % mainTable ),1);
       for k in jsConfig.keys():
         if mainTable in  jsConfig[k].keys():
            self.logs("Found %s table related with %s" % (k,mainTable));
            return(k) ;
       
       return None;
    def createQuery(self,sql,mainTable,jsConfig):
       tmpTable        = mainTable;
       renkey          = "";
       jsVal           = {};
       jsVal[tmpTable] = self.getSelect(sql);
       self.logs("Start",1);
       while True:
         self.logs("Start While");
         if (len(jsConfig[tmpTable].keys()))==0:
             tmpTable = self.searchREll(tmpTable,jsConfig);
             if tmpTable is None:
               break;
             continue;
         renkey = (list(jsConfig[tmpTable])[0]);
         self.logs("Search on %s table from table %s" % (renkey,tmpTable));
         sql = ""
         andCond = "";
         sql =  "select * from %s where " % renkey;
         for row in jsConfig[tmpTable][renkey]:
          ids=[];
          for inRow in jsVal[mainTable]:
            try:
             ids.append(str(inRow[row['column_dest']]));
            except  KeyError as e:
             self.logs("Error ");
          sql = "%s %s %s in  ( %s )" % (sql,andCond,row['column_dest'],','.join(ids));
          andCond =  "and"

         jsVal[renkey] = self.getSelect(sql);
         del jsConfig[tmpTable][renkey];
         if renkey in jsConfig.keys():
            tmpTable = renkey;

    def getSelect(self,Query):
       try:
          cursorTab = self.orgconn.cursor();
          self.logs(Query);
          cursorTab.execute(Query);
       except MSSQLDatabaseException as e:
         self.logs(e);

       except pymssql.OperationalError as e:
         self.logs(e);
         self.logs(Query);
       arrReturn = [];
       for row in cursorTab:
          c=0;
          tmp={};
          for col in  cursorTab.description:
             try:
              if row[c] is  None:
                 self.logs("Set None");
                 tmp[col[0]] = None;
              elif col[1] in [1,4,3]:
                 self.logs("Set str or int %s" % col[0]);
                 tmp[col[0]] =   str(row[c]).encode('utf-8') if col[1] in [1,4] else int(row[c]);
              else:
                 self.logs("Set Else");
                 tmp[col[0]] = row[c];
              c=c+1;   
             except TypeError as e:
              self.logs(row[c] is None);
              self.logs(type(row[c]));
              sys.exit(0);
             except ValueError as e:
              sys.exit(0);
          arrReturn.append(tmp);
       return(arrReturn); 
