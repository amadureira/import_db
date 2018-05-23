import db.sqlserver.connect

db = db.sqlserver.connect.SQL();
#db.createTable(db.getTables("DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"));
jsConfig = db.getRelation("DB_DES_CASASBAHIA_VIA_UNICA_MOBILE");
#print(jsConfig)
db.createQuery("CLIENTE",jsConfig);
