#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

import db.sqlserver.connect
sql="SELECT * FROM COLECAO  where Colecao.idColecao = 20";
table= "COLECAO"
db = db.sqlserver.connect.SQL();
#db.createTable(db.getTables("DB_DES_CASASBAHIA_VIA_UNICA_MOBILE"));
jsConfig = db.getRelation("DB_DES_CASASBAHIA_VIA_UNICA_MOBILE");
#print(jsConfig);
jsVal = db.createQuery(sql,table,jsConfig);
#jsVal = db.getSelect("SELECT * FROM COLECAO  where Colecao.idColecao BETWEEN 20 and  28");
print(jsVal);

