INSERT INTO iatistore_iaticodelistitem (codelist_id, code, activation_date, status, withdrawal_date)
	SELECT DISTINCT
		iatistore_iaticodelist.id codelist_id,
		xmltable."code",
		xmltable."activation_date",
		xmltable."status",
		xmltable."withdrawal_date" 
	FROM iatistore_iaticodelist,
	XMLTABLE(
		'/codelist/codelist-items/codelist-item' PASSING "content" 
		COLUMNS
			"code" text PATH 'code',
			"activation_date" date PATH '@activation-date',
			"status" text PATH '@status',
			"withdrawal_date" date PATH '@withdrawal-date'
	 )