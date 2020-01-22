CREATE TEMP TABLE iatistore_iaticodelistitem_name AS (
	WITH src AS (
		SELECT 
			iatistore_iaticodelist.id iaticodelist_id,
			xmltable.*
		FROM iatistore_iaticodelist,
		XMLTABLE(
			'/codelist/codelist-items/codelist-item/name/narrative' PASSING "content"
			COLUMNS "lang" text PATH '@xml:lang',
			"default_lang" text PATH '../../../../@xml:lang',
			"content" text PATH '.',
			"code" text PATH '../../code'
		)
	), json_aggregated AS (
		SELECT
			iaticodelist_id, code,
			json_object_agg(COALESCE(lang, default_lang), content) agg
		FROM src
		WHERE content != ''
		GROUP BY iaticodelist_id, code
	) SELECT * FROM json_aggregated);


CREATE INDEX ON iatistore_iaticodelistitem_name (code);
CREATE INDEX ON iatistore_iaticodelistitem_name (iaticodelist_id);
	
UPDATE iatistore_iaticodelistitem li SET "description" = (
	SELECT agg FROM iatistore_iaticodelistitem_name ja 
	WHERE ja.iaticodelist_id = li.codelist_id
	AND ja.code = li.code
);

DROP TABLE iatistore_iaticodelistitem_name;
