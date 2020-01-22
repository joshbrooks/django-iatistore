-- This code will update the values of the "Codelist" with a Name and Description
-- field based on the XML content.
WITH cols AS
  (SELECT id,
          xmltable.content,
          COALESCE(lang, default_lang) lc
   FROM iatistore_iaticodelist,
        xmltable('/codelist/metadata/name/narrative' passing content columns "lang" text PATH '@xml:lang', "default_lang" text PATH '../../../@xml:lang', "content" text PATH '.')),
     json_aggregated AS
  (SELECT id,
          json_object_agg(lc, content) agg
   FROM cols
   GROUP BY id
   )
UPDATE iatistore_iaticodelist
SET "name" =
  (SELECT agg
   FROM json_aggregated
   WHERE json_aggregated.id = iatistore_iaticodelist.id);