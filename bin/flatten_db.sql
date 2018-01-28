CREATE TABLE flat_acris AS 

WITH master AS 
	(SELECT DISTINCT document_id, crfn, doc_type, doc_amount, pct_transferred 
	 FROM real_property_master 
	 WHERE doc_type LIKE 'DEED'),
parties AS 
	(SELECT DISTINCT document_id, party_type, name, good_through_date 
	 FROM real_property_parties 
	 WHERE party_type > 1),
legals AS 
	(SELECT DISTINCT document_id, street_number, street_name, unit 
	 FROM real_property_legals)

	SELECT master.document_id, name, street_number, street_name, unit, crfn, doc_type, doc_amount, pct_transferred, party_type, good_through_date
	FROM master
	LEFT JOIN parties
		ON master.document_id = parties.document_id
	LEFT JOIN legals
		ON master.document_id = legals.document_id;
