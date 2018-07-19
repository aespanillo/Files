-- Function: public.credit_assessment_manual_search(numeric, character varying, character varying, character varying, numeric, character varying, character varying, character varying, numeric, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, numeric, character varying, character varying, character varying, character varying, character varying, numeric, numeric, numeric)

-- DROP FUNCTION public.credit_assessment_manual_search(numeric, character varying, character varying, character varying, numeric, character varying, character varying, character varying, numeric, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, numeric, character varying, character varying, character varying, character varying, character varying, numeric, numeric, numeric,character varying);

CREATE OR REPLACE FUNCTION public.credit_assessment_manual_search(
    IN infotype numeric,
    IN loantypein character varying,
    IN appdatefrom character varying,
    IN appdateto character varying,
    IN applicationtypein numeric,
    IN appcdlike character varying,
    IN financepricerange character varying,
    IN storecdlike character varying,
    IN storenamelike character varying,
    IN idcardtypein numeric,
    IN idcardnolike character varying,
    IN namelike character varying,
    IN localnamelike character varying,
    IN givennamelike character varying,
    IN middlenamelike character varying,
    IN familynamelike character varying,
    IN surveystatuslist character varying,
    IN workflowstatus character varying,
    IN guarantorinfotype numeric,
    IN guarantoridcardnolike character varying,
    IN guarantornamelike character varying,
    IN accountcodein character varying,
    IN countrycode character varying,
    IN displayorder character varying,
    IN countflag numeric,
    IN searchlimit numeric,
    IN searchoffset numeric,
	IN merchantcategory character varying,
	IN customerstate character varying)
  RETURNS TABLE(sysappcd character varying, appcd character varying, appdate character varying, applicationtype numeric, appstatus numeric, storename character varying, customercd character varying, judgementdate character varying, updcnt numeric, name character varying, localname character varying, givenname character varying, middlename character varying, familyname character varying, localfirstname character varying, localmiddlename character varying, localfamilyname character varying, idcardtype numeric, idcardno character varying, agentname character varying, financeprice numeric, currentworkflowstat character varying, loantype character varying, svystatus numeric, ismember numeric) AS
$BODY$

DECLARE string_first_assessment CHARACTER VARYING;
DECLARE string_second_assessment CHARACTER VARYING;
DECLARE string_final_assessment CHARACTER VARYING;

DECLARE sql_status_list CHARACTER VARYING;
DECLARE sql_assessor_authority CHARACTER VARYING;
DECLARE sql_applications CHARACTER VARYING;
DECLARE sql_customers CHARACTER VARYING;
DECLARE sql_agents CHARACTER VARYING;
DECLARE sql_apps_under_assessment CHARACTER VARYING;

BEGIN

string_first_assessment := 'FIRST_ASSESSMENT';
string_second_assessment := 'SECOND_ASSESSMENT';
string_final_assessment := 'FINAL_ASSESSMENT';

sql_status_list := '
CREATE TEMPORARY TABLE credit_assessment_manual_search_status_list ON COMMIT DROP AS
SELECT a.statuslist::NUMERIC
FROM (
	SELECT UNNEST(STRING_TO_ARRAY(';
IF surveyStatusList IS NOT NULL THEN sql_status_list := sql_status_list || quote_literal(surveyStatusList) || ' ';
ELSE sql_status_list := sql_status_list || quote_literal('0');
END IF;
sql_status_list := sql_status_list || ', ' || quote_literal(',') || ')) AS statuslist
	) a
WHERE a.statuslist <> ' || quote_literal('') || '; ';

sql_assessor_authority := '
CREATE TEMPORARY TABLE credit_assessment_manual_search_assessor_authority ON COMMIT DROP AS
SELECT tass.accountcode,
	mcred.loantype,
	mcred.judgementtype
FROM t_assessor tass
INNER JOIN m_creditassess mcred ON tass.positiontype=mcred.positiontype
WHERE tass.delflag=0 ';
IF countryCode IS NOT NULL THEN sql_assessor_authority := sql_assessor_authority || ' AND mcred.countrycd=' || quote_literal(countryCode) || ' '; END IF;
IF accountCodeIn IS NOT NULL THEN sql_assessor_authority := sql_assessor_authority || ' AND tass.accountcode=' || quote_literal(accountCodeIn) || ' '; END IF;
IF workflowStatus IS NOT NULL THEN sql_assessor_authority := sql_assessor_authority || ' AND mcred.judgementtype = ' || quote_literal(workflowStatus) || ' '; END IF;
sql_assessor_authority := sql_assessor_authority || '
ORDER BY tass.accountcode,
		mcred.loantype,
		mcred.judgementtype; ';
		
sql_applications := '
CREATE TEMPORARY TABLE credit_assessment_manual_search_applications ON COMMIT DROP AS
	SELECT tap.sysappcd,
		tap.appcd,
		tap.appdate,
		tap.applicationtype,
		tap.appstatus,
		tap.storename,
		tap.judgementdate,
		tap.updcnt,
		twf.currentworkflowstat,
		tap.loantype,
		CASE WHEN tap.applicationtype = 3 THEN 1
			ELSE 0
			END::NUMERIC AS ismember,
		tap.storecd,
		tap.customercd
	FROM t_workflow twf
	INNER JOIN t_application tap ON twf.appno=tap.appcd
	INNER JOIN t_product tprod ON tap.sysappcd=tprod.sysappcd
	INNER JOIN t_customer tcust ON(tcust.infotype = 1 AND tap.sysappcd=tcust.sysappcd)
	INNER JOIN credit_assessment_manual_search_assessor_authority aa ON aa.loantype::CHARACTER VARYING=tap.loantype
		AND aa.judgementtype=twf.currentworkflowstat
	WHERE tap.delflag=0 ';
	IF workflowStatus IS NOT NULL THEN sql_applications := sql_applications || ' AND twf.currentworkflowstat=' || quote_literal(workflowStatus) || ' ';
	ELSE sql_applications := sql_applications || ' AND twf.currentworkflowstat IN ('
		|| quote_literal(string_first_assessment) || ', '
		|| quote_literal(string_second_assessment) || ', '
		|| quote_literal(string_final_assessment) || ') ';
	END IF;
	IF appdateFrom IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.appdate >= ' || quote_literal(appdateFrom) || ' '; END IF;
	IF appdateTo IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.appdate <= ' || quote_literal(appdateTo) || ' '; END IF;
	IF loanTypeIn IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.loantype=' || quote_literal(loanTypeIn) || ' '; END IF;
	IF applicationTypeIn IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.applicationtype=' || applicationTypeIn || ' '; END IF;
	IF appCdLike IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.appcd LIKE ' || quote_literal(appCdLike) || ' '; END IF;
	IF storeCdLike IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.storecd LIKE ' || quote_literal(storeCdLike) || ' '; END IF;	
	IF storeNameLike IS NOT NULL THEN sql_applications := sql_applications || ' AND tap.storename LIKE ' || quote_literal(storeNameLike) || ' '; END IF;
	IF merchantCategory IS NOT NULL THEN sql_applications := sql_applications || ' AND tprod.category =' || quote_literal(merchantCategory) || ' '; END IF;
	IF customerState IS NOT NULL THEN sql_applications := sql_applications || ' AND  tcust.state =' || quote_literal(customerState) || ' '; END IF;	
sql_applications := sql_applications || '; ';

sql_customers := '
CREATE TEMPORARY TABLE credit_assessment_manual_search_customers ON COMMIT DROP AS
SELECT mc.customercd,
	mc.name,
	mc.localname,
	mc.givenname,
	mc.middlename,
	mc.familyname,
	mc.localfirstname,
	mc.localmiddlename,
	mc.localfamilyname,
	mid.idcardtype,
	mid.idcardno,
	mc.delflag,
	aa.appcd
FROM credit_assessment_manual_search_applications aa
INNER JOIN m_customer mc ON aa.customercd=mc.customercd
INNER JOIN m_customer_id mid ON aa.customercd=mid.customercd
WHERE mc.delflag=0 ';
IF nameLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mc.name LIKE ' || quote_literal(nameLike) || ' '; END IF;
	IF localnameLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mc.localname LIKE ' || quote_literal(localnameLike) || ' '; END IF;
	IF givennameLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mc.givenname LIKE ' || quote_literal(givennameLike) || ' '; END IF;
	IF middlenameLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mc.middlename LIKE ' || quote_literal(middlenameLike) || ' '; END IF;
	IF familynameLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mc.familyname LIKE ' || quote_literal(familynameLike) || ' '; END IF;
	IF idCardNoLike IS NOT NULL THEN sql_customers := sql_customers || ' AND mid.idcardno LIKE ' || quote_literal(idCardNoLike) || ' '; END IF;
	IF idCardTypeIn IS NOT NULL THEN sql_customers := sql_customers || ' AND mid.idcardtype=' || idCardTypeIn || ' '; END IF;
sql_customers := sql_customers || '; ';

sql_agents := '
CREATE TEMPORARY TABLE credit_assessment_manual_search_agents ON COMMIT DROP AS
SELECT ms.storecd,
	ma.agentname
FROM credit_assessment_manual_search_applications aa
INNER JOIN m_store ms ON aa.storecd=ms.storecd
INNER JOIN m_agent ma ON ms.agentcd=ma.agentcd
GROUP BY ms.storecd, ma.agentname; ';

sql_apps_under_assessment := '
SELECT ta.sysappcd,
	ta.appcd,
	ta.appdate,
	ta.applicationtype,
	ta.appstatus,
	ta.storename,
	mc.customercd,
	ta.judgementdate,
	ta.updcnt,
	mc.name,
	mc.localname,
	mc.givenname,
	mc.middlename,
	mc.familyname,
	mc.localfirstname,
	mc.localmiddlename,
	mc.localfamilyname,
	mc.idcardtype,
	mc.idcardno,
	ma.agentname,
	tpay.financeprice,
	ta.currentworkflowstat,
	ta.loantype,
	COALESCE(tsvy.status, 0) AS svystatus,
	ta.ismember
FROM credit_assessment_manual_search_applications ta
INNER JOIN credit_assessment_manual_search_customers mc ON ta.customercd=mc.customercd AND ta.appcd=mc.appcd
INNER JOIN credit_assessment_manual_search_agents ma ON ta.storecd=ma.storecd
INNER JOIN t_paystagesapplication tpay ON ta.sysappcd= tpay.sysappcd
INNER JOIN (
	SELECT a.sysappcd
	FROM credit_assessment_manual_search_applications aa
	INNER JOIN t_customer a ON aa.sysappcd=a.sysappcd
	WHERE a.delflag=0 ';
IF guarantorInfoType IS NOT NULL THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND a.infotype=' || guarantorInfoType || ' '; 
	IF guarantorIdCardNoLike IS NOT NULL THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND a.idcardno LIKE ' || quote_literal(guarantorIdCardNoLike) || ' '; END IF;
	IF guarantorNameLike IS NOT NULL THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND a.name LIKE ' || quote_literal(guarantorNameLike) || ' '; END IF;
	END IF;
sql_apps_under_assessment := sql_apps_under_assessment || '
	GROUP BY a.sysappcd
	) tcust ON ta.sysappcd=tcust.sysappcd
LEFT OUTER JOIN (
	SELECT a.appcd,
		MIN(b.status) AS status
	FROM credit_assessment_manual_search_applications aa
	INNER JOIN t_svyrequest a ON aa.appcd=a.appcd
	INNER JOIN t_svyassign b ON a.assigncd=b.assigncd
	WHERE a.delflag=0 
	GROUP BY a.appcd
	) tsvy ON ta.appcd=tsvy.appcd
WHERE mc.delflag = 0 ';
IF surveyStatusList IS NOT NULL THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND COALESCE(tsvy.status, 0) IN (SELECT statuslist FROM credit_assessment_manual_search_status_list) '; END IF;
IF financePriceRange IS NOT NULL THEN
	IF financePriceRange = '12' THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND tpay.financeprice <= 50000 '; END IF;
	IF financePriceRange = '13' THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND tpay.financeprice BETWEEN 50001 AND 100000 '; END IF;
	IF financePriceRange = '14' THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND tpay.financeprice BETWEEN 100001 AND 500000 '; END IF;
	IF financePriceRange = '15' THEN sql_apps_under_assessment := sql_apps_under_assessment || ' AND tpay.financeprice > 500000 '; END IF;
END IF;
IF countflag <> 1 THEN
	IF displayOrder IS NOT NULL THEN
		IF displayOrder = '1' THEN sql_apps_under_assessment := sql_apps_under_assessment || '
		ORDER BY ta.applicationtype DESC,
			ta.appdate ASC,
			ta.sysappcd ASC'; END IF;
		IF displayOrder = '2' THEN sql_apps_under_assessment := sql_apps_under_assessment || '
		ORDER BY ta.applicationtype DESC,
			ta.appdate DESC,
			ta.sysappcd ASC'; END IF;
	END IF;
	sql_apps_under_assessment := sql_apps_under_assessment || ' LIMIT ' || searchlimit || ' OFFSET ' || searchoffset || ' ';
END IF;
sql_apps_under_assessment := sql_apps_under_assessment || ';';

RAISE NOTICE '%', sql_status_list || sql_assessor_authority || sql_applications || sql_customers || sql_agents || sql_apps_under_assessment;
EXECUTE sql_status_list;
EXECUTE sql_assessor_authority;
EXECUTE sql_applications;
EXECUTE sql_customers;
EXECUTE sql_agents;
RETURN QUERY EXECUTE sql_apps_under_assessment;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION public.credit_assessment_manual_search(numeric, character varying, character varying, character varying, numeric, character varying, character varying, character varying,character varying, numeric, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, numeric, character varying, character varying, character varying, character varying, character varying, numeric, numeric, numeric, character varying, character varying)
  OWNER TO postgres;
