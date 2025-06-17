select
'' as  'Customer ID',
 dm.name 'Chart ID' ,
member_id 'Member ID',
patient_last_name 'Member Last Name', 
patient_first_name 'Member First Name',
patient_gender Gender,
DATE_FORMAT(em.patient_birth_date, '%m/%d/%Y') AS DOB,
DATE_FORMAT(dd.encounter_actual_period_start, '%m/%d/%Y') AS DOS,
condition_code 'ICD Code',
condition_code_description 'Code Description',
group_concat(distinct cms_hcc_v28_group_name SEPARATOR ';') 'HCC Code (V28)' ,
group_concat(distinct cms_hcc_v28_group_description SEPARATOR ';') 'HCC Description',
group_concat(distinct rxhcc_hcc_group_name SEPARATOR ';') 'RxHCC Code',
group_concat(distinct rxhcc_hcc_group_description SEPARATOR ';') 'RxHCC Description',
group_concat(distinct case when is_indicator <>1 then de.evidence_text end  SEPARATOR ';') 'Condition Evidence',
group_concat(distinct  concat(meat_category,'-',dev.evidence_text ) SEPARATOR ';') 'Meat Evidence',	
group_concat(distinct case when is_indicator =1 then de.evidence_text end SEPARATOR ';') Indicators,
claim_status Category,pm.name 
from 
encounter_mst em 
inner join 
document_mst dm on dm.encounter_id=em.id
inner join document_dos dd on dd.document_id=dm.id
left join project_mst pm on pm.id =em.project_id
inner join document_code dc on dc.document_dos_id =dd.id
left join document_code_evidence_map dcem on dcem.document_code_id =dc.id AND dCEM.IS_ACTIVE=1
left join document_evidence de on de.id=dcem.document_evidence_id
left join document_evidence_coordinates dem on dem.document_evidence_id=de.id
left join document_mst dm1 on dm1.id=de.document_id
left join document_code_cms_hcc_v28_group_map cch on cch.document_code_id=dc.id AND CCH.IS_ACTIVE=1 
left join document_code_meat_evidence_map ccem1 on ccem1.document_code_id =dc.id 
left join document_evidence dev on dev.id=ccem1.document_evidence_id
left join document_evidence_coordinates dec0 on dec0.document_evidence_id=dev.id
left join document_code_rxhcc_hcc_group_map ccr  on ccr.document_code_id=dc.id 
left join encounter_status_map es on es.encounter_id=em.id
where  (cms_hcc_v28_group_name IS NOT NULL OR rxhcc_hcc_group_name IS NOT NULL)  and em.project_id =372
group by member_id,dd.encounter_actual_period_start,condition_code;





WITH ranked_data AS (
  SELECT *,group_concat(distinct d_dos SEPARATOR ',') D_DOS,
         ROW_NUMBER() OVER (
           PARTITION BY `Member ID`, `ICD Code`
           ORDER BY 
             CASE Category
               WHEN 'Recaptured Condition' THEN 1
               WHEN 'SUSPECTING_CARE_GAPS' THEN 2
               WHEN 'SUSPECTING' THEN 3
               ELSE 4
             END,
             STR_TO_DATE(DOS, '%m/%d/%Y') DESC
         ) AS rn
  FROM (
    SELECT
      '' AS 'Customer ID',
      dm.name AS 'Chart ID',
      member_id AS 'Member ID',
      patient_last_name AS 'Member Last Name', 
      patient_first_name AS 'Member First Name',
      patient_gender AS Gender,
      DATE_FORMAT(em.patient_birth_date, '%m/%d/%Y') AS DOB,
      DATE_FORMAT(dd.encounter_actual_period_start, '%m/%d/%Y') AS DOS,
      dd.encounter_actual_period_start as d_dos,
      condition_code AS 'ICD Code',
      condition_code_description AS 'Code Description',
      GROUP_CONCAT(DISTINCT cms_hcc_v28_group_name SEPARATOR ';') AS 'HCC Code (V28)',
      GROUP_CONCAT(DISTINCT cms_hcc_v28_group_description SEPARATOR ';') AS 'HCC Description',
      GROUP_CONCAT(DISTINCT rxhcc_hcc_group_name SEPARATOR ';') AS 'RxHCC Code',
      GROUP_CONCAT(DISTINCT rxhcc_hcc_group_description SEPARATOR ';') AS 'RxHCC Description',
      GROUP_CONCAT(DISTINCT CASE WHEN is_indicator <> 1 THEN de.evidence_text END SEPARATOR ';') AS 'Condition Evidence',
      GROUP_CONCAT(DISTINCT CONCAT(meat_category, '-', dev.evidence_text) SEPARATOR ';') AS 'Meat Evidence',	
      GROUP_CONCAT(DISTINCT CASE WHEN is_indicator = 1 THEN de.evidence_text END SEPARATOR ';') AS Indicators,
      claim_status AS Category,
      pm.name
    FROM 
      encounter_mst em 
    INNER JOIN 
      document_mst dm ON dm.encounter_id = em.id
    INNER JOIN 
      document_dos dd ON dd.document_id = dm.id
    LEFT JOIN 
      project_mst pm ON pm.id = em.project_id
    INNER JOIN 
      document_code dc ON dc.document_dos_id = dd.id
    LEFT JOIN 
      document_code_evidence_map dcem ON dcem.document_code_id = dc.id AND dcem.is_active = 1
    LEFT JOIN 
      document_evidence de ON de.id = dcem.document_evidence_id
    LEFT JOIN 
      document_evidence_coordinates dem ON dem.document_evidence_id = de.id
    LEFT JOIN 
      document_mst dm1 ON dm1.id = de.document_id
    LEFT JOIN 
      document_code_cms_hcc_v28_group_map cch ON cch.document_code_id = dc.id AND cch.is_active = 1
    LEFT JOIN 
      document_code_meat_evidence_map ccem1 ON ccem1.document_code_id = dc.id 
    LEFT JOIN 
      document_evidence dev ON dev.id = ccem1.document_evidence_id
    LEFT JOIN 
      document_evidence_coordinates dec0 ON dec0.document_evidence_id = dev.id
    LEFT JOIN 
      document_code_rxhcc_hcc_group_map ccr ON ccr.document_code_id = dc.id 
    LEFT JOIN 
      encounter_status_map es ON es.encounter_id = em.id
    WHERE  
      (cms_hcc_v28_group_name IS NOT NULL OR rxhcc_hcc_group_name IS NOT NULL)
      AND em.project_id = 115
    GROUP BY 
      member_id, dd.encounter_actual_period_start, condition_code
  ) AS raw_data
)

SELECT * from
FROM ranked_data
WHERE rn = 1 AND `Member ID` IN ('H76682494') AND DIAG ='Z93.0';

