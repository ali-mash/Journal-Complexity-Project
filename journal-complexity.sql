
SELECT * FROM (
    SELECT
        sc.datasalon_code,
        jm.journal,
        jm.title,
        sc.fiscal_year,
        sc.submission_month,  
        sc.submission_count,
        rr.reject_ratio, 
       // rr.total_rejected_without_review,
        COALESCE(ar.avg_revisions, 0) AS avg_revisions,
        COALESCE(ar.avg_revisions1, 0) AS avg_revisions_exact,
        COALESCE(fd.median_days_to_final_decision, 0) AS median_days_to_final_decision,
        COALESCE(fd.median_days_to_accept, 0) AS median_days_to_accept,
        COALESCE(ad.median_days_to_export, 0) AS median_days_to_export,
        COALESCE(mtc.no_of_manuscript_types, 0) AS no_of_manuscript_types,
        jm.subject_category as first_level_wol_subject,
        jm.subject_area as second_level_wol_subject,
        jm.ownership_status,
        jm.anonymization_policy,
        jm.revenue_model,
        jm.PRPL,
        jm.PRPM,
        jm.editorial_office,
        jm.editorial_in_chief,
        jm.current_portfolio,
        jm.free_format,
        sc.art_type,
        sc.special_issue_count,
        sc.withdrawn_count,
       rr.retraction_count,
       rr.concern_count,
        0 AS ex_hindawi
    FROM (
        SELECT 
            datasalon_code, 
            fiscal_year,
            MAX(ath_article_type) as art_type,
            COUNT(CASE WHEN special_issue_manuscript = 'YES' THEN 1 ELSE NULL END) AS special_issue_count,
            COUNT(CASE WHEN withdrawndate IS NOT NULL THEN 1 END) AS withdrawn_count,
            MIN(DATE_TRUNC('MONTH', submissiondateoriginal)) AS submission_month,
            COUNT(*) AS submission_count
        FROM (
            SELECT 
                datasalon_code,
                datasalon_product_title,
                submissiondateoriginal,
                ath_article_type,
                withdrawndate,
                special_issue_manuscript,

                
                CASE 
                    WHEN EXTRACT(MONTH FROM submissiondateoriginal) >= 5 
                    THEN TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) + 1 || '-01-05', 'YYYY-MM-DD')
                    ELSE TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) || '-01-05', 'YYYY-MM-DD')
                END AS fiscal_year
            FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
            WHERE submissiondateoriginal >= DATE '2021-05-01'
        ) base_data
        GROUP BY datasalon_code, fiscal_year
    ) sc
    LEFT JOIN (



  SELECT 
            datasalon_code, 
            fiscal_year,
            
            ROUND(
                COUNT(CASE WHEN rejected_paper = 1 AND peer_reviewed IS NULL THEN 1 END) * 1.0 
                / NULLIF(COUNT(*), 0), 
            2
            ) 
            
            AS reject_ratio,
            //COUNT(CASE WHEN rejected_paper = 1 AND peer_reviewed IS NULL THEN 1 END) AS total_rejected_without_review

            
SUM(CASE WHEN ath_article_type = 'Retraction' THEN 1 ELSE 0 END) AS retraction_count,
SUM(CASE WHEN ath_article_type = 'Concern' THEN 1 ELSE 0 END) AS concern_count,


    
        FROM (
            SELECT 
                datasalon_code,
                submissiondateoriginal,
                CASE 
                    WHEN EXTRACT(MONTH FROM submissiondateoriginal) >= 5 
                    THEN TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) + 1 || '-01-05', 'YYYY-MM-DD')
                    ELSE TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) || '-01-05', 'YYYY-MM-DD')
                END AS fiscal_year
            FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
            WHERE submissiondateoriginal >= DATE '2021-05-01'
        ) base_data
        LEFT JOIN PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT USING (datasalon_code, submissiondateoriginal)
        GROUP BY datasalon_code, fiscal_year
    ) rr
        ON sc.datasalon_code = rr.datasalon_code 
        AND sc.fiscal_year = rr.fiscal_year
    LEFT JOIN (
        SELECT 
            datasalon_code, 
            fiscal_year,
            CEIL(AVG(revisionnumber)) AS avg_revisions,
            ROUND(AVG(revisionnumber), 1) AS avg_revisions1

  
        FROM (
            SELECT 
                datasalon_code,
                submissiondateoriginal,
                CASE 
                    WHEN EXTRACT(MONTH FROM submissiondateoriginal) >= 5 
                    THEN TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) + 1 || '-01-05', 'YYYY-MM-DD')
                    ELSE TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) || '-01-05', 'YYYY-MM-DD')
                END AS fiscal_year
            FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
            WHERE submissiondateoriginal >= DATE '2021-05-01'
        ) base_data
        LEFT JOIN PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT USING (datasalon_code, submissiondateoriginal)
        GROUP BY datasalon_code, fiscal_year
    ) ar
        ON sc.datasalon_code = ar.datasalon_code 
        AND sc.fiscal_year = ar.fiscal_year
    LEFT JOIN (
        SELECT 
            datasalon_code,
            fiscal_year,

ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY 
        CASE 
            WHEN FIRST_DECISION IN ('ACCEPT', 'REJECT', 'REFERRAL') 
                AND submissiondateoriginal IS NOT NULL
                AND FIRST_DECISION_DATE IS NOT NULL
                AND peer_reviewed = 1
                AND submissiondateoriginal <= FIRST_DECISION_DATE
            THEN DATEDIFF(day, submissiondateoriginal, FIRST_DECISION_DATE) * 86400

            WHEN FIRST_DECISION NOT IN ('ACCEPT', 'REJECT', 'REFERRAL') 
                AND decisiontype IN ('ACCEPT', 'REJECT', 'REFERRAL')
                AND submissiondateoriginal IS NOT NULL
                AND decisiondate IS NOT NULL
                AND peer_reviewed = 1
                AND submissiondateoriginal <= decisiondate
            THEN DATEDIFF(day, submissiondateoriginal, decisiondate) * 86400
            ELSE NULL
        END
    ) / 86400, 
    0 -- rounds to nearest whole number
) AS median_days_to_final_decision


,

ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY 
        CASE 
            WHEN FIRST_DECISION IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK') 
                AND submissiondateoriginal IS NOT NULL
                AND FIRST_DECISION_DATE IS NOT NULL
                AND peer_reviewed = 1
                AND submissiondateoriginal <= FIRST_DECISION_DATE
            THEN DATEDIFF(day, submissiondateoriginal, FIRST_DECISION_DATE) * 86400
            
            WHEN FIRST_DECISION NOT IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK') 
                AND FIRST_DECISION IS NOT NULL
                AND decisiontype IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK')
                AND submissiondateoriginal IS NOT NULL
                AND decisiondate IS NOT NULL
                AND peer_reviewed = 1
                AND submissiondateoriginal <= decisiondate
            THEN DATEDIFF(day, submissiondateoriginal, decisiondate) * 86400

            ELSE NULL
        END
    ) / 86400, 
    0 -- rounds to nearest whole number
) AS median_days_to_accept



        FROM (
            SELECT 
                datasalon_code,
                submissiondateoriginal,
                CASE 
                    WHEN EXTRACT(MONTH FROM submissiondateoriginal) >= 5 
                    THEN TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) + 1 || '-01-05', 'YYYY-MM-DD')
                    ELSE TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) || '-01-05', 'YYYY-MM-DD')
                END AS fiscal_year
            FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
            WHERE submissiondateoriginal >= DATE '2021-05-01'
        ) base_data
        LEFT JOIN PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT USING (datasalon_code, submissiondateoriginal)
        GROUP BY datasalon_code, fiscal_year
    ) fd
        ON sc.datasalon_code = fd.datasalon_code 
        AND sc.fiscal_year = fd.fiscal_year
    LEFT JOIN (
        SELECT 
            bd.datasalon_code,
            bd.fiscal_year,

    ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY 
        CASE 
            WHEN first_decision IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK')
                AND first_decision_date IS NOT NULL 
                AND art_bridge.art_wiley_rec_dt IS NOT NULL
                AND art_bridge.art_wiley_rec_dt >= first_decision_date
            THEN DATEDIFF(day, first_decision_date, art_bridge.art_wiley_rec_dt) * 86400

            WHEN (first_decision NOT IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK') OR first_decision IS NULL)
                AND decisiontype IN ('ACCEPT', 'ACCEPT_FOR_FIRST_LOOK')
                AND decisiondate IS NOT NULL
                AND art_bridge.art_wiley_rec_dt IS NOT NULL
                AND art_bridge.art_wiley_rec_dt >= decisiondate
            THEN DATEDIFF(day, decisiondate, art_bridge.art_wiley_rec_dt) * 86400

            ELSE NULL
        END
    ) / 86400, 
    0 -- rounds to nearest whole number
) AS median_days_to_export




            
        FROM (
            SELECT 
                datasalon_code,
                submissiondateoriginal,
                CASE 
                    WHEN EXTRACT(MONTH FROM submissiondateoriginal) >= 5 
                    THEN TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) + 1 || '-01-05', 'YYYY-MM-DD')
                    ELSE TO_DATE(EXTRACT(YEAR FROM submissiondateoriginal) || '-01-05', 'YYYY-MM-DD')
                END AS fiscal_year
            FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
            WHERE submissiondateoriginal >= DATE '2021-05-01'
        ) bd
        LEFT JOIN PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT sub_one_rpm
            ON bd.datasalon_code = sub_one_rpm.datasalon_code AND bd.submissiondateoriginal = sub_one_rpm.submissiondateoriginal
        LEFT JOIN PROD_EDW.RESEARCH.ARTICLE_BRIDGE_FULL art_bridge
            ON UPPER(art_bridge.MPA_UNIVERSAL_ID) = UPPER(sub_one_rpm.PLATFORM_SITE_MANUSCRIPT_ID)
        GROUP BY bd.datasalon_code, bd.fiscal_year
    ) ad
        ON sc.datasalon_code = ad.datasalon_code 
        AND sc.fiscal_year = ad.fiscal_year
    LEFT JOIN (
        SELECT 
            product_mdata.datasalon_code,
            ANY_VALUE(product_mdata.datasalon_product_title) as title,
            ANY_VALUE(product_mdata.step_code) AS journal,
            ANY_VALUE(step.OWNERSHIPSTATUS) AS ownership_status,
            ANY_VALUE(step.EDITORIAL_PEERREVIEWMODEL) AS anonymization_policy,
            ANY_VALUE(product_mdata.revenue_model) AS revenue_model,
            ANY_VALUE(CONTACTS_PEERREVIEWPERFORMANCELEAD) AS PRPL,
            ANY_VALUE(CONTACTS_PEERREVIEWPERFORMANCEMANAGER) AS PRPM,
            ANY_VALUE(step.EDITORIAL_EDITORIALOFFICEMODEL) as editorial_office,
            ANY_VALUE(step.EDITORIAL_EDITORINCHIEFMODEL) as editorial_in_chief,
            MAX(CASE WHEN step.CURRENTPORTFOLIO = 'TRUE' THEN 1 ELSE 0 END) AS current_portfolio,
            MAX(CASE WHEN step.EDITORIAL_FREEFORMAT = 'TRUE' THEN 1 ELSE 0 END) AS free_format,
            ANY_VALUE(product_mdata.first_level_wol_subject) as subject_category,
            ANY_VALUE(product_mdata.second_level_wol_subject) as subject_area,
        FROM PROD_EDW.RESEARCH_ANALYTICS.DIM_T_PRODUCT_METADATA product_mdata
        LEFT JOIN PROD_EDW.STEP.T_D_STEP_JOURNAL step
            ON product_mdata.step_code = step.journalgroupcode      
        GROUP BY product_mdata.datasalon_code
    ) jm
        ON sc.datasalon_code = jm.datasalon_code
    LEFT JOIN (
        SELECT 
            datasalon_code,
            COUNT(DISTINCT ath_article_type) AS no_of_manuscript_types,
        FROM PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
        WHERE submissiondateoriginal >= DATE '2021-05-01'
        GROUP BY datasalon_code
    ) mtc
        ON sc.datasalon_code = mtc.datasalon_code
    WHERE sc.datasalon_code NOT LIKE '97%'
    AND sc.datasalon_code NOT LIKE 'S1%' 
   
)  
     WHERE JOURNAL NOT IN (
      SELECT DISTINCT JOURNAL_CODE
        FROM PROD_EDW.RESEARCH_ANALYTICS.DIM_T_PRODUCT_METADATA
        WHERE BMIS_CODE LIKE 'HINDAWI%'
        AND current_publisher = 'WILEY'
    )  
    
ORDER BY submission_count DESC


select * from PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT where datasalon_code = 'EAS2'
