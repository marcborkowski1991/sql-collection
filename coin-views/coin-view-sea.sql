WITH CTE_PC AS (
    SELECT
        CHANNEL_NAME AS CHANNEL
        , PLANNING_CONTEXT_CODE
        , PLANNING_CONTEXT_NAME
        , CAMP_ID
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_CAMPID_TO_KAMEL_PLANNINGCONTEXT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_PRIVATE.CAMPID_TO_KAMEL_PLANNINGCONTEXT
)
SELECT
    REPORTING_DATE
    , PLANNING_CONTEXT_CODE
    , PLANNING_CONTEXT_NAME
    , CHANNEL
    , CAMP_ID
    , SOURCE_SYSTEM
    , IMPRESSIONS
    , CLICKS
    , COST_EURO
    , CASE
        WHEN INSTR(CAMP_ID, '-TW.BC-') > 0 OR INSTR(CAMP_ID, '-PF.BC-') > 0 THEN 2
        WHEN
            INSTR(CAMP_ID, '-W.BC-') > 0
            OR INSTR(CAMP_ID, '-F.BC-') > 0
            OR (INSTR(CAMP_ID, '-SS.AG-') > 0 AND INSTR(CAMP_ID, '-D.C-') > 0)
            OR INSTR(CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(CAMP_ID, '(\d+)\.WS'), '\d+') / 100
        WHEN INSTR(CAMP_ID, '.FS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(CAMP_ID, '(\d+)\.FS'), '\d+') / 100
        ELSE NULL
    END AS BUDGET_LOGIC_SHARE    
FROM (
    --SEA Google Keyword
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'SEA')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'G.MP-A.AS-S.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'Google AdWords')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_GOOGLE_KEYWORD_PERFORMANCE_REPORT' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_GOOGLE_KEYWORD_PERFORMANCE_REPORT AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_GOOGLE_KEYWORD_PERFORMANCE_REPORT AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.DATE_COL >= '2020-01-01'
    GROUP BY
       LOCAL.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --SEA DSA Google
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'SEA')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'G.MP-DS.AS-S.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'Google DSA')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_GOOGLE_DSA_PERFORMANCE_REPORT_UNION' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_GOOGLE_DSA_PERFORMANCE_REPORT_UNION AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_GOOGLE_DSA_PERFORMANCE_REPORT_UNION AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.DATE_COL >= '2020-01-01'
    GROUP BY
        LOCAL.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --SEA Bing Keyword
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'SEA')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'B.MP-BA.AS-S.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'Bing Ads')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BING_KEYWORD_PERFORMANCE_REPORT_FICO' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BING_KEYWORD_PERFORMANCE_REPORT AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BING_KEYWORD_PERFORMANCE_REPORT_FICO AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.DATE_COL >= '2020-01-01'
    GROUP BY
        LOCAL.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --SEA Bing Keyword Altdaten (01.01.2020 bis 30.06.2020)
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'SEA')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'B.MP-BA.AS-S.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'Bing Ads')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BING_KEYWORD_PERFORMANCE_REPORT_FICO' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BING_KEYWORD_PERFORMANCE_REPORT_FICO AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BING_KEYWORD_PERFORMANCE_REPORT_FICO AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.DATE_COL BETWEEN '2020-01-01' AND '2020-06-30'
    GROUP BY
        LOCAL.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --SEA Bing DSA
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'SEA')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'B.MP-BD.AS-S.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'Bing DSA')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BING_DSA_PERFORMANCE_REPORT' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BING_DSA_PERFORMANCE_REPORT AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BING_DSA_PERFORMANCE_REPORT AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.DATE_COL >= '2020-01-01'
    GROUP BY
        LOCAL.REPORTING_DATE, DAT.CAMP_ID
)
WHERE INSTR(CAMP_ID, 'B.AC') = 0
;
