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
    --Social Ads
    --Facebook
    SELECT
        DAT.REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'Social Ads')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_CODE
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        AND INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'RETARGETING') > 0
                        THEN 'F.MP-DP.AS-APP.BT-OA.T-SA.C'
                    WHEN
                        INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        AND INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'RETARGETING') = 0
                        THEN 'F.MP-FS.AS-APP.BT-OA.T-SA.C'
                    WHEN INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'RETARGETING') > 0 THEN 'F.MP-DP.AS-SA.C'
                    WHEN INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'WEB PROSPECTING') > 0 THEN 'F.MP-FS.AS-SA.C'
                END
            )
        ) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_NAME
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        AND INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'RETARGETING') > 0
                        THEN 'Facebook App Install Dynamic Ads'
                    WHEN
                        INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        AND INSTR(DAT.FACEBOOK_CAMPAIGN_NAME, 'RETARGETING') = 0
                        THEN 'Facebook App Install Static Ads'
                    WHEN INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'RETARGETING') > 0 THEN 'Facebook Dynamic Ads'
                    WHEN INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'WEB PROSPECTING') > 0 THEN 'Facebook Static Ads'
                END
            )
        ) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_FACEBOOK_AD_PERFORMANCE' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_MPC_FACEBOOK_AD_PERFORMANCE_CAMPID AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_PRIVATE.MPC_FACEBOOK_AD_PERFORMANCE_CAMPID AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        DAT.REPORTING_DATE >= '2020-01-01'
        AND (INSTR(DAT.CAMP_ID, '-SA.C-') > 0 OR INSTR(UPPER(DAT.FACEBOOK_CAMPAIGN_NAME), 'PERFORMANCE') > 0)
        AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.FACEBOOK_ACCOUNT_ID NOT IN (
            10156300243778700
            , 79042523
            , 10154137716208700
            , 10154135741873700
            , 10154137716063700
            , 10154135739178700
            , 10154715337948700
            , 10154354445848700
        )
    GROUP BY
        DAT.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --Pinterest
    SELECT
        DAT.REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'Social Ads')) AS CHANNEL
        , DAT.CAMP_ID AS CAMP_ID
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_CODE
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'TRAFFIC') > 0
                        THEN 'P.MP-PS.AS-SA.C'
                    WHEN
                        INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'CONVERSION') > 0
                        THEN 'P.MP-PF.AS-SA.C'
                END
            )
        ) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_NAME
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'TRAFFIC') > 0
                        THEN 'Pinterest Static'
                    WHEN
                        INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'CONVERSION') > 0
                        THEN 'Pinterest Product Feed'
                END
            )
        ) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS_PAID) AS IMPRESSIONS
        , SUM(DAT.CLICKS_PAID) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        DAT.REPORTING_DATE >= '2020-01-01'
        AND (COALESCE(DAT.IMPRESSIONS_PAID, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS_PAID, 0)) > 0
        AND (INSTR(DAT.CAMP_ID, '-SA.C-') > 0 OR INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') > 0)
    GROUP BY
        DAT.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --Snapchat
    SELECT
        DAT.REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'Social Ads')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_CODE
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'DYNAMISCH') > 0
                        THEN 'SN.MP-D.AS-SA.C'
                    WHEN
                        INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        THEN 'SN.MP-S.AS-APP.BT-SA.C'
                END
            )
        ) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_NAME
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'DYNAMISCH') > 0
                        THEN 'Snapchat Dynamic Ads'
                    WHEN
                        INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'PERFORMANCE') > 0
                        AND INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        THEN 'Snapchat Static App Install'
                END
            )
        ) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_SNAP_AD_PERFORMANCE' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , 0 AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_SNAP_AD_PERFORMANCE AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_SNAP_AD_PERFORMANCE AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        DAT.REPORTING_DATE >= '2020-01-01'
        AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0)) > 0
        AND (INSTR(DAT.CAMP_ID, '-SA.C-') > 0 OR INSTR(UPPER(DAT.SNAP_CAMPAIGN_NAME), 'PERFORMANCE') > 0)
    GROUP BY
        DAT.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --YouTube
    SELECT
        DAT.DATE_COL AS REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'Social Ads')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_CODE, 'Y.MP-GV.AS-SA.C')) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(COALESCE(CTE_PC.PLANNING_CONTEXT_NAME, 'YouTube - Google Video')) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_GOOGLE_AD_PERFORMANCE_REPORT' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_GOOGLE_AD_PERFORMANCE_REPORT AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_GOOGLE_AD_PERFORMANCE_REPORT AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        DAT.DATE_COL >= '2020-01-01'
        AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND DAT.GOOGLE_ACCOUNT_ID IN (1878078603, 5703347048)
        AND (
            INSTR(DAT.CAMP_ID, '-SA.C-') > 0
            OR INSTR(UPPER(DAT.GOOGLE_CAMPAIGN_NAME), 'PERFORMANCE') > 0
            OR INSTR(UPPER(DAT.GOOGLE_CAMPAIGN_NAME), 'WKZ') > 0
        )
    GROUP BY
        LOCAL.REPORTING_DATE, DAT.CAMP_ID
    ------------------
    UNION ALL
    ------------------
    --TikTok
    SELECT
        DAT.REPORTING_DATE
        , FIRST_VALUE(COALESCE(CTE_PC.CHANNEL, 'Social Ads')) AS CHANNEL
        , DAT.CAMP_ID
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_CODE
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.TIKTOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        OR DAT.TIKTOK_ACCOUNT_ID = 7018146072233000962
                        THEN 'TI.MP-S.AS-APP.BT-SA.C'
                    WHEN DAT.TIKTOK_ACCOUNT_ID = 7078229405281124353 THEN 'TI.MP-S.AS-SA.C'
                    WHEN DAT.TIKTOK_ACCOUNT_ID = 7067878364589064193 THEN 'TI.MP-D.AS-SA.C' -- neues Konto nur für Performance
                END
            )
        ) AS PLANNING_CONTEXT_CODE
        , FIRST_VALUE(
            COALESCE(
                CTE_PC.PLANNING_CONTEXT_NAME
                , CASE
                    WHEN
                        INSTR(UPPER(DAT.TIKTOK_CAMPAIGN_NAME), 'APP INSTALL') > 0
                        OR DAT.TIKTOK_ACCOUNT_ID = 7018146072233000962
                        THEN 'TikTok Static App Install'
                    WHEN DAT.TIKTOK_ACCOUNT_ID = 7078229405281124353 THEN 'TikTok Static Ads'
                    WHEN DAT.TIKTOK_ACCOUNT_ID = 7067878364589064193 THEN 'TikTok Dynamic Ads' -- neues Konto nur für Performance
                END
            )
        ) AS PLANNING_CONTEXT_NAME
        , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_TIKTOK_AD_PERFORMANCE' AS SOURCE_SYSTEM
        , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
        , SUM(DAT.CLICKS) AS CLICKS
        , SUM(DAT.COST_EURO) AS COST_EURO
    FROM
        PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_TIKTOK_AD_PERFORMANCE AS DAT
        --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_TIKTOK_AD_PERFORMANCE AS DAT
    LEFT JOIN
        CTE_PC
        ON DAT.CAMP_ID = CTE_PC.CAMP_ID
    WHERE
        DAT.REPORTING_DATE >= '2020-01-01'
        AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) > 0
        AND (INSTR(DAT.CAMP_ID, '-SA.C-') > 0 OR INSTR(UPPER(DAT.TIKTOK_CAMPAIGN_NAME), 'PERFORMANCE') > 0)
    GROUP BY
        DAT.REPORTING_DATE, DAT.CAMP_ID
)
WHERE
    INSTR(CAMP_ID, 'B.AC-') = 0 OR CAMP_ID IS NULL
;
