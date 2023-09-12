SELECT
    COALESCE(TV.BUDGET_LOGIC, P.BUDGET_LOGIC) AS BUDGET_LOGIC
    ,CASE
        WHEN COALESCE(TV.BUDGET_LOGIC, P.BUDGET_LOGIC) = 1 THEN 1
        WHEN COALESCE(TV.BUDGET_LOGIC, P.BUDGET_LOGIC) = 3 THEN 0
        ELSE NULL
    END AS BUDGET_LOGIC_SHARE
    ,COALESCE(TV.CAMPAIGN_TYPE, P.CAMPAIGN_TYPE) AS CAMPAIGN_TYPE
    ,COALESCE(TV.IS_PLAN, P.IS_PLAN) AS IS_PLAN
    ,'' AS CAMP_ID
    ,'TV' AS CHANNEL
    ,0 AS CLICKS
    ,SUM(CASE
        WHEN (P.PLANNING_DATE<= TV.MAX_BROADCAST_DATE OR P.PLANNING_DATE IS NULL)
            THEN (TV.COST_EURO_NNN)
        ELSE (P.COST_EURO_NNN_PLANNED)
    END )AS MEDIA_COST
    ,SUM(COALESCE(TV.COST_EURO_NNN,P.COST_EURO_NNN_PLANNED) * BBA.FEE) AS INFRASTRUCTURE_COST
    ,LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    ,SUM(CASE
        WHEN (P.PLANNING_DATE<= TV.MAX_BROADCAST_DATE OR P.PLANNING_DATE IS NULL)
            THEN (TV.VIEWERS_MILLIONS_E18P)
        ELSE (P.GROSS_CONTACTS_PLANNED)
    END ) AS IMPRESSIONS
    , COALESCE(TV.BROADCAST_DATE,P.PLANNING_DATE) AS REPORTING_DATE
    ,'${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BPN_TV_FLIGHTS' AS SOURCE_SYSTEM
FROM (
    SELECT ADD_DAYS(DATE '2020-01-01', LEVEL - 1) AS DATUM
    FROM
        DUAL
    CONNECT BY LEVEL <= DAYS_BETWEEN(CURRENT_DATE, '2020-01-01') + 1
) AS D
LEFT JOIN
    (
        SELECT
            BROADCAST_DATE
            , UPPER(TV_FLIGHT_NAME) AS CAMPAIGN_NAME
            , CASE
                WHEN INSTR(LOCAL.CAMPAIGN_NAME, '_WKZ_') > 0 THEN 3
                ELSE 1
            END AS BUDGET_LOGIC
            , CASE
                WHEN INSTR(LOCAL.CAMPAIGN_NAME, '_VERTRIEB_') THEN 'VERTRIEB'
                ELSE 'BRANDING'
            END AS CAMPAIGN_TYPE
            , FALSE AS IS_PLAN -- nur TV und Rundfunk Webradio
            , SUM(COST_EURO_NNN)  AS COST_EURO_NNN
            , MAX(BROADCAST_DATE) OVER (PARTITION BY LOCAL.CAMPAIGN_NAME)  AS MAX_BROADCAST_DATE
            , SUM(VIEWERS_MILLIONS_E18P *1000000) AS VIEWERS_MILLIONS_E18P
        FROM
	      PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_TV_FLIGHTS
           -- ${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_TV_FLIGHTS

        GROUP BY
            BROADCAST_DATE, LOCAL.CAMPAIGN_NAME
    )  AS TV
    ON TV.BROADCAST_DATE=D.DATUM
FULL OUTER JOIN
    (
        SELECT
            PLANNING_DATE
            ,UPPER(BPN_CAMPAIGN_NAME) AS CAMPAIGN_NAME
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'WKZ' THEN 3 ELSE 1  END AS BUDGET_LOGIC
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'VERTRIEB' THEN 'VERTRIEB'  ELSE 'BRANDING'  END AS CAMPAIGN_TYPE
            ,CHANNEL
            ,TRUE AS IS_PLAN
            ,SUM(COST_EURO_NNN_PLANNED) AS COST_EURO_NNN_PLANNED
            ,SUM(GROSS_CONTACTS_PLANNED) AS GROSS_CONTACTS_PLANNED
        FROM
	       PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA	
           -- ${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA

        WHERE UPPER(CHANNEL) = 'TV'
        GROUP BY
            PLANNING_DATE
            , LOCAL.CAMPAIGN_NAME
            , LOCAL.BUDGET_LOGIC
            , LOCAL.CAMPAIGN_TYPE
            , CHANNEL
            , LOCAL.IS_PLAN
    ) AS P
    ON D.DATUM=P.PLANNING_DATE
    AND TV.CAMPAIGN_NAME =P.CAMPAIGN_NAME
LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R BBA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R AS BBA
    -- durch ${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_STAGE.BOARD_BRANDING_AGENTURFEE ersetzen

    ON CAST(BBA.YEAR_MONTH AS DECIMAL(6)) = CAST(TO_CHAR(COALESCE(TV.BROADCAST_DATE,P.PLANNING_DATE),'yyyymm') AS DECIMAL(6))
    AND BBA.CHANNEL_BRANDING = 'TV'
WHERE (TV.BROADCAST_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1 ) OR (P.PLANNING_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1)
GROUP BY
    LOCAL.BUDGET_LOGIC
    ,LOCAL.BUDGET_LOGIC_SHARE
    ,LOCAL.CAMPAIGN_TYPE
    ,LOCAL.IS_PLAN
    ,LOCAL.CAMP_ID
    ,LOCAL.CHANNEL
    ,LOCAL.REPORTING_DATE
UNION ALL
-- FUNK / HÖRFUNK
SELECT
    COALESCE(RCM.BUDGET_LOGIC, P.BUDGET_LOGIC) AS BUDGET_LOGIC
    ,CASE
        WHEN COALESCE(RCM.BUDGET_LOGIC, P.BUDGET_LOGIC) = 1 THEN 1
        WHEN COALESCE(RCM.BUDGET_LOGIC, P.BUDGET_LOGIC) = 3 THEN 0
        ELSE NULL
    END AS BUDGET_LOGIC_SHARE
    ,COALESCE(RCM.CAMPAIGN_TYPE, P.CAMPAIGN_TYPE) AS CAMPAIGN_TYPE
    ,COALESCE(RCM.IS_PLAN, P.IS_PLAN) AS IS_PLAN
    ,'' AS CAMP_ID
    ,'Audio' AS CHANNEL
    ,0 AS CLICKS
    ,SUM(CASE
        WHEN (P.PLANNING_DATE<=RCM.MAX_REPORTING_START_DATE OR P.PLANNING_DATE IS NULL)
            THEN (RCM.COST_EURO_NNN)
        ELSE (P.COST_EURO_NNN_PLANNED)
    END )AS MEDIA_COST
    ,SUM(COALESCE(RCM.COST_EURO_NNN,P.COST_EURO_NNN_PLANNED) * BBA.FEE) AS INFRASTRUCTURE_COST
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    ,SUM(
        CASE
            WHEN (  P.PLANNING_DATE<=RCM.MAX_REPORTING_START_DATE OR  P.PLANNING_DATE IS NULL)
                THEN (RCM.CONTACTS)
            ELSE (P.GROSS_CONTACTS_PLANNED)
        END
    ) AS IMPRESSIONS
    , COALESCE(RCM.REPORTING_START_DATE,P.PLANNING_DATE) AS REPORTING_DATE
    ,'${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BPN_RADIO_CAMPAIGN_MARKETER' AS SOURCE_SYSTEM
FROM
    (
        SELECT ADD_DAYS(DATE '2020-01-01', LEVEL - 1) AS DATUM
        FROM
            DUAL
        CONNECT BY LEVEL <= DAYS_BETWEEN(CURRENT_DATE, '2020-01-01') + 1
    ) AS D
LEFT JOIN
    (
        SELECT
            REPORTING_START_DATE
            ,UPPER(BPN_RADIO_CAMPAIGN_NAME) AS CAMPAIGN_NAME
            ,CASE WHEN INSTR(BPN_RADIO_CAMPAIGN_NAME, '_WKZ_') > 0 THEN 3 ELSE 1  END AS BUDGET_LOGIC
            ,CASE WHEN INSTR(BPN_RADIO_CAMPAIGN_NAME, '_VERTRIEB_') >0  THEN 'VERTRIEB' ELSE 'BRANDING'  END AS CAMPAIGN_TYPE
            , FALSE AS IS_PLAN
            ,SUM(COST_EURO_NNN)  AS COST_EURO_NNN
            ,SUM(CONTACTS) AS CONTACTS
            ,MAX(REPORTING_START_DATE) OVER (PARTITION BY BPN_RADIO_CAMPAIGN_NAME)  AS MAX_REPORTING_START_DATE
        FROM
            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_RADIO_CAMPAIGN_MARKETER
            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_RADIO_CAMPAIGN_MARKETER

        WHERE REPORTING_START_DATE = REPORTING_END_DATE
        GROUP BY
            REPORTING_START_DATE
            , LOCAL.CAMPAIGN_NAME
            , LOCAL.BUDGET_LOGIC
            , LOCAL.CAMPAIGN_TYPE
            , BPN_RADIO_CAMPAIGN_NAME
    ) AS RCM
    ON RCM.REPORTING_START_DATE=D.DATUM
FULL OUTER JOIN
    (
        SELECT
            PLANNING_DATE
            ,UPPER(BPN_CAMPAIGN_NAME) AS CAMPAIGN_NAME
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'WKZ' THEN 3 ELSE 1  END AS BUDGET_LOGIC
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'VERTRIEB' THEN 'VERTRIEB'  ELSE 'BRANDING'  END AS CAMPAIGN_TYPE
            ,CHANNEL
            ,TRUE AS IS_PLAN
            ,SUM(COST_EURO_NNN_PLANNED) AS COST_EURO_NNN_PLANNED
            ,SUM(GROSS_CONTACTS_PLANNED) AS GROSS_CONTACTS_PLANNED
        FROM
            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA
            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA

        WHERE UPPER(CHANNEL) = 'HÖRFUNK'
        GROUP BY
            PLANNING_DATE
            , LOCAL.CAMPAIGN_NAME
            , LOCAL.BUDGET_LOGIC
            , LOCAL.CAMPAIGN_TYPE
            , CHANNEL
    ) AS P
    ON D.DATUM=P.PLANNING_DATE
    AND RCM.CAMPAIGN_NAME = P.CAMPAIGN_NAME
LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R BBA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R AS BBA

    ON CAST(BBA.YEAR_MONTH AS DECIMAL(6)) = CAST(TO_CHAR(COALESCE(RCM.REPORTING_START_DATE,P.PLANNING_DATE),'yyyymm') AS DECIMAL(6))
    AND UPPER(BBA.CHANNEL_BRANDING) = 'FUNK'
WHERE  (RCM.REPORTING_START_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1 ) OR (P.PLANNING_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1)
GROUP BY
    LOCAL.BUDGET_LOGIC
    ,LOCAL.BUDGET_LOGIC_SHARE
    ,LOCAL.CAMPAIGN_TYPE
    ,LOCAL.IS_PLAN
    ,LOCAL.CAMP_ID
    ,LOCAL.CHANNEL
    ,LOCAL.REPORTING_DATE
UNION ALL
-- FUNK / WEBRADIO
SELECT
    COALESCE(WR.BUDGET_LOGIC, P.BUDGET_LOGIC) AS BUDGET_LOGIC
    ,CASE
        WHEN COALESCE(WR.BUDGET_LOGIC, P.BUDGET_LOGIC) = 1 THEN 1
        WHEN COALESCE(WR.BUDGET_LOGIC, P.BUDGET_LOGIC) = 3 THEN 0
        ELSE NULL
    END AS BUDGET_LOGIC_SHARE
    ,COALESCE(WR.CAMPAIGN_TYPE, P.CAMPAIGN_TYPE) AS CAMPAIGN_TYPE
    ,COALESCE(WR.IS_PLAN, P.IS_PLAN) AS IS_PLAN
    ,'' AS CAMP_ID
    ,'Audio' AS CHANNEL
    ,0 AS CLICKS
    ,SUM(CASE
        WHEN (P.PLANNING_DATE<=WR.MAX_REPORTING_DATE OR P.PLANNING_DATE IS NULL)
            THEN (WR.COST_EURO_NNN)
        ELSE (P.COST_EURO_NNN_PLANNED)
    END )AS MEDIA_COST
    ,SUM(COALESCE(WR.COST_EURO_NNN,P.COST_EURO_NNN_PLANNED) * BBA.FEE) AS INFRASTRUCTURE_COST
    ,LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    ,SUM(CASE
        WHEN (  P.PLANNING_DATE<=WR.MAX_REPORTING_DATE OR  P.PLANNING_DATE IS NULL)
            THEN (WR.IMPRESSIONS)
        ELSE (P.GROSS_CONTACTS_PLANNED)
    END ) AS IMPRESSIONS
    , COALESCE(WR.REPORTING_DATE,P.PLANNING_DATE) AS REPORTING_DATE
    ,'${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BPN_RADIO_CAMPAIGN_WEBRADIO' AS SOURCE_SYSTEM
FROM
    (
        SELECT ADD_DAYS(DATE '2020-01-01', LEVEL - 1) AS DATUM
        FROM
            DUAL
        CONNECT BY LEVEL <= DAYS_BETWEEN(CURRENT_DATE, '2020-01-01') + 1
    ) AS D
LEFT JOIN
    (
        SELECT
            REPORTING_DATE
            ,UPPER(BPN_RADIO_CAMPAIGN_NAME) AS CAMPAIGN_NAME
            ,CASE WHEN INSTR(BPN_RADIO_CAMPAIGN_NAME, '_WKZ_') > 0 THEN 3 ELSE 1  END AS BUDGET_LOGIC
            ,CASE WHEN INSTR(BPN_RADIO_CAMPAIGN_NAME, '_VERTRIEB_') >0  THEN 'VERTRIEB' ELSE 'BRANDING'  END AS CAMPAIGN_TYPE
            , FALSE AS IS_PLAN
            ,SUM(COST_EURO_NNN)  AS COST_EURO_NNN
            ,SUM(IMPRESSIONS) AS IMPRESSIONS
            ,MAX(REPORTING_DATE) OVER (PARTITION BY BPN_RADIO_CAMPAIGN_NAME)  AS MAX_REPORTING_DATE
        FROM
            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_RADIO_CAMPAIGN_WEBRADIO
            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_RADIO_CAMPAIGN_WEBRADIO

        GROUP BY
            REPORTING_DATE
            , LOCAL.CAMPAIGN_NAME
            , LOCAL.BUDGET_LOGIC
            , LOCAL.CAMPAIGN_TYPE
            , BPN_RADIO_CAMPAIGN_NAME
    ) AS WR
    ON WR.REPORTING_DATE=D.DATUM
FULL OUTER JOIN
    (
        SELECT
            PLANNING_DATE
            ,UPPER(BPN_CAMPAIGN_NAME) AS CAMPAIGN_NAME
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'WKZ' THEN 3 ELSE 1  END AS BUDGET_LOGIC
            ,CASE WHEN UPPER(BUDGET_ORIGIN) = 'VERTRIEB' THEN 'VERTRIEB'  ELSE 'BRANDING'  END AS CAMPAIGN_TYPE
            ,CHANNEL
            ,TRUE AS IS_PLAN
            ,SUM(COST_EURO_NNN_PLANNED) AS COST_EURO_NNN_PLANNED
            ,SUM(GROSS_CONTACTS_PLANNED) AS GROSS_CONTACTS_PLANNED
        FROM
            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA
            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_PLANNING_DATA

        WHERE
            UPPER(CHANNEL) = 'WEBRADIO'
        GROUP BY
            PLANNING_DATE, LOCAL.CAMPAIGN_NAME, LOCAL.BUDGET_LOGIC, LOCAL.CAMPAIGN_TYPE,CHANNEL
    ) AS P
    ON D.DATUM=P.PLANNING_DATE
    AND WR.CAMPAIGN_NAME=P.CAMPAIGN_NAME
LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R BBA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R AS BBA

    ON CAST(BBA.YEAR_MONTH AS DECIMAL(6)) = CAST(TO_CHAR(COALESCE(WR.REPORTING_DATE,P.PLANNING_DATE),'yyyymm') AS DECIMAL(6))
    AND UPPER(BBA.CHANNEL_BRANDING) = 'FUNK'
WHERE  (WR.REPORTING_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1 ) OR (P.PLANNING_DATE BETWEEN '2021-01-01' AND CURRENT_DATE -1)
GROUP BY
    LOCAL.BUDGET_LOGIC
    ,LOCAL.BUDGET_LOGIC_SHARE
    ,LOCAL.CAMPAIGN_TYPE
    ,LOCAL.IS_PLAN
    ,LOCAL.CAMP_ID
    ,LOCAL.CHANNEL
    ,LOCAL.REPORTING_DATE
UNION ALL
-- DIGITAL
SELECT
    CASE
        WHEN CAMPAIGN_SCOPE = 'WKZ' THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
    END AS BUDGET_LOGIC_SHARE
    , CASE
        WHEN CAMPAIGN_SCOPE = 'Vertrieb' THEN 'VERTRIEB'
        ELSE 'BRANDING'
    END AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , '' AS CAMP_ID
    , CHANNEL AS CHANNEL
    , SUM(CLICKS) AS CLICKS
    , SUM(MEDIA_COST) AS MEDIA_COST
    , SUM(INFRASTRUCTURE_COST) AS INFRASTRUCTURE_COST
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(IMPRESSIONS) AS IMPRESSIONS
    , REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_BPN_DIGITALMEDIA_MEDIAPLAN' AS SOURCE_SYSTEM
FROM
    (
        SELECT
            REPORTING_DATE
            , CAMPAIGN_SCOPE
            , ADGAP_IDS
            , CHANNEL
            , SUM(CLICKS) AS CLICKS
            , SUM(MEDIA_COST) AS MEDIA_COST
            , SUM(MEDIA_COST * FEE) AS INFRASTRUCTURE_COST
            , SUM(IMPRESSIONS) AS IMPRESSIONS
        FROM
            (
                SELECT
                    REPORTING_DATE
                    , CAMPAIGN_SCOPE
                    , ADGAP_IDS
                    , CHANNEL
                    , SUM(CLICKS) AS CLICKS
                    , SUM(CASE
                        WHEN KOSTEN_NNN_EFF > KOSTEN_NNN THEN KOSTEN_NNN
                        ELSE KOSTEN_NNN_EFF
                    END ) AS MEDIA_COST
                    , MAX(FEE) AS FEE
                    , SUM(IMPRESSIONS) AS IMPRESSIONS
                FROM
                    (
                        SELECT
                            DATUM AS REPORTING_DATE
                            , CAMPAIGN_SCOPE
                            , ADGAP_IDS
                            ,CHANNEL
                            , SUM(KOSTEN_NNN) AS KOSTEN_NNN
                            , SUM(CLICKS) AS CLICKS
                            , SUM(
                                COALESCE(
                                    CASE
                                        WHEN
                                            MEDIA_BILLING_TYPE = 'CPM'
                                            THEN (
                                                KOSTEN_NNN / NULLIF(BUCHUNGSVOLUMEN, 0)
                                            ) * IMPRESSIONS
                                        WHEN
                                            MEDIA_BILLING_TYPE = 'vCPM'
                                            THEN (
                                                KOSTEN_NNN / NULLIF(BUCHUNGSVOLUMEN, 0)
                                            ) * IMPRESSIONS
                                        WHEN
                                            MEDIA_BILLING_TYPE = 'CPC'
                                            THEN (
                                                KOSTEN_NNN / NULLIF(BUCHUNGSVOLUMEN, 0)
                                            ) * CLICKS
                                        ELSE KOSTEN_NNN
                                    END
                                    , 0
                                )
                            ) AS KOSTEN_NNN_EFF
                            , MAX(FEE) AS FEE
                            , SUM(IMPRESSIONS) AS IMPRESSIONS
                        FROM
                            (
                                SELECT
                                    DIGITAL.ADGAP_IDS
                                    , DIGITAL.DATUM
                                    , DIGITAL.MEDIA_BILLING_TYPE
                                    , DIGITAL.MEDIA_TYPE
                                    , DIGITAL.CAMPAIGN_SCOPE
                                    , DIGITAL.CHANNEL
                                    , MAX(BBA.FEE) AS FEE
                                    , SUM(DIGITAL.KOSTEN_NNN) AS KOSTEN_NNN
                                    , SUM(DIGITAL.IMPRESSIONS) AS IMPRESSIONS
                                    , SUM(DIGITAL.BUCHUNGSVOLUMEN) AS BUCHUNGSVOLUMEN
                                    , SUM(DIGITAL.CLICKS) AS CLICKS
                                FROM
                                    (
                                        SELECT
                                            M.ADGAP_IDS
                                            , D.DATUM
                                            , M.CAMPAIGN_SCOPE
                                            ,CASE
                                                WHEN M.CHANNEL = 'ATV' THEN 'Adressable TV'
                                                WHEN M.CHANNEL = 'OLV' THEN 'Online Video'
                                                WHEN  M.CHANNEL = 'DISPLAY BRANDING' THEN 'Display Branding'
                                                ELSE M.CHANNEL
                                            END AS CHANNEL
                                            , CASE
                                                WHEN UPPER(M.MEDIA_TYPE) = 'ADDRESSABLE TV' THEN 'IO'
                                                WHEN UPPER(M.MEDIA_TYPE) = 'DISPLAY' THEN 'IO'
                                                WHEN UPPER(M.MEDIA_TYPE) = 'OLV' THEN 'IO'
                                                WHEN UPPER(M.MEDIA_TYPE) = 'PAID MOBILE' THEN 'IO'
                                                WHEN UPPER(M.MEDIA_TYPE) = 'PROGRAMMATIC' THEN 'PROGRAMMATIC'
                                                ELSE 'OTHER'
                                            END AS MEDIA_TYPE
                                            , M.MEDIA_BILLING_TYPE
                                            , M.MEDIA_CREATIVE_TYPE
                                            , M.CLIENT
                                            , DAYS_BETWEEN(
                                                M.PLACEMENT_END_CET, M.PLACEMENT_START_CET
                                            ) + 1 AS PLACEMENT_DURATION
                                            , SUM(
                                                M.TARGET_GROUP_SIZE
                                            ) / NULLIF(LOCAL.PLACEMENT_DURATION, 0) AS BUCHUNGSVOLUMEN
                                            , SUM(
                                                M.DAILY_MEDIA_COST_NNN_EURO
                                            ) / NULLIF(LOCAL.PLACEMENT_DURATION, 0) AS KOSTEN_NNN
                                            , SUM(0) AS CLICKS
                                            , SUM(0) AS IMPRESSIONS
                                        FROM
                                            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_MEDIAPLAN AS M
                                            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_MEDIAPLAN AS M

                                        LEFT JOIN
                                            (
                                                SELECT ADD_DAYS(DATE '2020-01-01', LEVEL - 1) AS DATUM
                                                FROM
                                                    DUAL
                                                CONNECT BY LEVEL <= DAYS_BETWEEN(CURRENT_DATE, '2020-01-01') + 1
                                            ) AS D
                                            ON 1 = 1
                                        WHERE
                                            D.DATUM BETWEEN M.PLACEMENT_START_CET AND M.PLACEMENT_END_CET
                                            AND LOWER(M.CAMPAIGN_SCOPE) IN ('branding', 'wkz', 'vertrieb')
                                            AND LOWER(M.MEDIA_TYPE) NOT IN ('digital radio')
                                            AND (LOWER(M.MEDIA_CREATIVE_TYPE) NOT IN ('sonderkombination'))
                                            AND D.DATUM >= '2021-01-01'
                                        GROUP BY
                                            M.ADGAP_IDS
                                            , D.DATUM
                                            , M.CAMPAIGN_SCOPE
                                            , LOCAL.CHANNEL
                                            , LOCAL.MEDIA_TYPE
                                            , M.MEDIA_BILLING_TYPE
                                            , M.MEDIA_CREATIVE_TYPE
                                            , M.CLIENT
                                            , LOCAL.PLACEMENT_DURATION
                                        UNION ALL
                                        SELECT
                                            MAPP.ADGAP_IDS
                                            , D.DATUM AS DATUM
                                            , MP.CAMPAIGN_SCOPE
                                            , MP.CHANNEL
                                            , CASE
                                                WHEN UPPER(MP.MEDIA_TYPE) = 'ADDRESSABLE TV' THEN 'IO'
                                                WHEN UPPER(MP.MEDIA_TYPE) = 'DISPLAY' THEN 'IO'
                                                WHEN UPPER(MP.MEDIA_TYPE) = 'OLV' THEN 'IO'
                                                WHEN UPPER(MP.MEDIA_TYPE) = 'PAID MOBILE' THEN 'IO'
                                                WHEN UPPER(MP.MEDIA_TYPE) = 'PROGRAMMATIC' THEN 'PROGRAMMATIC'
                                                ELSE 'OTHER'
                                            END AS MEDIA_TYPE
                                            , MP.MEDIA_BILLING_TYPE
                                            , MP.MEDIA_CREATIVE_TYPE
                                            , MP.CLIENT
                                            , DAYS_BETWEEN(
                                                MP.PLACEMENT_END_CET, MP.PLACEMENT_START_CET
                                            ) + 1 AS PLACEMENT_DURATION
                                            , SUM(0) AS BUCHUNGSVOLUMEN
                                            , SUM(0) AS KOSTEN_NNN
                                            , SUM(AD.CLICKS) AS CLICKS
                                            , SUM(AD.IMPRESSIONS) AS IMPRESSIONS
                                        FROM
                                            (
                                                SELECT ADD_DAYS(DATE '2020-01-01', LEVEL - 1) AS DATUM
                                                FROM
                                                    DUAL
                                                CONNECT BY LEVEL <= DAYS_BETWEEN(CURRENT_DATE, '2020-01-01') + 1
                                            ) AS D
                                        -------------Adition-------------
                                        INNER JOIN
                                            (
                                                SELECT
                                                    COALESCE(
                                                        BANNER_EXTERNAL_ID, CAMPAIGN_EXTERNAL_ID
                                                    ) AS ADGAP_ID
                                                    , REPORTING_DATE
                                                    , SUM(CLICKS) AS CLICKS
                                                    , SUM(IMPRESSIONS) AS IMPRESSIONS
                                                FROM
                                                    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_ADITION_PERFORMANCE
                                                    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_ADITION_PERFORMANCE

                                                GROUP BY
                                                    LOCAL.ADGAP_ID, REPORTING_DATE
                                            ) AS AD
                                            ON AD.REPORTING_DATE = D.DATUM
                                        -- AdGap ID Mapping--
                                        INNER JOIN
                                            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_ADGAP_ID_MAPPING_R MAPP
                                            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_ADGAP_ID_MAPPING_R AS MAPP

                                            ON MAPP.ADGAP_ID = AD.ADGAP_ID
                                        -- Mediaplan--
                                        INNER JOIN
                                            (
                                                SELECT
                                                    ADGAP_IDS
                                                    , CAMPAIGN_SCOPE
                                                    , CASE
                                                        WHEN CHANNEL = 'ATV' THEN 'Adressable TV'
                                                        WHEN CHANNEL = 'OLV' THEN 'Online Video'
                                                        WHEN  CHANNEL = 'DISPLAY BRANDING' THEN 'Display Branding'
                                                        ELSE CHANNEL
                                                    END AS CHANNEL
                                                    , MEDIA_BILLING_TYPE
                                                    , MEDIA_CREATIVE_TYPE
                                                    , MEDIA_TYPE
                                                    , CLIENT
                                                    , PLACEMENT_START_CET
                                                    , PLACEMENT_END_CET
                                                FROM
                                                    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_MEDIAPLAN
                                                    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BPN_DIGITALMEDIA_MEDIAPLAN

                                            ) AS MP
                                            ON MAPP.ADGAP_IDS = MP.ADGAP_IDS

                                        WHERE
                                            UPPER(MP.CAMPAIGN_SCOPE) IN ('BRANDING', 'WKZ', 'VERTRIEB')
                                            AND UPPER(MEDIA_TYPE) NOT IN ('DIGITAL RADIO')
                                            AND UPPER(MP.MEDIA_CREATIVE_TYPE) NOT IN ('SONDERKOMBINATION')
                                            AND DATUM BETWEEN '2021-01-01' AND CURRENT_DATE - 1
                                        GROUP BY
                                            MAPP.ADGAP_IDS
                                            , LOCAL.DATUM
                                            , MP.CAMPAIGN_SCOPE
                                            , MP.CHANNEL
                                            , LOCAL.MEDIA_TYPE
                                            , MP.MEDIA_BILLING_TYPE
                                            , MP.MEDIA_CREATIVE_TYPE
                                            , MP.CLIENT
                                            , LOCAL.PLACEMENT_DURATION
                                    ) AS DIGITAL
                                LEFT JOIN
                                     PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R BBA
                                    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R AS BBA

                                    ON CAST(
                                        BBA.YEAR_MONTH AS DECIMAL(6)
                                    ) = CAST(TO_CHAR(DIGITAL.DATUM, 'YYYYMM') AS DECIMAL(6))
                                    AND UPPER(BBA.CHANNEL_BRANDING) = 'DIGITAL'
                                    AND UPPER(BBA.SUB_CHANNEL_BRANDING) = DIGITAL.MEDIA_TYPE
                                GROUP BY
                                    DIGITAL.ADGAP_IDS
                                    , DIGITAL.DATUM
                                    , DIGITAL.MEDIA_BILLING_TYPE
                                    , DIGITAL.MEDIA_TYPE
                                    , DIGITAL.CAMPAIGN_SCOPE
                                    , DIGITAL.CHANNEL
                            )
                        GROUP BY
                            LOCAL.REPORTING_DATE
                            , CAMPAIGN_SCOPE
                            , ADGAP_IDS
                            ,CHANNEL
                    )
                GROUP BY
                    REPORTING_DATE
                    , CAMPAIGN_SCOPE
                    , ADGAP_IDS
                    ,CHANNEL
            )
        GROUP BY
            REPORTING_DATE
            , CAMPAIGN_SCOPE
            , ADGAP_IDS
            ,CHANNEL
    )
WHERE
    REPORTING_DATE <= CURRENT_DATE - 1
GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
-- (D)OOH
SELECT
    CASE
        WHEN INSTR(UPPER(JVB_DOOH.JOSTVONBRANDIS_CAMPAIGN_NAME), '_WKZ_') > 0 THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    ,CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        ELSE NULL
    END AS BUDGET_LOGIC_SHARE
    ,CASE
        WHEN INSTR(UPPER(JVB_DOOH.JOSTVONBRANDIS_CAMPAIGN_NAME), '_VERTRIEB_') > 0 THEN 'VERTRIEB'
        ELSE 'BRANDING'
    END AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , '' AS CAMP_ID
    , '(Digital) Out Of Home' AS CHANNEL
    , 0 AS CLICKS
    , SUM(
        CASE
            WHEN
                UPPER(
                    JVB_DOOH.SUB_CHANNEL
                ) = 'PROGRAMMATIC (D)OOH'
                THEN (
                    JVB_DOOH.COST_EURO_NN_WITHOUT_BONUS
                ) - (JVB_DOOH.COST_EURO_NN_WITHOUT_BONUS) * (BBA.DISCOUNT)
            WHEN
                UPPER(
                    JVB_DOOH.SUB_CHANNEL
                ) = '(D)OOH' THEN (JVB_DOOH.COST_EURO_NN_WITHOUT_BONUS) - (JVB_DOOH.COST_EURO_NN_WITHOUT_BONUS) * (BBA.DISCOUNT)
        END
    ) AS MEDIA_COST
    , SUM(
        CASE
            WHEN UPPER(JVB_DOOH.SUB_CHANNEL) = 'PROGRAMMATIC (D)OOH'
                THEN
                    (JVB_DOOH.COST_EURO_NNN * BBA.MANAGED_FEE)
                    + (JVB_DOOH.COST_EURO_NNN * BBA.DSP_FEE)
                    + (JVB_DOOH.COST_EURO_NNN * BBA.FEE)
            WHEN UPPER(JVB_DOOH.SUB_CHANNEL) = '(D)OOH' THEN
                (JVB_DOOH.COST_EURO_NNN * BBA.FEE)
        END
    ) AS INFRASTRUCTURE_COST
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , 0 AS IMPRESSIONS
    , JVB_DOOH.REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_JOSTVONBRANDIS_PERFORMANCE' AS SOURCE_SYSTEM
FROM
    (
        SELECT
            CAST(PLANNING_DATE AS DATE) AS REPORTING_DATE
            , JOSTVONBRANDIS_CAMPAIGN_NAME
            , CASE
                WHEN INSTR(UPPER(COST_TYPE), 'PROGR.') > 0 THEN 'PROGRAMMATIC (D)OOH'
                ELSE '(D)OOH'
            END AS SUB_CHANNEL
            , SUM(COST_EURO_NN_WITHOUT_BONUS) AS COST_EURO_NN_WITHOUT_BONUS
            , SUM(COST_EURO_NNN) AS COST_EURO_NNN
        FROM
            PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_JOSTVONBRANDIS_PERFORMANCE
            --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_JOSTVONBRANDIS_PERFORMANCE

        WHERE
            UPPER(COST_TYPE) NOT IN
            (
                'FOTO DOKU'
                , 'GENEHMIGUNG'
                , 'JVB HONORAR PRODUKTION'
                , 'LEISTUNGSHONORAR'
                , 'PRODUKTION ANALOG'
                , 'PRODUKTION DIGITAL'
                , 'SONSTIGES'
                , 'STORNOGEBUEHR'
                , 'VERSAND'
            )
        GROUP BY

            LOCAL.REPORTING_DATE
            , LOCAL.SUB_CHANNEL
            , JOSTVONBRANDIS_CAMPAIGN_NAME
    ) AS JVB_DOOH
LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R BBA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_BOARD_BRANDING_AGENCY_FEE_R AS BBA

    ON CAST(BBA.YEAR_MONTH AS DECIMAL(6)) = CAST(TO_CHAR(JVB_DOOH.REPORTING_DATE, 'YYYYMM') AS DECIMAL(6))
    AND UPPER(BBA.CHANNEL_BRANDING) = '(D)OOH'
    AND UPPER(BBA.SUB_CHANNEL_BRANDING) = UPPER(JVB_DOOH.SUB_CHANNEL)
WHERE
    EXTRACT(YEAR FROM JVB_DOOH.REPORTING_DATE) >= 2021 AND JVB_DOOH.REPORTING_DATE <= CURRENT_DATE - 1
GROUP BY
    LOCAL.BUDGET_LOGIC
    ,LOCAL.BUDGET_LOGIC_SHARE
    ,LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , JVB_DOOH.REPORTING_DATE

UNION ALL
--DISPLAY (INHOUSE)
--ODIN
SELECT
    CASE
        WHEN INSTR(DAT.CAMP_ID, '-TW.BC-') > 0 THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(DAT.CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(DAT.CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    ,'Display Branding' AS CHANNEL
    , 0 AS PARTNER_CLICKS
    , SUM(DAT.NET_REVENUE_EURO) AS MEDIA_COST
    -->Revjet: 5% auf Kosten und 0,006$ auf die Impression
    , SUM(DAT.NET_REVENUE_EURO * 0.05) + (SUM(DAT.IMPRESSIONS / 1000 * 0.006) / MAX(ECB_R.EXCHANGE_RATE_EUR_TO_USD)) AS INFRASTRUCTURE_COST
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
    , DAT.REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_ODIN_AD_PERFORMANCE' AS SOURCE_SYSTEM
FROM
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_ODIN_AD_PERFORMANCE AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_ODIN_AD_PERFORMANCE AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON CA.CAMPAIGN_ID = DAT.CAMP_ID
LEFT JOIN
    PRD_BIWA_SERVICE.CAMPAIGNDATA_MPC_ECB_EXCHANGE_RATES_R AS ECB_R
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_ECB_EXCHANGE_RATES_R AS ECB_R
    ON ECB_R.REPORTING_DATE = DAT.REPORTING_DATE
WHERE
    DAT.REPORTING_DATE >= '2021-01-01'
    AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.NET_REVENUE_EURO, 0)) != 0
    AND DAT.ODIN_CUSTOMER_ID = 4 --2=Advertising Services, 4=Strategy & Brand
GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
--SOCIAL ADS (INHOUSE)
--Facebook
SELECT
    CASE
        WHEN
            INSTR(DAT.CAMP_ID, '-TW.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-PF.BC-') > 0
            THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-F.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    , 'Social Ads Branding' AS CHANNEL
    , SUM(DAT.CLICKS) AS PARTNER_CLICKS
    , SUM(DAT.COST_EURO) AS MEDIA_COST
    , SUM(DAT.COST_EURO * 0.015) AS INFRASTRUCTURE_COST  --Smartly 1,5% des Mediaspends
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
    , DAT.REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_FACEBOOK_AD_PERFORMANCE' AS SOURCE_SYSTEM
FROM
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_MPC_FACEBOOK_AD_PERFORMANCE_CAMPID AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_MPC_FACEBOOK_AD_PERFORMANCE_CAMPID AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON CA.CAMPAIGN_ID = DAT.CAMP_ID
WHERE
    DAT.REPORTING_DATE >= '2021-01-01'
    AND DAT.FACEBOOK_ACCOUNT_ID = 79042523 --OTTO Page Account
    AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) != 0
GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
--Pinterest
SELECT
    CASE
        WHEN
            INSTR(DAT.CAMP_ID, '-TW.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-PF.BC-') > 0
            THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-F.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(DAT.CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(DAT.CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    , 'Social Ads Branding' AS CHANNEL
    , SUM(DAT.CLICKS_PAID) AS PARTNER_CLICKS
    , SUM(DAT.COST_EURO) AS MEDIA_COST
    , 0 AS INFRASTRUCTURE_COST  --kein Smartly Invest
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS_PAID) AS IMPRESSIONS
    , DAT.REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE' AS SOURCE_SYSTEM
FROM
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_PINTEREST_PROMOTION_PIN_PERFORMANCE AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON CA.CAMPAIGN_ID = DAT.CAMP_ID
WHERE
    DAT.REPORTING_DATE >= '2021-01-01'
    AND (COALESCE(DAT.IMPRESSIONS_PAID, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS_PAID, 0)) != 0
    -- AND PINTEREST_ADVERTISER_ID='549757038100'
    -- AND PINTEREST_ORDER_LINE_ID IN ('2817498577793','2817498596222','2817498596221')
    AND (INSTR(DAT.CAMP_ID, '-SA.C-') = 0 OR INSTR(UPPER(DAT.PINTEREST_CAMPAIGN_NAME), 'PERFORMANCE') = 0)
GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
--Snapchat
SELECT
    CASE
        WHEN
            INSTR(DAT.CAMP_ID, '-TW.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-PF.BC-') > 0
            THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-F.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(DAT.CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(DAT.CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    , 'Social Ads Branding' AS CHANNEL
    , 0 AS PARTNER_CLICKS
    , SUM(DAT.COST_EURO) AS MEDIA_COST
    , SUM(DAT.COST_EURO * 0.015) AS INFRASTRUCTURE_COST  --Smartly 1,5% des Mediaspends
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
    , DAT.REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_SNAP_AD_PERFORMANCE' AS SOURCE_SYSTEM
FROM
     PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_SNAP_AD_PERFORMANCE AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_SNAP_AD_PERFORMANCE AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON DAT.CAMP_ID = CA.CAMPAIGN_ID
WHERE
    DAT.REPORTING_DATE >= '2021-01-01'
    AND LOWER(DAT.SNAP_ACCOUNT_NAME) = 'otto branding'
    AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0)) != 0

GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
--YouTube
SELECT
    CASE
        WHEN
            INSTR(DAT.CAMP_ID, '-TW.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-PF.BC-') > 0
            THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-F.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(DAT.CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(DAT.CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    , 'Online Video' AS CHANNEL
    , SUM(DAT.CLICKS) AS PARTNER_CLICKS
    , SUM(DAT.COST_EURO) AS MEDIA_COST
    , 0 AS INFRASTRUCTURE_COST  --kein Smartly Invest
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
    , DAT.DATE_COL AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_GOOGLE_AD_PERFORMANCE_REPORT' AS SOURCE_SYSTEM
FROM
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_GOOGLE_AD_PERFORMANCE_REPORT AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_GOOGLE_AD_PERFORMANCE_REPORT AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON CA.CAMPAIGN_ID = DAT.CAMP_ID
WHERE
    DAT.DATE_COL >= '2021-01-01'
    AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) != 0
    AND DAT.GOOGLE_ACCOUNT_ID IN (9972258233, 1878078603)
    AND (INSTR(DAT.CAMP_ID, '-SA.C-') = 0 OR INSTR(UPPER(DAT.GOOGLE_CAMPAIGN_NAME), 'PERFORMANCE') = 0)
GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE

UNION ALL
--TikTok
SELECT
    CASE
        WHEN
            INSTR(DAT.CAMP_ID, '-TW.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-PF.BC-') > 0
            THEN 2
        WHEN
            INSTR(DAT.CAMP_ID, '-W.BC-') > 0
            OR INSTR(DAT.CAMP_ID, '-F.BC-') > 0
            OR (INSTR(DAT.CAMP_ID, '-SS.AG-') > 0 AND INSTR(DAT.CAMP_ID, '-D.C-') > 0)
            OR INSTR(DAT.CAMP_ID, '-SES.C-') > 0
            THEN 3
        ELSE 1
    END AS BUDGET_LOGIC
    , CASE
        WHEN LOCAL.BUDGET_LOGIC = 1 THEN 1
        WHEN LOCAL.BUDGET_LOGIC = 3 THEN 0
        WHEN INSTR(CAMP_ID, '.WS-') > 0 THEN 1 - REGEXP_SUBSTR(REGEXP_SUBSTR(CAMP_ID, '(\d+)\.WS'), '\d+') / 100
    END AS BUDGET_LOGIC_SHARE
    , 'BRANDING' AS CAMPAIGN_TYPE
    , FALSE AS IS_PLAN
    , DAT.CAMP_ID AS CAMP_ID
    , 'Social Ads Branding' AS CHANNEL
    , SUM(DAT.CLICKS) AS PARTNER_CLICKS
    , SUM(DAT.COST_EURO) AS MEDIA_COST
    , 0 AS INFRASTRUCTURE_COST  --kein Smartly Invest
    , LOCAL.MEDIA_COST + LOCAL.INFRASTRUCTURE_COST AS COST_EURO
    , SUM(DAT.IMPRESSIONS) AS IMPRESSIONS
    , DAT.REPORTING_DATE AS REPORTING_DATE
    , '${EXAENV}_BIWC_SERVICE_CAMPAIGNDATA_PROV.MPC_TIKTOK_AD_PERFORMANCE' AS SOURCE_SYSTEM
FROM
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.CAMPAIGNDATA_MPC_TIKTOK_AD_PERFORMANCE AS DAT
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.CAMPAIGNDATA_MPC_TIKTOK_AD_PERFORMANCE AS DAT

LEFT JOIN
    PRD_BIWA_ONLINEMARKETINGSTEUERUNG.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    --${EXAENV}_BIWD_ONLINEMARKETINGSTEUERUNG_CONS.ONLINEMARKETINGSTEUERUNG_OM_CAMPAIGN_PERSIST AS CA
    ON DAT.CAMP_ID = CA.CAMPAIGN_ID
WHERE
    DAT.REPORTING_DATE >= '2021-01-01'
    AND DAT.TIKTOK_ACCOUNT_ID = 7031568624221437954
    AND (COALESCE(DAT.IMPRESSIONS, 0) + COALESCE(DAT.COST_EURO, 0) + COALESCE(DAT.CLICKS, 0)) != 0
    AND (INSTR(DAT.CAMP_ID, '-SA.C-') = 0 OR INSTR(UPPER(DAT.TIKTOK_CAMPAIGN_NAME), 'PERFORMANCE') = 0)

GROUP BY
    LOCAL.BUDGET_LOGIC
    , LOCAL.BUDGET_LOGIC_SHARE
    , LOCAL.CAMPAIGN_TYPE
    , LOCAL.IS_PLAN
    , LOCAL.CAMP_ID
    , LOCAL.CHANNEL
    , LOCAL.REPORTING_DATE
;