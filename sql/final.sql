WITH USER_GROUP_LOG AS 
(
    WITH USER_GROUP_LOG_TMP AS
    (
        SELECT
            LUGA.HK_L_USER_GROUP_ACTIVITY,
            LUGA.HK_GROUP_ID,
            LUGA.HK_USER_ID,
            SAH.EVENT
        FROM STV2024021942__DWH.L_USER_GROUP_ACTIVITY LUGA 
        LEFT JOIN STV2024021942__DWH.S_AUTH_HISTORY SAH ON LUGA.HK_L_USER_GROUP_ACTIVITY = SAH.HK_L_USER_GROUP_ACTIVITY
        WHERE LUGA.HK_GROUP_ID IN (
                                   SELECT HK_GROUP_ID 
                                   FROM STV2024021942__DWH.H_GROUPS HG 
                                   ORDER BY REGISTRATION_DT 
                                   LIMIT 10
                                   )
        AND SAH.EVENT = 'ADD'
    )
    SELECT 
        HK_GROUP_ID,
        COUNT(DISTINCT HK_USER_ID) AS CNT_ADDED_USERS
    FROM USER_GROUP_LOG_TMP
    GROUP BY 1
)
,USER_GROUP_MESSAGES AS (
    SELECT
        HK_GROUP_ID,
        COUNT(DISTINCT HK_USER_ID) AS CNT_USERS_IN_GROUP_WITH_MESSAGES
    FROM (
        SELECT
            LGD.HK_GROUP_ID,
            LUM.HK_USER_ID,
            LGD.HK_MESSAGE_ID
        FROM STV2024021942__DWH.L_GROUPS_DIALOGS LGD 
        LEFT JOIN STV2024021942__DWH.L_USER_MESSAGE LUM ON LGD.HK_MESSAGE_ID = LUM.HK_MESSAGE_ID
         ) TMP
    GROUP BY 1
)
SELECT 
    UGL.HK_GROUP_ID,
    UGL.CNT_ADDED_USERS,
    UGM.CNT_USERS_IN_GROUP_WITH_MESSAGES,
    UGM.CNT_USERS_IN_GROUP_WITH_MESSAGES / UGL.CNT_ADDED_USERS AS GROUP_CONVERSION
FROM USER_GROUP_LOG AS UGL
LEFT JOIN USER_GROUP_MESSAGES AS UGM ON UGL.HK_GROUP_ID = UGM.HK_GROUP_ID
ORDER BY UGM.CNT_USERS_IN_GROUP_WITH_MESSAGES / UGL.CNT_ADDED_USERS DESC 