CREATE TABLE IF NOT EXISTS STV2024021942__DWH.L_USER_GROUP_ACTIVITY
(
    HK_L_USER_GROUP_ACTIVITY BIGINT UNIQUE PRIMARY KEY,
    HK_USER_ID      BIGINT NOT NULL CONSTRAINT FK_L_USER_GROUP_ACTIVITY_USER REFERENCES STV2024021942__DWH.H_USERS (HK_USER_ID),
    HK_GROUP_ID BIGINT NOT NULL CONSTRAINT FK_L_USER_GROUP_ACTIVITY_GROUP REFERENCES STV2024021942__DWH.H_GROUPS (HK_GROUP_ID),
    LOAD_DT DATETIME,
    LOAD_SRC VARCHAR(20)
)
ORDER BY LOAD_DT
SEGMENTED BY HK_L_USER_GROUP_ACTIVITY ALL NODES
PARTITION BY LOAD_DT::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(LOAD_DT::DATE, 3, 2);