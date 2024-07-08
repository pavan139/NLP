SELECT 
    SSN_N as SSN,
    FST_NM as FirstName,
    LST_NM as LastName,
    HIRE_D as DateOfHire,
    ADJ_HIRE_D as AdjustedDateOfHire,
    TERM_D as TerminationDate,
    DVSN_NM as DivisionName,
    HIGH_CMPS_I as HCEIndicator,
    STS_C as StatusCode
FROM 
    DCRODB00.CVMCPI_PART_INFO
WHERE 
    PLAN_N = '86643';


SELECT 
    SSN_N as SSN,
    DVSN_NM as PreviousDivisionName,
    DVSN_CHG_D as DivisionChangeDate
FROM 
    DCRODB00.CVMCPI_PART_HISTORICAL
WHERE 
    PLAN_N = '86643' 
    AND DVSN_CHG_D BETWEEN '01-JAN-2023' AND '15-MAY-2024';


-- Hours from specific period
SELECT 
    SSN_ID as SSN,
    SUM(TXN_A) as Hours
FROM 
    V_MCSUHY_SRV_UNT_HR_YR
WHERE 
    PLAN_N = '86643' 
    AND TXN_D BETWEEN '01-APR-2023' AND '01-MAY-2024'
GROUP BY 
    SSN_ID;

-- Inception to date hours
SELECT 
    SSN_ID as SSN,
    SUM(TXN_A) as TotalHours
FROM 
    V_MCSUHY_SRV_UNT_HR_YR
WHERE 
    PLAN_N = '86643'
GROUP BY 
    SSN_ID;

SELECT SSN_N as SSN, SUM(TXN_CASH_A) as PreTaxDeferralAmount
FROM TXN_CASH_A.V_RCH1_HIST_ALL
WHERE PLAN_N IN ('86643') AND SRC_C_IN = 2 AND TXN_C = '220' AND ACCT_ITEM_C = '1'
AND TXN_D BETWEEN '03/15/2023' TO '05/15/2024'
GROUP BY SSN_N;

SELECT 
    SSN_N as SSN,
    SUM(CASE WHEN SRC_C_IN = 1 OR SRC_C_IN = 3 THEN TXN_CASH_A ELSE 0 END) as MandatoryMatchAmount,
    SUM(CASE WHEN SRC_C_IN = 2 THEN TXN_CASH_A ELSE 0 END) as PreTaxDeferralAmount,
    SUM(CASE WHEN SRC_C_IN = 13 THEN TXN_CASH_A ELSE 0 END) as RothDeferralAmount,
    SUM(CASE WHEN SRC_C_IN = 5 THEN TXN_CASH_A ELSE 0 END) as VoluntaryMatchAmount,
    SUM(CASE WHEN SRC_C_IN = 15 THEN TXN_CASH_A ELSE 0 END) as QNECAmount
FROM 
    TXN_CASH_A.V_RCH1_HIST_ALL
WHERE 
    PLAN_N IN ('86643') 
    AND TXN_C = '220' 
    AND ACCT_ITEM_C = '1'
    AND (
        (SRC_C_IN IN (1, 3) AND TXN_D BETWEEN '05/02/2016' AND '05/01/2024') OR
        (SRC_C_IN IN (2, 13, 5, 15) AND TXN_D BETWEEN '03/15/2023' TO '05/15/2024')
    )
GROUP BY 
    SSN_N;


