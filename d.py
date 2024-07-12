Hours/Mandatory Contribution Population: 
•	Starting population (starting population is the same for both hours/mandatory contribution and voluntary contribution review):
o	Participants with an active type status code of A – Active, E – Eligible, I – Inactive, L – Leave of Absence, S – Suspended, U – Military Leave, X – Transfer
o	Participants with a non-active type status code and a termination date on/after 04/01/2023 of D – Death, M – Disability, R – Retired, and T – Terminated.
o	Employee Social Security Number DOES NOT contain Alpha Character (beneficiaries and alternate payees)
o	Bad SSN’s - separate group with all data points
•	Data pull of contributions for all participants from starting population with contributions in Mandatory source 01 & Mandatory Match source 03 inception to date, provide the date of the first contribution into source 01 & source 03 and date of the last contribution into source 01 & source 03.
•	Carve the starting population into the following groups:
o	Population 1: Identify participants who had contributions into Mandatory source 01 and Mandatory Match source 03 prior to 04/01/2023.  
	Population 1A: No division change since 04/01/2023 and ADOH < 04/01/2023 or blank ADOH – no further review needed.
	Population 1B: Division changes after 04/01/2023 and ADOH <04/01/2023 or blank ADOH – further review needed. 
	Population 1C: Participants with an ADOH > 04/01/2023 – further review needed.  
o	Population 2: Identify participants who are in division F, SO, SD, and Q and have an HCE indicator of N or NULL.  
	Population 2A: No division change since 04/01/2023 – no further review needed.
	Population 2B: Division changes after 04/01/2023 – further review needed.
o	Population 3: Identify participants with a division name of B, QP, N, FP, T, MAIN, NB, NULL (blank), and any other division not equal to F, SO, SD, Q, H, S or W.
	Population 3A: No division change since 04/01/2023 – no further review needed.
	Population 3B: Division changes after 04/01/2023 – further review needed.
o	Population 4: Identify participants with a current division of F, SO, SD, and Q with an HCE indicator of Y and participants in division H, S, or W (all regardless of HCE flag).
Note: List of participants in F, SO, SD, and Q with initial HCE indicator of N but updated to Y will be provided by VUMC and need to be moved to population 4 for further review.
	
Hours/Mandatory Contribution Population file names (CSV file format):
Population Number	CSV Output File Name
Population 1A	HMC Pop 1A
Population 1B	HMC Pop 1B
Population 1C	HMC Pop 1C
Population 2A	HMC Pop 2A
Population 2B	HMC Pop 2B
Population 3A	HMC Pop 3A
Population 3B	HMC Pop 3B
Population 4	HMC Pop 4
BAD SSN’s 	HMC BAD SSN

File layout for the hours/mandatory contribution populations above:
Include SSN, First Name, Last Name, Employee ID, Hire Date, Adjusted Hire date, Termination date, Status, division name, division history from 04/01/2023 thru current and date of division change, HCE flag, date of first contribution into source 01 and source 03, date of last contribution into source 01 and source 03, total hours for 2022, 2023, and 2024 (one participant per line).

Voluntary Contribution Population:
•	Starting population:
o	Participants with an active type status code of A – Active, E – Eligible, I – Inactive, L – Leave of Absence, S – Suspended, U – Military Leave, X – Transfer
o	Participants with a non-active type status code and a termination date on/after 04/01/2023 of D – Death, M – Disability, R – Retired, and T – Terminated.
o	Employee Social Security Number DOES NOT contain Alpha Character (beneficiaries and alternate payees)
o	Bad SSN’s separate population
•	Data pull of all Pre-tax deferrals and Roth deferrals with respective effective dates.
•	Carve the starting population into the following groups:
o	Population 1: Identify participants who have no Voluntary deferral history for Pre-tax or Roth deferrals (group will be reviewed for Voluntary Match contributions).
o	Population 2: Identify participants who are in division F, SO, SD, and Q and have an HCE indicator of N or NULL.
o	Population 3: Identify participants with a division name of B, QP, N, FP, T, MAIN, NB, NULL (blank).
o	Population 4: Identify participants in division F, SO, SD, and Q with an HCE indicator of Y and participants in division H, S, or W.
