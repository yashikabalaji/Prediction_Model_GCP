
CREATE OR REPLACE VIEW `vlba-rsd-grp3.RSD_department.employee_details_with_success_value` AS
SELECT 
    emp_info.PERNR, 
    emp_info.Gender, 
    DATE_DIFF(CURRENT_DATE(), DATE(emp_info.`Birth date`), YEAR) AS Age,
    emp_info.Nationality, 
    emp_info.`Marital Status Key` AS Marital_Status,
    emp_contract_info.`Business Area` AS Business_Area,
    emp_contract_info.`Payroll Area` AS Payroll_Area,
    emp_contract_info.`Position` AS Position_of_Employee,
    emp_qualifications.`EducationField` AS Education_Field, 
    emp_qualifications.`Institution` AS Education_Institution,
    COALESCE(emp_success_projects.number_of_successful_projects, 0) AS number_of_successful_projects,
    COALESCE(emp_failed_projects.number_of_failed_projects, 0) AS number_of_failed_projects,
    CASE 
        WHEN COALESCE(emp_success_projects.number_of_successful_projects, 0) + COALESCE(emp_failed_projects.number_of_failed_projects, 0) = 0
        THEN 0.0
        ELSE ROUND(
            COALESCE(emp_success_projects.number_of_successful_projects, 0) / 
            (COALESCE(emp_success_projects.number_of_successful_projects, 0) + COALESCE(emp_failed_projects.number_of_failed_projects, 0)), 
            2
        )
    END AS success_value
FROM 
    `vlba-rsd-grp3.RSD_department.RSD_EmplInfo` emp_info
JOIN 
    `vlba-rsd-grp3.RSD_department.RSD_EmplContrInfo` emp_contract_info
ON 
    emp_info.PERNR = emp_contract_info.PERNR
JOIN 
    `vlba-rsd-grp3.RSD_department.RSD_EmplQualifications` emp_qualifications
ON 
    emp_info.PERNR = emp_qualifications.PERNR
LEFT JOIN 
    (SELECT 
        project_members.MemberId, 
        COUNT(sapproj.ID) AS number_of_successful_projects 
     FROM 
        `vlba-rsd-grp3.RSD_department.RSD_SAPProj` sapproj
     JOIN 
        `vlba-rsd-grp3.RSD_department.RSD_ProjMembers` project_members
     ON 
        sapproj.ID = project_members.Project_ID 
     WHERE 
        sapproj.state = 'successful'
     GROUP BY 
        project_members.MemberId
    ) emp_success_projects
ON 
    emp_info.PERNR = emp_success_projects.MemberId
LEFT JOIN 
    (SELECT 
        project_members.MemberId, 
        COUNT(sapproj.ID) AS number_of_failed_projects 
     FROM 
        `vlba-rsd-grp3.RSD_department.RSD_SAPProj` sapproj
     JOIN 
        `vlba-rsd-grp3.RSD_department.RSD_ProjMembers` project_members
     ON 
        sapproj.ID = project_members.Project_ID 
     WHERE 
        sapproj.state = 'failed'
     GROUP BY 
        project_members.MemberId
    ) emp_failed_projects 
ON 
    emp_info.PERNR = emp_failed_projects.MemberId
WHERE
    emp_contract_info.`Business Area` = 'MPD'
ORDER BY 
    emp_info.PERNR;
