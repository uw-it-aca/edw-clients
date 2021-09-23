# Copyright 2021 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import pandas as pd
import sqlalchemy as sa
from edw_clients.base import BaseDAO
from edw_clients.utilities import term_name_to_number


class EDWCompassDAO(BaseDAO):

    def __init__(self):
        super(BaseDAO, self).__init__()

    def parse_sis_term_id(self, sis_term_id):
        year = None
        quarter_num = None
        try:
            parts = sis_term_id.split("-")
            year = parts[0]
            quarter_num = str(term_name_to_number(parts[1]))
        except (KeyError, ValueError) as e:
            raise ValueError(f"Exception occured parsing sis-term-id: {e}")
        return year, quarter_num

    def get_enrolled_students_cte(self, sis_term_id, filters=None):
        year, quarter_num = self.parse_sis_term_id(sis_term_id)
        yrq = "".join([str(year), str(quarter_num)])
        query = sa.select([sa.text(
            f"""
            DISTINCT
                enr.StudentNumber,
                stu1.uw_netid as UWNetID,
                enr.StudentName,
                enr.StudentNamePreferredFirst,
                enr.StudentNamePreferredMiddle,
                enr.StudentNamePreferredLast,
                enr.BirthDate,
                enr.StudentEmail,
                enr.ExternalEmail,
                enr.LocalPhoneNumber,
                enr.Gender,
                enr.GPA,
                enr.TotalCredits,
                enr.TotalUWCredits,
                enr.CampusCode,
                enr.CampusDesc,
                enr.ClassCode,
                enr.ClassDesc,
                enr.EnrollStatusCode,
                enr.ExemptionCode,
                enr.ExemptionDesc,
                enr.SpecialProgramCode, 
                enr.SpecialProgramDesc,
                enr.HonorsProgramCode,
                enr.HonorsProgramInd,
                enr.ResidentCode,
                enr.ResidentDesc,
                enr.PermAddrLine1,
                enr.PermAddrLine2,
                enr.PermAddrCity,
                enr.PermAddrState,
                enr.PermAddr5DigitZip,
                enr.PermAddr4DigitZip,
                enr.PermAddrCountry,
                enr.PermAddrPostalCode,
                enr.Major1,
                smc.major_abbr,
                smc.major_full_nm,
                smc.major_name,
                smc.major_short_nm,
                enr.IntendedMajor1,
                enr.RegisteredInQuarter
            FROM EDWPresentation.sec.EnrolledStudent AS enr
            LEFT JOIN UWSDBDataStore.sec.student_1 AS stu1 ON enr.SystemKey = stu1.system_key
            LEFT JOIN UWSDBDataStore.sec.student_1_college_major AS cm ON enr.SystemKey = cm.system_key
            LEFT JOIN UWSDBDataStore.sec.sr_major_code AS smc ON cm.major_abbr = smc.major_abbr AND smc.major_pathway = cm.pathway
            """  # noqa
        )])
        query = query.where(sa.text(f"AcademicYrQtr = '{yrq}'"))
        return query.cte('student_info')

    def get_number_enrolled_students(self, sis_term_id):
        cte = self.get_enrolled_students_cte(sis_term_id)
        query = sa.select([
            sa.text('COUNT(StudentNumber)')]).select_from(cte)
        conn = self.get_connection()
        return conn.execute(query).scalar()

    def get_enrolled_students_df(self, sis_term_id, filters=None):
        cte = self.get_enrolled_students_cte(sis_term_id, filters=filters)
        query = (
            sa.select([
                sa.column('StudentNumber'),
                sa.column('UWNetID'),
                sa.column('StudentName'),
                sa.column('StudentNamePreferredFirst'),
                sa.column('StudentNamePreferredMiddle'),
                sa.column('StudentNamePreferredLast'),
                sa.column('BirthDate'),
                sa.column('StudentEmail'),
                sa.column('ExternalEmail'),
                sa.column('LocalPhoneNumber'),
                sa.column('Gender'),
                sa.column('GPA'),
                sa.column('TotalCredits'),
                sa.column('TotalUWCredits'),
                sa.column('CampusCode'),
                sa.column('CampusDesc'),
                sa.column('ClassCode'),
                sa.column('ClassDesc'),
                sa.column('EnrollStatusCode'),
                sa.column('ExemptionCode'),
                sa.column('ExemptionDesc'),
                sa.column('SpecialProgramCode'), 
                sa.column('SpecialProgramDesc'),
                sa.column('HonorsProgramCode'),
                sa.column('HonorsProgramInd'),
                sa.column('ResidentCode'),
                sa.column('ResidentDesc'),
                sa.column('PermAddrLine1'),
                sa.column('PermAddrLine2'),
                sa.column('PermAddrCity'),
                sa.column('PermAddrState'),
                sa.column('PermAddr5DigitZip'),
                sa.column('PermAddr4DigitZip'),
                sa.column('PermAddrCountry'),
                sa.column('PermAddrPostalCode'),
                sa.column('Major1'),
                sa.column('major_abbr'),
                sa.column('major_full_nm'),
                sa.column('major_name'),
                sa.column('major_short_nm'),
                sa.column('IntendedMajor1'),
                sa.column('RegisteredInQuarter')
            ])
            .select_from(cte)
            .order_by('StudentName')
        )
        conn = self.get_connection()
        enrolled_df = pd.read_sql(query, conn)
        return enrolled_df