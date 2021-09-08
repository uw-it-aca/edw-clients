# Copyright 2021 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import pandas as pd
import sqlalchemy as sa
from edw_clients.base import BaseDAO
from edw_clients.utilities import term_name_to_number


class CompassDAO(BaseDAO):

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
            DISTINCT TOP(50)
                enr.StudentNumber,
                stu1.uw_netid as UWNetID,
                enr.StudentName,
                enr.BirthDate,
                enr.StudentEmail,
                enr.ExternalEmail,
                enr.LocalPhoneNumber,
                enr.Gender,
                enr.GPA,
                enr.TotalCredits,
                dm.MajorFullName,
                enr.CampusDesc,
                enr.ClassDesc,
                enr.EnrollStatusCode
            FROM EDWPresentation.sec.EnrolledStudent AS enr
            LEFT JOIN UWSDBDataStore.sec.student_1 AS stu1 ON enr.SystemKey = stu1.system_key
            LEFT JOIN EDWPresentation.sec.factStudentProgramEnrollment AS fspe ON fspe.StudentKeyId = enr.SystemKey
            LEFT JOIN EDWPresentation.sec.dimMajor AS dm ON dm.MajorKeyId = fspe.MajorKeyId
            """
        )])
        query = query.where(sa.text(f"AcademicYrQtr = '{yrq}'"))
        query = query.where(sa.text(f"dm.MajorFullName IS NOT NULL"))
        if filters and filters.get("searchFilter"):
            filter_text = filters["searchFilter"]["filterText"]
            filter_type = filters["searchFilter"]["filterType"]
            if filter_type == "student-number":
                query = query.where(
                    sa.text(f"StudentNumber LIKE '%{filter_text}%'"))
            elif filter_type == "student-name":
                query = query.where(
                    sa.text(f"UPPER(enr.StudentName) LIKE "
                            f"'UPPER('%{filter_text}%'"))
            elif filter_type == "student-email":
                query = query.where(
                    sa.text(f"UPPER(enr.StudentEmail) LIKE "
                            f"UPPER('%{filter_text}%')"))
        return query.cte('student_info')

    def get_enrolled_students_df(self, sis_term_id, filters=None):
        cte = self.get_enrolled_students_cte(sis_term_id, filters=filters)
        query = (
            sa.select([
                'StudentNumber',
                'UWNetID',
                'StudentName',
                'BirthDate',
                'StudentEmail',
                'ExternalEmail',
                'LocalPhoneNumber',
                'Gender',
                'GPA',
                'TotalCredits',
                'CampusDesc',
                'ClassDesc',
                'EnrollStatusCode',
                "STRING_AGG(MajorFullName, ', ') AS MajorDesc"
            ])
            .select_from(cte)
            .group_by(
                'StudentNumber',
                'UWNetID',
                'StudentName',
                'BirthDate',
                'StudentEmail',
                'ExternalEmail',
                'LocalPhoneNumber',
                'Gender',
                'GPA',
                'TotalCredits',
                'CampusDesc',
                'ClassDesc',
                'EnrollStatusCode')
            .order_by('StudentName')
        )
        conn = self.get_connection("EDWPresentation")
        enrolled_df = pd.read_sql(query, conn)
        return enrolled_df