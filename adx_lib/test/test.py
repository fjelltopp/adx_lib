from adx_lib.pjnz_file import PJNZFile
from adx_lib import spectrum_tables
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector
from rpy2.robjects import r
import numpy

"""
This script is used to test the files in this repo.

IT SHOULD (OF COURSE) BECOME A PROPER TEST SUIT.
"""

schemas_directory = '../../../ckanext-unaids/ckanext/unaids/validation_table_schemas/'
pjnz_directory = '../../../'

pjnz = PJNZFile(pjnz_directory+'Malawi_2019_v22.PJNZ')


# pjnz.extract_surv_data()

# pjnz.dp_tables = {
#     'ARVRegimen MV2': {
#         "type": float,
#         "columns": ["Drop"] + pjnz.default_columns
#     }
# }
# print(pjnz.dp('ARVRegimen MV2'))

# spectrum_tables.ARTTable(
#     schemas_directory + 'spectrum_art.json'
# ).create_csv_table(pjnz)
#
# spectrum_tables.BreastfeedingTable(
#     schemas_directory + 'spectrum_breastfeeding.json'
# ).create_csv_table(pjnz)

# spectrum_tables.ANCTestingTable(
#     schemas_directory + 'spectrum_anc_test.json'
# ).create_csv_table(pjnz)
#
spectrum_tables.ANCPrevalenceTable(
    schemas_directory + 'spectrum_anc_prev.json'
).create_csv_table(pjnz)
#
# spectrum_tables.PMTCTTable(
#     schemas_directory + 'spectrum_pmtct.json'
# ).create_csv_table(pjnz)

# spectrum_tables.CaseMortalityTable(
#     schemas_directory + 'spectrum_case_mortality.json'
# ).create_csv_table(pjnz)

# spectrum_tables.KnownStatusTable(
#     schemas_directory + 'spectrum_ks.json'
# ).create_csv_table(pjnz)
