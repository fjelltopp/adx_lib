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

pjnz = PJNZFile(pjnz_directory+'Mauritius_2018_shadow.PJNZ')

# subpops = r['read_epp_subpops'](pjnz_directory+'Mauritius_2018_shadow.PJNZ').rx2('subpops')
# pops = subpops.names
# for pop in pops:
#     print("=======" + pop + "========")
#     print(r['attributes'](subpops.rx2(pop)))

#print(pjnz.epp('subpops'))

# pjnz.epp_subpop()
# pjnz.extract_surv_data()

# pjnz.dp_tables = {
#     'ARVRegimen MV2': {
#         "type": float,
#         "columns": ["Drop"] + pjnz.default_columns
#     }
# }
# print(pjnz.dp('ARVRegimen MV2'))

# spectrum_tables.SizeTable(
#     schemas_directory + 'spectrum_size.json'
# ).create_csv_table(pjnz)

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

# spectrum_tables.ANCPrevalenceTable(
#     schemas_directory + 'spectrum_anc_prev.json'
# ).create_csv_table(pjnz)

# spectrum_tables.ConcPrevalenceTable(
#     schemas_directory + 'spectrum_conc_prev.json'
# ).create_csv_table(pjnz)

# spectrum_tables.TurnoverTable(
#     schemas_directory + 'spectrum_turnover.json'
# ).create_csv_table(pjnz)

# spectrum_tables.HHTable(
#     schemas_directory + 'spectrum_hh.json'
# ).create_csv_table(pjnz)
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
