from adx_lib.pjnz_file import PJNZFile
from adx_lib import spectrum_tables
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector
from rpy2.robjects import r
import numpy
import json
import importlib
import os

"""
This script is used to test the files in this repo.

IT SHOULD (OF COURSE) BECOME A PROPER TEST SUIT.
"""

schemas_directory = '../../../ckanext-unaids/ckanext/unaids/validation_table_schemas/'
spectrum_schema_path = '../../../ckanext-unaids/ckanext/unaids/package_schemas/spectrum.json'
pjnz_directory = '../../../'

pjnz = PJNZFile(pjnz_directory+'Botswana_ 2019.PJNZ')


def create_spectrum_package(pjnz_path, directory='.'):
    print("\n===" + directory + "===")
    pjnz = PJNZFile(pjnz_path)
    with open(spectrum_schema_path) as json_file:
        schema = json.load(json_file)
        if not os.path.exists(directory):
            os.makedirs(directory)
        for resource in schema['resources']:
            print(resource['name'] + " - " + resource.get('python_class', 'No Python Class'))

            validator_schema = filter(
                lambda f: f.get('field_name', '') == 'validator_schema',
                resource['resource_fields']
            )[0]['field_value']

            class_name = resource['python_class'].split('.')[-1]
            module_name = '.'.join(resource['python_class'].split('.')[:-1])
            imported_module = importlib.import_module(module_name)
            imported_class = getattr(imported_module, class_name)
            schemed_table = imported_class(schemas_directory + validator_schema + '.json')

            schemed_table.create_csv_table(pjnz, directory=directory)


spectrum_files = {
    'Burkina_Faso_2019': 'Burkina_Faso_2019_05_03_Final.PJNZ',
    'Botswana_2019': 'Botswana_ 2019.PJNZ',
    'Malawi_2019': 'Malawi_2019_v22.PJNZ',
    'Mauritius_2018': 'Mauritius_2018_shadow.PJNZ',
    'Cameroon_2019': 'Cameroon_2019_05_06_Final.PJNZ',
    #'Senegal_2018': 'Senegal_2018_final_v5_63.pjnz',
    #'Niger_2018': 'Niger_2018_final_v5_63.PJNZ',
    #'Mauritanie_2018': 'Mauritanie_2018_final_v5_65.PJNZ'
}

for k, v in spectrum_files.iteritems():
    create_spectrum_package(pjnz_directory+v, k)

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
#
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
