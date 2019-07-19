from collections import OrderedDict
from schemed_table import SchemedTable
from rpy2.robjects import pandas2ri
import pandas
import numpy as np
import pandas as pd
import logging

# Imports below are used by python snippets in the json schemas.
# They may look unused, but they actually are used!
from datetime import datetime


def build_spectrum_table(spectrum_file, schema, index=None, **kwargs):
        """
        This function factorises out common code required to auto-populate
        an ADR Spectrum resource from a PJNZ file. It uses the ADR resource
        validation schema to build a dataframe and insert into it data from
        the PJNZ file.

        IMPORTANT - This function evaluates snippets of code from the JSON
        schemas. This is not ideal, as the snippets would ideally be brought
        into the Python ecosystem. However, for the time being it was seen as
        the cleanest way to store the complex mapping of data from PJNZ to ADR
        resource.
        """
        # We reference the spectrum file a lot, so give it a shorthand ref
        sf = spectrum_file

        # Remove the first schema field as this is the header/index
        schema = schema.copy()
        first_field = schema['fields'].pop(0)

        # Assemble the populated data file in dictionaries
        new_table = OrderedDict()
        for field in schema['fields']:
            if field.get('spectrum_file_key', False):

                # Fill row in with spectrum data
                try:
                    # IMPORTANT - We evaluate a snippet of code from the JSON file
                    data_series = list(eval(field['spectrum_file_key']))
                except Exception:
                    logging.error(
                        "Failed to evaluate spectrum_file_key: " +
                        field['spectrum_file_key']
                    )
                    raise
                new_table[field['name']] = data_series

            else:
                # If no spectrum_file_key given, then leave row empty
                new_table[field['name']] = [np.NAN]*len(sf.year_range)

        new_table = pd.DataFrame.from_dict(
            new_table,
            **kwargs
        )

        # Fix the indicies if they are mannually specified
        if index:
            new_table.index = index
        new_table.insert(0, first_field['name'], new_table.index)

        return new_table


class ANCPrevalenceTable(SchemedTable):

    def create_table(self, pjnz):
        """
        ANC Prevelance table is taken from the XML file, and loaded through
        Imperial's SpecIO R Package.
        """
        # Use the SpecIO package to extrac epp model data.
        epp_data = pjnz._extract_epp_data()
        regions = epp_data.names
        anc_prev = {}

        # Utility function to convert an R matrix into a pandas dataframe
        def _r2df(r_matrix):
            dataframe = pandas2ri.ri2py_dataframe(r_matrix)
            dataframe.columns = r_matrix.colnames
            dataframe.index = r_matrix.rownames
            return dataframe

        # Data is stored by region in the SpecIO outputs.
        for region in regions:

            #  Assemble all the different pieces of data required
            anc_ss_perc = _r2df(epp_data.rx2(region).rx2('anc.prev'))
            anc_ss_perc['Type'] = "ANC-SS (%)"
            anc_ss_num = _r2df(epp_data.rx2(region).rx2('anc.n'))
            anc_ss_num['Type'] = "ANC-SS (N)"
            anc_rt_perc = _r2df(epp_data.rx2(region).rx2('ancrtsite.prev'))
            anc_rt_perc['Type'] = "ANC-RT (%)"
            anc_rt_num = _r2df(epp_data.rx2(region).rx2('ancrtsite.n'))
            anc_rt_num['Type'] = "ANC-RT (N)"

            anc_region_prev = pandas.concat(
                [anc_ss_perc, anc_ss_num, anc_rt_perc, anc_rt_num]
            )
            anc_region_prev['Region'] = region
            anc_prev[region] = anc_region_prev

        # Assemble final table from multiple regional tables
        anc_prev = pandas.concat(anc_prev.values())
        anc_prev['Site'] = anc_prev.index

        # Many values have been set to some huge negative figure.
        # Assume these should be empty?
        tmp = anc_prev._get_numeric_data()
        tmp[tmp < 0] = np.NaN

        # Sort the columns and the rows as desired.
        columns = list(map(lambda x: x['name'], self.schema['fields']))
        anc_prev = anc_prev[columns]
        anc_prev = anc_prev.sort_values(by=['Site', 'Region'])

        return anc_prev


class ANCTestingTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "ANCTestingValues MV": {
                "type": int
            }
        }

        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        ).dropna(how='all', axis='columns')


class BreastfeedingTable(SchemedTable):

    def create_table(self, spectrum_file):
        
        spectrum_file.dp_tables = {
            "InfantFeedingOptions MV": {
                "type": float
            }
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient="columns",
            index=self.schema['fields'][0]['example_values']
        )


class PMTCTTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "ARVRegimen MV2": {
                "type": int,
                "columns": ["Drop"] + spectrum_file.default_columns
            }
        }

        # We only want to return a subset of the columns
        columns_to_keep = [self.schema['fields'][0]['name']]
        columns_to_keep += self.schema['fields'][0]["example_values"]
        columns_to_keep = list(map(str, columns_to_keep))

        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )[columns_to_keep]


class ARTTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "HAARTBySex MV": {"type": int},
            "MedianCD4 MV": {"type": float},
            "PercLostFollowup MV": {"type": int},
            "CD4ThreshHoldAdults MV": {"type": int},
            "ChildARTCalc MV2": {"type": float},
            "ChildTreatInputs MV3": {"type": float},
            "PercLostFollowupChild MV": {"type": int},
            "CD4ThreshHold MV": {"type": int},
            "ChildNeedPMTCT MV": {"type": float},
            "ChildOnPMTCT MV": {"type": int},
            "NumNewARTPats MV": {"type": float},
            "MedCD4CountInit MV": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )


class CaseMortalityTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "FitIncidence MV6": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )


class KnownStatusTable(SchemedTable):

    def create_table(self, spectrum_file):

        spectrum_file.dp_tables = {
            "VrialSuppressionInput MV": {"type": int}
        }
        return build_spectrum_table(
            spectrum_file,
            self.schema,
            orient='index',
            columns=spectrum_file.year_range
        )
